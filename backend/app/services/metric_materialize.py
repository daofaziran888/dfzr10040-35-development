"""物化指标引擎：按来源分区计算、按确认轮次原子发布的物化指标明细。

设计要点
--------
1. 输入侧（事实表 + 分区版本）
   - 每个来源分区维护事实版本表，同一事实的迟到更正按版本覆盖，绝不重复累加。
   - 分区有单调递增的分区版本号，任何更正都会推进版本号，可作为乐观锁。
2. 计算侧（确认轮次）
   - 一轮重算先声明完整的来源分区范围，逐个分区读取并固化当时的分区版本与事实依据。
   - 提交前再次核对分区版本：范围内任一分区在读取后被迟到更正推进过版本，
     判定为输入版本冲突，本轮拒绝发布，报表继续沿用上一轮已确认结果。
   - 提交成功后整轮原子发布，报表在任意时刻只能读到同一轮确认的完整分区范围。
3. 报表侧（物化指标明细）
   - 每条明细记录分组值、聚合结果、所属轮次、来源分区、分区版本和逐事实依据，
     历史已确认轮次全部保留，报表与重算都从同一轮明细复算/取证。
"""
from __future__ import annotations

import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class MetricFact:
    """来源事实：同一 (来源, 分区, 事实键) 按版本覆盖，删除保留版本墓碑。"""

    fact_key: str
    source: str
    partition: str
    value: float
    version: int
    op: str = "upsert"  # upsert / delete
    updated_at: str = field(default_factory=_now_iso)
    delivery_id: str = ""  # 投递幂等键，同一更正重复投递不会重复生效


@dataclass
class PartitionState:
    """来源分区状态：事实版本表 + 单调分区版本号。"""

    source: str
    partition: str
    partition_version: int = 0
    facts: dict[str, MetricFact] = field(default_factory=dict)
    # delivery_id -> (fact_key, version)，用于重复投递幂等
    delivered: dict[str, tuple[str, int]] = field(default_factory=dict)


@dataclass
class PartitionRead:
    """一轮计算中对单个来源分区的读取记录（证据）。"""

    source: str
    partition: str
    partition_version: int
    fact_versions: dict[str, int]
    fact_count: int


@dataclass
class MaterializedMetric:
    """物化指标明细：分组级真实结果及其完整依据。"""

    detail_id: int
    rule_id: int
    round_id: int
    group_value: str
    agg_function: str
    metric_value: float
    source_partitions: list[str]
    # 每个来源分区确认时的版本号，复算必须从同一版本范围出发
    partition_versions: dict[str, int]
    # 逐事实依据：来源 / 分区 / 事实键 / 版本 / 取值
    evidence: list[dict[str, Any]]
    published_at: str


@dataclass
class ComputeRound:
    """一次重算轮次：声明分区范围、逐分区读取、冲突检测后原子确认。"""

    round_id: int
    rule_id: int
    target_table: str
    agg_function: str
    group_field: str
    # 本轮承诺的完整来源分区范围 ["来源:分区", ...]
    scope: list[str]
    status: str  # 计算中 / 已确认 / 版本冲突 / 已放弃
    reads: dict[str, PartitionRead] = field(default_factory=dict)
    details: list[MaterializedMetric] = field(default_factory=list)
    conflicts: list[dict[str, Any]] = field(default_factory=list)
    opened_at: str = field(default_factory=_now_iso)
    confirmed_at: str = ""


class MetricMaterializer:
    """物化指标引擎：输入幂等、分区版本乐观锁、轮次原子发布。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._rules: dict[int, dict[str, Any]] = {}
        self._partitions: dict[tuple[str, str], PartitionState] = {}
        self._rounds: dict[int, ComputeRound] = {}
        self._details: list[MaterializedMetric] = []
        self._next_rule_id = 1
        self._next_round_id = 1
        self._next_detail_id = 1

    # ------------------------------------------------------------------
    # 规则登记
    # ------------------------------------------------------------------
    def register_rule(self, target_table: str, agg_function: str,
                      group_field: str, storage_table: str = "") -> dict[str, Any]:
        """登记聚合规则（来源分区上的分组聚合，目前支持 sum/count）。"""
        with self._lock:
            rule_id = self._next_rule_id
            self._next_rule_id += 1
            rule = {
                "rule_id": rule_id,
                "rule_code": f"METR-MAT-{rule_id:04d}",
                "target_table": target_table,
                "agg_function": agg_function.strip().lower(),
                "group_field": group_field,
                "storage_table": storage_table or f"aggr_{target_table}_materialized",
                "created_at": _now_iso(),
                "latest_round_id": 0,
            }
            self._rules[rule_id] = rule
            return dict(rule)

    def list_rules(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(rule) for rule in self._rules.values()]

    # ------------------------------------------------------------------
    # 输入侧：来源分区事实与版本
    # ------------------------------------------------------------------
    def ingest_fact(self, source: str, partition: str, fact_key: str,
                    value: float, *, version: int | None = None,
                    op: str = "upsert", delivery_id: str = "") -> dict[str, Any]:
        """向来源分区投递事实/更正。

        - 带 ``delivery_id`` 的重复投递直接幂等返回，同一事实更正不会重复累加。
        - 显式 ``version`` 用于乐观并发：旧版本事实不能覆盖新版本。
        - 任一有效写入都会推进分区版本号，供轮次提交时做冲突检测。
        """
        with self._lock:
            state = self._partitions.setdefault(
                (source, partition), PartitionState(source=source, partition=partition))

            if delivery_id and delivery_id in state.delivered:
                seen_key, seen_version = state.delivered[delivery_id]
                existing = state.facts.get(seen_key)
                return {
                    "ok": True,
                    "deduplicated": True,
                    "source": source,
                    "partition": partition,
                    "fact_key": seen_key,
                    "version": seen_version,
                    "partition_version": state.partition_version,
                    "current_value": None if existing is None or existing.op == "delete"
                    else existing.value,
                }

            current = state.facts.get(fact_key)
            next_version = version if version is not None else (
                current.version + 1 if current else 1)
            if current is not None and next_version <= current.version:
                # 迟到的旧版本事实，拒绝回退覆盖
                return {
                    "ok": False,
                    "deduplicated": False,
                    "reason": "stale_version",
                    "source": source,
                    "partition": partition,
                    "fact_key": fact_key,
                    "version": next_version,
                    "current_version": current.version,
                    "partition_version": state.partition_version,
                }

            fact = MetricFact(
                fact_key=fact_key, source=source, partition=partition,
                value=float(value), version=next_version, op=op,
                delivery_id=delivery_id)
            state.facts[fact_key] = fact
            state.partition_version += 1
            if delivery_id:
                state.delivered[delivery_id] = (fact_key, next_version)
            return {
                "ok": True,
                "deduplicated": False,
                "source": source,
                "partition": partition,
                "fact_key": fact_key,
                "version": next_version,
                "partition_version": state.partition_version,
                "current_value": None if op == "delete" else float(value),
            }

    def list_partitions(self) -> list[dict[str, Any]]:
        """列出当前全部来源分区及其版本与事实数。"""
        with self._lock:
            return [
                {
                    "source": state.source,
                    "partition": state.partition,
                    "partition_version": state.partition_version,
                    "fact_count": len(state.facts),
                    "active_fact_count": sum(
                        1 for fact in state.facts.values() if fact.op != "delete"),
                }
                for state in sorted(self._partitions.values(),
                                    key=lambda s: (s.source, s.partition))
            ]

    def _partition_token(self, source: str, partition: str) -> str:
        return f"{source}:{partition}"

    # ------------------------------------------------------------------
    # 计算侧：轮次打开、逐分区读取、提交确认
    # ------------------------------------------------------------------
    def open_round(self, rule_id: int,
                   scope: list[tuple[str, str]] | None = None) -> ComputeRound:
        """打开一轮重算并声明完整来源分区范围。

        ``scope`` 为 ``[(来源, 分区), ...]``；缺省取规则目标表当前全部来源分区。
        """
        with self._lock:
            rule = self._rules.get(rule_id)
            if rule is None:
                raise KeyError(f"聚合规则 {rule_id} 不存在")
            if scope is None:
                tokens = [
                    self._partition_token(state.source, state.partition)
                    for state in self._partitions.values()
                ]
            else:
                tokens = [self._partition_token(source, partition)
                          for source, partition in scope]
            # 去重并稳定排序，保证每轮范围明确、可复算
            tokens = sorted(set(tokens))
            round_id = self._next_round_id
            self._next_round_id += 1
            round_ = ComputeRound(
                round_id=round_id,
                rule_id=rule_id,
                target_table=rule["target_table"],
                agg_function=rule["agg_function"],
                group_field=rule["group_field"],
                scope=tokens,
                status="计算中",
            )
            self._rounds[round_id] = round_
            return round_

    def read_partition(self, round_id: int, source: str,
                       partition: str) -> PartitionRead:
        """轮次内读取一个来源分区，固化当时分区版本与事实版本，作为确认依据。"""
        with self._lock:
            round_ = self._rounds.get(round_id)
            if round_ is None:
                raise KeyError(f"计算轮次 {round_id} 不存在")
            if round_.status != "计算中":
                raise RuntimeError(f"轮次 {round_id} 状态为 {round_.status}，不可继续读取")
            token = self._partition_token(source, partition)
            if token not in round_.scope:
                raise ValueError(f"分区 {token} 不在轮次 {round_id} 声明范围内")
            state = self._partitions.get((source, partition))
            if state is None:
                read = PartitionRead(
                    source=source, partition=partition,
                    partition_version=0, fact_versions={}, fact_count=0)
            else:
                read = PartitionRead(
                    source=source,
                    partition=partition,
                    partition_version=state.partition_version,
                    fact_versions={key: fact.version
                                   for key, fact in state.facts.items()},
                    fact_count=len(state.facts),
                )
            round_.reads[token] = read
            return read

    def _aggregate(self, round_: ComputeRound) -> list[MaterializedMetric]:
        """依据轮次已读取分区的事实版本做分组聚合。"""
        grouped: dict[str, list[MetricFact]] = {}
        evidence_by_group: dict[str, list[dict[str, Any]]] = {}
        partition_versions: dict[str, int] = {}

        for token in round_.scope:
            read = round_.reads.get(token)
            if read is None:
                raise RuntimeError(f"分区 {token} 尚未读取，无法提交轮次 {round_.round_id}")
            partition_versions[token] = read.partition_version
            state = self._partitions.get((read.source, read.partition))
            if state is None:
                continue
            for fact_key, fact in state.facts.items():
                # 只允许使用读取时刻已固化的事实版本，杜绝读取后混入新版本
                if read.fact_versions.get(fact_key) != fact.version:
                    continue
                if fact.op == "delete":
                    continue
                group_value = self._group_of(round_, fact)
                grouped.setdefault(group_value, []).append(fact)
                evidence_by_group.setdefault(group_value, []).append({
                    "source": fact.source,
                    "partition": fact.partition,
                    "fact_key": fact.fact_key,
                    "version": fact.version,
                    "value": fact.value,
                    "delivery_id": fact.delivery_id,
                })

        details: list[MaterializedMetric] = []
        for group_value in sorted(grouped):
            facts = grouped[group_value]
            if round_.agg_function == "sum":
                value = sum(fact.value for fact in facts)
            elif round_.agg_function == "count":
                value = float(len(facts))
            elif round_.agg_function == "max":
                value = max(fact.value for fact in facts)
            elif round_.agg_function == "min":
                value = min(fact.value for fact in facts)
            else:
                raise ValueError(f"不支持的聚合函数：{round_.agg_function}")
            detail = MaterializedMetric(
                detail_id=self._next_detail_id,
                rule_id=round_.rule_id,
                round_id=round_.round_id,
                group_value=group_value,
                agg_function=round_.agg_function,
                metric_value=value,
                source_partitions=list(round_.scope),
                partition_versions=dict(partition_versions),
                evidence=evidence_by_group[group_value],
                published_at=_now_iso(),
            )
            self._next_detail_id += 1
            details.append(detail)
        return details

    @staticmethod
    def _group_of(round_: ComputeRound, fact: MetricFact) -> str:
        """分组取值：约定事实键以 ``<分组值>#<业务键>`` 编码，缺省回退为事实键。"""
        if "#" in fact.fact_key:
            return fact.fact_key.split("#", 1)[0]
        return fact.fact_key

    def commit_round(self, round_id: int) -> dict[str, Any]:
        """提交轮次：先做分区版本冲突检测，通过后原子发布整轮物化明细。"""
        with self._lock:
            round_ = self._rounds.get(round_id)
            if round_ is None:
                raise KeyError(f"计算轮次 {round_id} 不存在")
            if round_.status != "计算中":
                return {"ok": False, "round_id": round_id, "status": round_.status,
                        "message": f"轮次状态为 {round_.status}，不可提交"}

            missing = [token for token in round_.scope
                       if token not in round_.reads]
            if missing:
                return {"ok": False, "round_id": round_id, "status": round_.status,
                        "message": f"分区未读取完整，缺少：{'、'.join(missing)}"}

            # 乐观锁：范围内任一分区版本在读取后被推进，即输入版本冲突
            conflicts: list[dict[str, Any]] = []
            for token in round_.scope:
                read = round_.reads[token]
                state = self._partitions.get((read.source, read.partition))
                current_version = state.partition_version if state else 0
                if current_version != read.partition_version:
                    conflicts.append({
                        "source": read.source,
                        "partition": read.partition,
                        "read_version": read.partition_version,
                        "current_version": current_version,
                    })

            if conflicts:
                round_.status = "版本冲突"
                round_.conflicts = conflicts
                return {
                    "ok": False,
                    "round_id": round_id,
                    "status": "版本冲突",
                    "conflicts": conflicts,
                    "message": "输入版本冲突：来源分区在读取后发生迟到更正，本轮拒绝发布",
                }

            details = self._aggregate(round_)
            round_.details = details
            round_.status = "已确认"
            round_.confirmed_at = _now_iso()
            self._details.extend(details)
            self._rules[round_.rule_id]["latest_round_id"] = round_.round_id
            return {
                "ok": True,
                "round_id": round_id,
                "status": "已确认",
                "scope": list(round_.scope),
                "partition_versions": {
                    token: round_.reads[token].partition_version
                    for token in round_.scope
                },
                "details": [self._detail_dict(detail) for detail in details],
                "message": f"轮次 {round_id} 已原子发布 {len(details)} 条物化指标明细",
            }

    def retry_round(self, conflict_round_id: int) -> ComputeRound:
        """冲突轮次重试：按原范围重新打开一轮，重新读取各分区最新版本。"""
        with self._lock:
            conflict = self._rounds.get(conflict_round_id)
            if conflict is None:
                raise KeyError(f"计算轮次 {conflict_round_id} 不存在")
            scope_pairs = [tuple(token.split(":", 1)) for token in conflict.scope]
            fresh = self.open_round(conflict.rule_id, scope_pairs)  # type: ignore[arg-type]
            for source, partition in scope_pairs:
                self.read_partition(fresh.round_id, source, partition)
            return fresh

    def abandon_round(self, round_id: int) -> dict[str, Any]:
        """放弃未确认轮次（例如确认无法收敛时），已发布明细不受影响。"""
        with self._lock:
            round_ = self._rounds.get(round_id)
            if round_ is None:
                raise KeyError(f"计算轮次 {round_id} 不存在")
            if round_.status == "计算中":
                round_.status = "已放弃"
            return {"ok": True, "round_id": round_id, "status": round_.status}

    # ------------------------------------------------------------------
    # 报表侧：读取已确认物化明细
    # ------------------------------------------------------------------
    def get_report(self, rule_id: int,
                   round_id: int | None = None) -> dict[str, Any]:
        """报表读取物化指标明细。

        缺省返回规则最新已确认轮次；指定 ``round_id`` 可读取历史已确认轮次。
        计算中/冲突轮次永远不会出现在报表里，因此刷新拿到的分组值必然来自
        同一轮确认的完整分区范围。
        """
        with self._lock:
            rule = self._rules.get(rule_id)
            if rule is None:
                raise KeyError(f"聚合规则 {rule_id} 不存在")
            target_round_id = round_id or rule["latest_round_id"]
            if not target_round_id:
                return {
                    "ok": True,
                    "rule_id": rule_id,
                    "round_id": 0,
                    "status": "无已确认轮次",
                    "scope": [],
                    "details": [],
                }
            round_ = self._rounds.get(target_round_id)
            if round_ is None or round_.status != "已确认":
                return {
                    "ok": False,
                    "rule_id": rule_id,
                    "round_id": target_round_id,
                    "status": round_.status if round_ else "不存在",
                    "details": [],
                    "message": "该轮次未确认，报表只读取已确认物化明细",
                }
            return {
                "ok": True,
                "rule_id": rule_id,
                "round_id": round_.round_id,
                "status": round_.status,
                "confirmed_at": round_.confirmed_at,
                "scope": list(round_.scope),
                "partition_versions": {
                    token: round_.reads[token].partition_version
                    for token in round_.scope
                },
                "details": [self._detail_dict(detail) for detail in round_.details],
            }

    def list_rounds(self, rule_id: int | None = None) -> list[dict[str, Any]]:
        """列出轮次（含冲突/放弃记录），保留全部历史确认结果与来源。"""
        with self._lock:
            rounds = [r for r in self._rounds.values()
                      if rule_id is None or r.rule_id == rule_id]
            return [
                {
                    "round_id": r.round_id,
                    "rule_id": r.rule_id,
                    "status": r.status,
                    "scope": list(r.scope),
                    "read_versions": {
                        token: read.partition_version
                        for token, read in r.reads.items()
                    },
                    "conflicts": list(r.conflicts),
                    "detail_count": len(r.details),
                    "opened_at": r.opened_at,
                    "confirmed_at": r.confirmed_at,
                }
                for r in sorted(rounds, key=lambda item: item.round_id)
            ]

    def list_details(self, rule_id: int | None = None,
                     group_value: str | None = None,
                     round_id: int | None = None) -> list[dict[str, Any]]:
        """查询物化指标明细（可按规则、分组、轮次过滤），历史轮次全部保留。"""
        with self._lock:
            result = []
            for detail in self._details:
                if rule_id is not None and detail.rule_id != rule_id:
                    continue
                if round_id is not None and detail.round_id != round_id:
                    continue
                if group_value is not None and detail.group_value != group_value:
                    continue
                result.append(self._detail_dict(detail))
            return result

    def verify_round(self, round_id: int) -> dict[str, Any]:
        """从该轮确认的完整分区范围与事实版本复算，核对物化明细可复现。

        复算只使用轮次确认时固化在明细里的逐事实依据（含版本号），
        不触碰当前事实表，因此迟到更正不会改变历史轮次的复核结论；
        同时报告当前分区版本是否已相对确认范围发生漂移。
        """
        with self._lock:
            round_ = self._rounds.get(round_id)
            if round_ is None:
                raise KeyError(f"计算轮次 {round_id} 不存在")
            if round_.status != "已确认":
                return {"ok": False, "round_id": round_id,
                        "status": round_.status, "message": "轮次未确认，无可复核明细"}

            # 从已固化依据复算：同一分组下按确认的事实版本重新施加聚合函数
            recomputed: dict[str, float] = {}
            for detail in round_.details:
                values = [float(item["value"]) for item in detail.evidence]
                if not values:
                    continue
                if round_.agg_function == "sum":
                    recomputed[detail.group_value] = sum(values)
                elif round_.agg_function == "count":
                    recomputed[detail.group_value] = float(len(values))
                elif round_.agg_function == "max":
                    recomputed[detail.group_value] = max(values)
                elif round_.agg_function == "min":
                    recomputed[detail.group_value] = min(values)

            published = {detail.group_value: detail.metric_value
                         for detail in round_.details}
            consistent = published == recomputed

            # 提示当前事实表相对确认范围的漂移（不影响历史轮次一致性）
            drifted: list[dict[str, Any]] = []
            for token in round_.scope:
                read = round_.reads[token]
                state = self._partitions.get((read.source, read.partition))
                current_version = state.partition_version if state else 0
                if current_version != read.partition_version:
                    drifted.append({
                        "source": read.source, "partition": read.partition,
                        "confirmed_version": read.partition_version,
                        "current_version": current_version,
                    })

            return {
                "ok": consistent,
                "round_id": round_id,
                "scope": list(round_.scope),
                "partition_versions": {
                    token: read.partition_version
                    for token, read in round_.reads.items()
                },
                "published_values": published,
                "recomputed_values": recomputed,
                "partition_drift": drifted,
                "message": "复算与已确认物化明细一致" if consistent
                else "复算结果与物化明细不一致",
            }

    # ------------------------------------------------------------------
    # 持久化快照（真实结果落盘，重启后仍可读取已确认明细）
    # ------------------------------------------------------------------
    def snapshot(self) -> dict[str, Any]:
        """导出可 JSON 序列化的完整状态。"""
        with self._lock:
            return {
                "rules": [dict(rule) for rule in self._rules.values()],
                "partitions": [
                    {
                        "source": state.source,
                        "partition": state.partition,
                        "partition_version": state.partition_version,
                        "facts": [asdict(fact) for fact in state.facts.values()],
                        "delivered": {key: list(value)
                                      for key, value in state.delivered.items()},
                    }
                    for state in self._partitions.values()
                ],
                "rounds": [self._round_dict(r) for r in self._rounds.values()],
                "details": [self._detail_dict(d) for d in self._details],
                "next_rule_id": self._next_rule_id,
                "next_round_id": self._next_round_id,
                "next_detail_id": self._next_detail_id,
            }

    def restore(self, payload: dict[str, Any]) -> None:
        """从快照恢复状态。"""
        with self._lock:
            self._rules = {rule["rule_id"]: dict(rule) for rule in payload.get("rules", [])}
            self._partitions = {}
            for item in payload.get("partitions", []):
                state = PartitionState(
                    source=item["source"], partition=item["partition"],
                    partition_version=item["partition_version"])
                for fact_payload in item.get("facts", []):
                    fact = MetricFact(**fact_payload)
                    state.facts[fact.fact_key] = fact
                state.delivered = {
                    key: (value[0], value[1])
                    for key, value in item.get("delivered", {}).items()
                }
                self._partitions[(state.source, state.partition)] = state
            self._rounds = {}
            for item in payload.get("rounds", []):
                reads = {
                    token: PartitionRead(**read_payload)
                    for token, read_payload in item.get("reads", {}).items()
                }
                details = [MaterializedMetric(**detail_payload)
                           for detail_payload in item.get("details", [])]
                self._rounds[item["round_id"]] = ComputeRound(
                    round_id=item["round_id"], rule_id=item["rule_id"],
                    target_table=item["target_table"], agg_function=item["agg_function"],
                    group_field=item["group_field"], scope=list(item["scope"]),
                    status=item["status"], reads=reads, details=details,
                    conflicts=list(item.get("conflicts", [])),
                    opened_at=item["opened_at"], confirmed_at=item.get("confirmed_at", ""))
            self._details = [MaterializedMetric(**item)
                             for item in payload.get("details", [])]
            self._next_rule_id = payload.get("next_rule_id", 1)
            self._next_round_id = payload.get("next_round_id", 1)
            self._next_detail_id = payload.get("next_detail_id", 1)

    @staticmethod
    def _detail_dict(detail: MaterializedMetric) -> dict[str, Any]:
        return asdict(detail)

    def _round_dict(self, round_: ComputeRound) -> dict[str, Any]:
        return {
            "round_id": round_.round_id,
            "rule_id": round_.rule_id,
            "target_table": round_.target_table,
            "agg_function": round_.agg_function,
            "group_field": round_.group_field,
            "scope": list(round_.scope),
            "status": round_.status,
            "reads": {token: asdict(read) for token, read in round_.reads.items()},
            "details": [asdict(detail) for detail in round_.details],
            "conflicts": list(round_.conflicts),
            "opened_at": round_.opened_at,
            "confirmed_at": round_.confirmed_at,
        }


# 全局单例：服务层与路由共享
materializer = MetricMaterializer()
