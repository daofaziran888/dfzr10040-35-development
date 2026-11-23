"""物化指标明细测试：来源分区计算、轮次确认发布、迟到更正与幂等。

场景（与需求描述一一对应）：

1. 先用来源 A 的事实 8、12（合计 20）和来源 B 的 5 计算并确认一轮；
2. 发起重算，读完 A 之后把同一迟到更正（A 的 12 改为 14，合计 22）投递两次；
3. 同事在修订期间刷新报表：任何刷新读到的都是上一轮已确认值 A=20、B=5，
   且明细可从同一轮确认的完整分区范围复算；
4. 重算提交因输入版本冲突被拒绝，重试轮次重新读取最新分区后确认，
   同一事实更正只生效一次，最终 A=22、B=5；
5. 历史已确认轮次与来源依据全部保留。
"""
from __future__ import annotations

import json

import pytest

from app.metric_store import (
    default_state_path,
    load_materialized_state,
    persist_materialized_state,
)
from app.services.metric_materialize import MetricMaterializer


@pytest.fixture
def engine() -> MetricMaterializer:
    return MetricMaterializer()


def _open_and_commit(engine: MetricMaterializer, rule_id: int,
                     scope: list[tuple[str, str]] | None = None) -> dict:
    round_ = engine.open_round(rule_id, scope)
    pairs = scope if scope is not None else [
        tuple(token.split(":", 1)) for token in round_.scope  # type: ignore[misc]
    ]
    for source, partition in pairs:
        engine.read_partition(round_.round_id, source, partition)
    return engine.commit_round(round_.round_id)


class TestFirstConfirmedRound:
    """第一轮：A 的 8、12 与 B 的 5。"""

    def test_first_round_materializes_a20_b5(self, engine: MetricMaterializer) -> None:
        rule = engine.register_rule(target_table="WT_SALES", agg_function="sum",
                                    group_field="region")
        rule_id = rule["rule_id"]

        # 来源 A 分区 p1：8 + 12 = 20；来源 B 分区 p1：5
        engine.ingest_fact("src_a", "p1", "A#order-1", 8)
        engine.ingest_fact("src_a", "p1", "A#order-2", 12)
        engine.ingest_fact("src_b", "p1", "B#order-1", 5)

        result = _open_and_commit(engine, rule_id)
        assert result["ok"] is True
        assert result["scope"] == ["src_a:p1", "src_b:p1"]

        report = engine.get_report(rule_id)
        values = {item["group_value"]: item["metric_value"]
                  for item in report["details"]}
        assert values == {"A": 20.0, "B": 5.0}
        assert report["round_id"] == 1
        # 报表明细携带同一轮确认的完整分区范围与版本
        for detail in report["details"]:
            assert detail["source_partitions"] == ["src_a:p1", "src_b:p1"]
            assert detail["partition_versions"] == {"src_a:p1": 2, "src_b:p1": 1}

    def test_detail_evidence_is_persisted(self, engine: MetricMaterializer) -> None:
        rule_id = engine.register_rule(target_table="WT_SALES",
                                       agg_function="sum", group_field="region")["rule_id"]
        engine.ingest_fact("src_a", "p1", "A#order-1", 8, delivery_id="d1")
        engine.ingest_fact("src_a", "p1", "A#order-2", 12, delivery_id="d2")
        engine.ingest_fact("src_b", "p1", "B#order-1", 5, delivery_id="d3")
        _open_and_commit(engine, rule_id)

        details = engine.list_details(rule_id=rule_id, group_value="A")
        assert len(details) == 1
        evidence = {(item["source"], item["fact_key"]): item["value"]
                    for item in details[0]["evidence"]}
        assert evidence == {
            ("src_a", "A#order-1"): 8.0,
            ("src_a", "A#order-2"): 12.0,
        }


class TestLateCorrectionDuringRecompute:
    """重算读到 A 后迟到更正到达，且同一更正被投递两次。"""

    def test_conflict_round_never_publishes_and_report_stays_stable(
        self, engine: MetricMaterializer,
    ) -> None:
        rule_id = engine.register_rule(target_table="WT_SALES",
                                       agg_function="sum", group_field="region")["rule_id"]
        engine.ingest_fact("src_a", "p1", "A#order-1", 8, delivery_id="d1")
        engine.ingest_fact("src_a", "p1", "A#order-2", 12, delivery_id="d2")
        engine.ingest_fact("src_b", "p1", "B#order-1", 5, delivery_id="d3")
        first = _open_and_commit(engine, rule_id)
        assert first["ok"] is True

        # 修订期间同事能看到的报表始终是第一轮已确认值
        report_before = engine.get_report(rule_id)
        assert {d["group_value"]: d["metric_value"]
                for d in report_before["details"]} == {"A": 20.0, "B": 5.0}

        # 第二轮重算：声明完整范围，先读 A（此时仍是 8、12）
        round2 = engine.open_round(rule_id, [("src_a", "p1"), ("src_b", "p1")])
        read_a = engine.read_partition(round2.round_id, "src_a", "p1")
        assert read_a.partition_version == 2

        # 迟到更正：A#order-2 从 12 改为 14（A 合计 22），同一更正连续投递两次
        correction1 = engine.ingest_fact(
            "src_a", "p1", "A#order-2", 14, delivery_id="correction-order-2")
        correction2 = engine.ingest_fact(
            "src_a", "p1", "A#order-2", 14, delivery_id="correction-order-2")
        assert correction1["deduplicated"] is False
        assert correction1["partition_version"] == 3
        # 第二次是重复投递：幂等命中，分区版本不再推进，事实也不会重复累加
        assert correction2["deduplicated"] is True
        assert correction2["partition_version"] == 3

        # 修订期间再刷新报表：仍是第一轮结果，绝不会出现半成品
        report_during = engine.get_report(rule_id)
        assert report_during["round_id"] == 1
        assert {d["group_value"]: d["metric_value"]
                for d in report_during["details"]} == {"A": 20.0, "B": 5.0}

        # 读完 B 后提交：A 分区版本已从 2 推进到 3，输入版本冲突，拒绝发布
        engine.read_partition(round2.round_id, "src_b", "p1")
        commit = engine.commit_round(round2.round_id)
        assert commit["ok"] is False
        assert commit["status"] == "版本冲突"
        assert commit["conflicts"] == [{
            "source": "src_a", "partition": "p1",
            "read_version": 2, "current_version": 3,
        }]

        # 冲突轮次不能上报表，报表仍锁定第一轮已确认的完整分区范围
        report_after_conflict = engine.get_report(rule_id)
        assert report_after_conflict["round_id"] == 1
        verify = engine.verify_round(1)
        assert verify["ok"] is True
        assert verify["recomputed_values"] == {"A": 20.0, "B": 5.0}

        # 重试轮次：按原范围重新读取最新分区版本后提交
        round3 = engine.retry_round(round2.round_id)
        assert round3.scope == ["src_a:p1", "src_b:p1"]
        retry_commit = engine.commit_round(round3.round_id)
        assert retry_commit["ok"] is True
        assert retry_commit["partition_versions"] == {
            "src_a:p1": 3, "src_b:p1": 1}

        final_report = engine.get_report(rule_id)
        assert final_report["round_id"] == round3.round_id
        final_values = {d["group_value"]: d["metric_value"]
                        for d in final_report["details"]}
        # 更正只生效一次：A = 8 + 14 = 22，B 仍为 5
        assert final_values == {"A": 22.0, "B": 5.0}

    def test_duplicate_correction_never_double_added(self,
                                                     engine: MetricMaterializer) -> None:
        rule_id = engine.register_rule(target_table="WT_SALES",
                                       agg_function="sum", group_field="region")["rule_id"]
        engine.ingest_fact("src_a", "p1", "A#order-1", 8)
        engine.ingest_fact("src_a", "p1", "A#order-2", 12)
        engine.ingest_fact("src_b", "p1", "B#order-1", 5)
        _open_and_commit(engine, rule_id)

        # 不带 delivery_id 时重复投递同值更正也会因版本号只推进一次而被覆盖语义处理；
        # 带相同 delivery_id 则在投递层直接幂等
        first = engine.ingest_fact("src_a", "p1", "A#order-2", 14,
                                   delivery_id="c1")
        second = engine.ingest_fact("src_a", "p1", "A#order-2", 14,
                                    delivery_id="c1")
        third = engine.ingest_fact("src_a", "p1", "A#order-2", 14,
                                   delivery_id="c1")
        assert first["deduplicated"] is False
        assert second["deduplicated"] is True
        assert third["deduplicated"] is True

        result = _open_and_commit(engine, rule_id)
        assert result["ok"] is True
        report = engine.get_report(rule_id)
        values = {d["group_value"]: d["metric_value"] for d in report["details"]}
        assert values == {"A": 22.0, "B": 5.0}
        # A 分组依据只有两条事实，更正没有产生第三条
        a_detail = next(d for d in report["details"] if d["group_value"] == "A")
        assert len(a_detail["evidence"]) == 2


class TestHistoryAndRecompute:
    """历史轮次保留、按轮次取证与重算一致性。"""

    def test_history_rounds_and_details_are_retained(
        self, engine: MetricMaterializer,
    ) -> None:
        rule_id = engine.register_rule(target_table="WT_SALES",
                                       agg_function="sum", group_field="region")["rule_id"]
        engine.ingest_fact("src_a", "p1", "A#order-1", 8)
        engine.ingest_fact("src_a", "p1", "A#order-2", 12)
        engine.ingest_fact("src_b", "p1", "B#order-1", 5)
        _open_and_commit(engine, rule_id)

        engine.ingest_fact("src_a", "p1", "A#order-2", 14, delivery_id="c1")
        _open_and_commit(engine, rule_id)

        rounds = engine.list_rounds(rule_id)
        assert [r["round_id"] for r in rounds] == [1, 2]
        assert all(r["status"] == "已确认" for r in rounds)

        # 历史第一轮明细仍可读取，保留以前已确认结果和来源
        old_report = engine.get_report(rule_id, round_id=1)
        assert {d["group_value"]: d["metric_value"]
                for d in old_report["details"]} == {"A": 20.0, "B": 5.0}
        latest = engine.get_report(rule_id)
        assert {d["group_value"]: d["metric_value"]
                for d in latest["details"]} == {"A": 22.0, "B": 5.0}

        # 每轮都能从自己确认的分区版本范围复算
        assert engine.verify_round(1)["ok"] is True
        assert engine.verify_round(2)["ok"] is True

        # 明细查询：最终 A=22、B=5 的分组明细可查
        latest_details = engine.list_details(rule_id=rule_id, round_id=2)
        assert {d["group_value"]: d["metric_value"] for d in latest_details} == {
            "A": 22.0, "B": 5.0}

    def test_scope_is_complete_and_partial_read_blocks_commit(
        self, engine: MetricMaterializer,
    ) -> None:
        rule_id = engine.register_rule(target_table="WT_SALES",
                                       agg_function="sum", group_field="region")["rule_id"]
        engine.ingest_fact("src_a", "p1", "A#order-1", 8)
        engine.ingest_fact("src_b", "p1", "B#order-1", 5)

        round_ = engine.open_round(rule_id)
        engine.read_partition(round_.round_id, "src_a", "p1")
        # 未读完声明范围不能提交，防止不完整分区范围发布
        commit = engine.commit_round(round_.round_id)
        assert commit["ok"] is False
        assert "src_b:p1" in commit["message"]

    def test_stale_version_cannot_overwrite_newer_fact(
        self, engine: MetricMaterializer,
    ) -> None:
        engine.ingest_fact("src_a", "p1", "A#order-1", 8, version=1)
        rejected = engine.ingest_fact("src_a", "p1", "A#order-1", 1, version=1)
        assert rejected["ok"] is False
        assert rejected["reason"] == "stale_version"
        partitions = {p["partition"]: p for p in engine.list_partitions()}["p1"]
        assert partitions["partition_version"] == 1


class TestPersistence:
    """物化明细持久化：落盘后重启仍能读到已确认分组值与依据。"""

    def test_snapshot_restore_roundtrip(self, engine: MetricMaterializer,
                                        tmp_path) -> None:
        rule_id = engine.register_rule(target_table="WT_SALES",
                                       agg_function="sum", group_field="region")["rule_id"]
        engine.ingest_fact("src_a", "p1", "A#order-1", 8, delivery_id="d1")
        engine.ingest_fact("src_a", "p1", "A#order-2", 12, delivery_id="d2")
        engine.ingest_fact("src_b", "p1", "B#order-1", 5, delivery_id="d3")
        _open_and_commit(engine, rule_id)
        engine.ingest_fact("src_a", "p1", "A#order-2", 14, delivery_id="c1")
        _open_and_commit(engine, rule_id)

        state_file = tmp_path / "metric_state.json"
        persist_materialized_state(state_file, engine=engine)
        payload = json.loads(state_file.read_text(encoding="utf-8"))
        assert len(payload["details"]) == 4  # 两轮 × 两个分组

        restored = MetricMaterializer()
        restored.restore(payload)
        report = restored.get_report(rule_id)
        assert {d["group_value"]: d["metric_value"]
                for d in report["details"]} == {"A": 22.0, "B": 5.0}
        # 重复更正恢复后仍然幂等
        again = restored.ingest_fact("src_a", "p1", "A#order-2", 14,
                                     delivery_id="c1")
        assert again["deduplicated"] is True

    def test_module_load_and_save_helpers(self, monkeypatch, tmp_path) -> None:
        from app.services.metric_materialize import MetricMaterializer

        state_file = tmp_path / "state.json"
        monkeypatch.setenv("METRIC_STATE_FILE", str(state_file))
        isolated = MetricMaterializer()
        assert default_state_path() == state_file
        assert load_materialized_state(state_file, engine=isolated) is False
        persist_materialized_state(state_file, engine=isolated)
        assert load_materialized_state(state_file, engine=isolated) is True
