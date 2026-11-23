"""物化指标路由：来源分区事实投递、分区重算轮次、确认发布与报表读取。

报表只读“已确认”轮次的物化指标明细；轮次提交带分区版本乐观锁，
迟到更正引发的输入版本冲突会拒绝发布并保留上一轮结果。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.metric_store import persist_materialized_state
from app.services.metric_materialize import materializer

router = APIRouter(prefix="/api/metric-materialize", tags=["物化指标明细"])


class RulePayload(BaseModel):
    target_table: str
    agg_function: str = "sum"
    group_field: str = "region"
    storage_table: str = ""


class FactPayload(BaseModel):
    source: str
    partition: str
    fact_key: str
    value: float
    version: int | None = None
    op: str = "upsert"
    delivery_id: str = ""


class ScopeItem(BaseModel):
    source: str
    partition: str


class RoundPayload(BaseModel):
    scope: list[ScopeItem] | None = None


class RetryPayload(BaseModel):
    retry: bool = True


def _persist() -> None:
    # 每次状态变更后落盘，保证已确认明细是持久保存的真实结果
    persist_materialized_state()


@router.post("/rules")
def register_rule(payload: RulePayload) -> dict[str, Any]:
    rule = materializer.register_rule(
        target_table=payload.target_table,
        agg_function=payload.agg_function,
        group_field=payload.group_field,
        storage_table=payload.storage_table,
    )
    _persist()
    return {"ok": True, "rule": rule}


@router.get("/rules")
def list_rules() -> dict[str, Any]:
    return {"ok": True, "items": materializer.list_rules()}


@router.post("/facts")
def ingest_fact(payload: FactPayload) -> dict[str, Any]:
    result = materializer.ingest_fact(
        source=payload.source,
        partition=payload.partition,
        fact_key=payload.fact_key,
        value=payload.value,
        version=payload.version,
        op=payload.op,
        delivery_id=payload.delivery_id,
    )
    _persist()
    return result


@router.get("/partitions")
def list_partitions() -> dict[str, Any]:
    return {"ok": True, "items": materializer.list_partitions()}


@router.post("/rules/{rule_id}/rounds")
def open_round(rule_id: int, payload: RoundPayload) -> dict[str, Any]:
    scope = None
    if payload.scope is not None:
        scope = [(item.source, item.partition) for item in payload.scope]
    try:
        round_ = materializer.open_round(rule_id, scope)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _persist()
    return {"ok": True, "round_id": round_.round_id, "scope": round_.scope,
            "status": round_.status}


@router.post("/rounds/{round_id}/reads")
def read_partition(round_id: int, payload: ScopeItem) -> dict[str, Any]:
    try:
        read = materializer.read_partition(round_id, payload.source, payload.partition)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"ok": True, "read": {
        "source": read.source,
        "partition": read.partition,
        "partition_version": read.partition_version,
        "fact_versions": read.fact_versions,
        "fact_count": read.fact_count,
    }}


@router.post("/rounds/{round_id}/commit")
def commit_round(round_id: int, payload: RetryPayload | None = None) -> dict[str, Any]:
    try:
        result = materializer.commit_round(round_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    # 输入版本冲突时可要求自动重试：按原范围重新读取最新分区版本再提交
    if not result.get("ok") and result.get("status") == "版本冲突" \
            and payload is not None and payload.retry:
        fresh = materializer.retry_round(round_id)
        result = materializer.commit_round(fresh.round_id)
        result["retried_from_round"] = round_id
    _persist()
    return result


@router.post("/rounds/{round_id}/abandon")
def abandon_round(round_id: int) -> dict[str, Any]:
    try:
        result = materializer.abandon_round(round_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _persist()
    return result


@router.get("/rules/{rule_id}/report")
def get_report(rule_id: int, round_id: int | None = None) -> dict[str, Any]:
    try:
        return materializer.get_report(rule_id, round_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/rules/{rule_id}/rounds")
def list_rounds(rule_id: int) -> dict[str, Any]:
    return {"ok": True, "items": materializer.list_rounds(rule_id)}


@router.get("/details")
def list_details(rule_id: int | None = None, group_value: str | None = None,
                 round_id: int | None = None) -> dict[str, Any]:
    return {"ok": True,
            "items": materializer.list_details(rule_id, group_value, round_id)}


@router.post("/rounds/{round_id}/verify")
def verify_round(round_id: int) -> dict[str, Any]:
    try:
        return materializer.verify_round(round_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
