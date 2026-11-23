"""物化指标明细领域服务：通用资源接口读取已确认轮次持久化的真实分组结果。

与仅保存规则和计算时间的 ``metric_aggr`` 不同，本模块的每一行都是某一轮
原子发布的物化指标明细，携带来源分区、分区版本和逐事实依据；报表与重算
都从这些已确认明细取值，而不是即时重算。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.metric_materialize import materializer


class MetricDetailService(BaseModuleService):
    MODULE = "metric_detail"
    ENTITY = "物化指标明细"
    OPERATION_TERM = "物化"
    LIST_FIELDS = [
        "明细编号", "规则编号", "确认轮次", "分组值", "聚合函数",
        "指标值", "来源分区", "分区版本", "依据事实数", "发布时间",
    ]
    REQUIRED_FIELDS = ["规则编号", "分组值", "指标值"]
    STATUS_ORDER = ["计算中", "已确认", "版本冲突", "已放弃"]
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        # 明细由物化引擎在轮次确认时产生，资源接口只做只读投影
        rows: list[dict[str, Any]] = []
        for index, detail in enumerate(materializer.list_details(), start=1):
            row = {
                "id": index,
                "status": "已确认",
                "pending": False,
                "abnormal": False,
                "明细编号": f"MATD-{detail['detail_id']:04d}",
                "规则编号": f"METR-MAT-{detail['rule_id']:04d}",
                "确认轮次": detail["round_id"],
                "分组值": detail["group_value"],
                "聚合函数": detail["agg_function"],
                "指标值": detail["metric_value"],
                "来源分区": "、".join(detail["source_partitions"]),
                "分区版本": "、".join(
                    f"{token}={version}"
                    for token, version in detail["partition_versions"].items()
                ),
                "依据事实数": len(detail["evidence"]),
                "发布时间": detail["published_at"],
            }
            rows.append(row)

        for field, expected in (filters or {}).items():
            if field in self.LIST_FIELDS:
                rows = [
                    row for row in rows
                    if expected.casefold() in str(row.get(field, "")).casefold()
                ]
        if keyword:
            rows = [
                row for row in rows
                if keyword.casefold() in str(row.get("明细编号", "")).casefold()
            ]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        # 明细只能通过重算轮次确认发布，禁止手工登记
        return None, ["物化指标明细仅能由重算轮次确认发布，请使用物化指标接口提交轮次"]

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        return None, "物化指标明细为已确认结果，不支持动作流转，重算请发起新的计算轮次"
