"""领域服务注册表：通用资源路由按模块 key 分派到独立业务服务。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.source_def import SourceDefService
from app.services.schema_map import SchemaMapService
from app.services.dim_table import DimTableService
from app.services.fusion_job import FusionJobService
from app.services.merge_conflict import MergeConflictService
from app.services.quality_gate import QualityGateService
from app.services.snapshot_mgr import SnapshotMgrService
from app.services.backfill_task import BackfillTaskService
from app.services.lineage_edge import LineageEdgeService
from app.services.replay_queue import ReplayQueueService
from app.services.watermark_track import WatermarkTrackService
from app.services.partition_rule import PartitionRuleService
from app.services.wide_table_def import WideTableDefService
from app.services.delta_merge import DeltaMergeService
from app.services.cold_store import ColdStoreService
from app.services.export_snapshot import ExportSnapshotService
from app.services.metric_aggr import MetricAggrService
from app.services.metric_detail import MetricDetailService
from app.services.cross_validate import CrossValidateService
from app.services.data_drift import DataDriftService
from app.services.fusion_monitor import FusionMonitorService
from app.services.audit_trail import AuditTrailService
from app.services.catalog_search import CatalogSearchService

SERVICES: dict[str, BaseModuleService] = {
    "source_def": SourceDefService(),
    "schema_map": SchemaMapService(),
    "dim_table": DimTableService(),
    "fusion_job": FusionJobService(),
    "merge_conflict": MergeConflictService(),
    "quality_gate": QualityGateService(),
    "snapshot_mgr": SnapshotMgrService(),
    "backfill_task": BackfillTaskService(),
    "lineage_edge": LineageEdgeService(),
    "replay_queue": ReplayQueueService(),
    "watermark_track": WatermarkTrackService(),
    "partition_rule": PartitionRuleService(),
    "wide_table_def": WideTableDefService(),
    "delta_merge": DeltaMergeService(),
    "cold_store": ColdStoreService(),
    "export_snapshot": ExportSnapshotService(),
    "metric_aggr": MetricAggrService(),
    "metric_detail": MetricDetailService(),
    "cross_validate": CrossValidateService(),
    "data_drift": DataDriftService(),
    "fusion_monitor": FusionMonitorService(),
    "audit_trail": AuditTrailService(),
    "catalog_search": CatalogSearchService(),
}


def service_for(module: str) -> BaseModuleService | None:
    return SERVICES.get(module)
