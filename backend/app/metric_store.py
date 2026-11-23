"""物化指标持久化：把轮次确认的物化明细与来源分区版本落盘。

内存仓库本身不持久，物化指标明细要求“持久保存的真实结果”，
这里用原子写入的 JSON 文件承载，重启后自动恢复，报表仍能读到
历史已确认轮次的分组值、来源分区与逐事实依据。
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.services.metric_materialize import MetricMaterializer


def default_state_path() -> Path:
    """状态文件路径可由 METRIC_STATE_FILE 覆盖，默认放在后端数据目录。"""
    override = os.environ.get("METRIC_STATE_FILE", "").strip()
    if override:
        return Path(override)
    data_dir = Path(__file__).resolve().parents[1] / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir / "metric_materialized_state.json"


def persist_materialized_state(
    path: str | os.PathLike[str] | None = None,
    engine: "MetricMaterializer | None" = None,
) -> str:
    """原子落盘物化引擎状态，返回实际写入路径。

    ``engine`` 缺省使用进程级全局单例；测试等隔离场景可显式传入独立实例。
    """
    if engine is None:
        from app.services.metric_materialize import materializer as engine
    target = Path(path) if path is not None else default_state_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(engine.snapshot(), ensure_ascii=False, indent=2)
    # 同目录临时文件 + os.replace 保证发布原子性，读到的文件永远是完整一轮快照
    fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp",
                                    dir=str(target.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
        os.replace(tmp_name, target)
    except BaseException:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
        raise
    return str(target)


def load_materialized_state(
    path: str | os.PathLike[str] | None = None,
    engine: "MetricMaterializer | None" = None,
) -> bool:
    """启动时恢复已持久化的物化状态；文件不存在时静默跳过。"""
    if engine is None:
        from app.services.metric_materialize import materializer as engine
    target = Path(path) if path is not None else default_state_path()
    if not target.exists():
        return False
    with target.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    engine.restore(payload)
    return True
