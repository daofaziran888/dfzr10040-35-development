"""端到端核对：对真实 uvicorn 进程复现需求场景，验证重启持久化与通用资源明细。

仅依赖标准库，通过子进程启动后端服务（独立状态文件），用 urllib 发请求。
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
_state_dir = tempfile.mkdtemp(prefix="metric_e2e_")
state_file = os.path.join(_state_dir, "state.json")


def call(method: str, path: str, payload: dict | None = None,
         base: str = "http://127.0.0.1:8000") -> dict:
    data = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(base + path, data=data, headers=headers,
                                 method=method)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def wait_healthy(proc: subprocess.Popen) -> None:
    for _ in range(50):
        try:
            body = call("GET", "/api/health")
            if body.get("ok"):
                return
        except OSError:
            pass
        if proc.poll() is not None:
            raise RuntimeError("后端进程提前退出")
        time.sleep(0.2)
    raise RuntimeError("后端健康检查超时")


def main() -> None:
    env = dict(os.environ, METRIC_STATE_FILE=state_file)
    proc = subprocess.Popen(
        ["./.venv/bin/uvicorn", "app.main:app", "--host", "127.0.0.1",
         "--port", "8000"],
        cwd=str(BACKEND_DIR), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        wait_healthy(proc)

        # 1. 登记规则
        rule = call("POST", "/api/metric-materialize/rules", {
            "target_table": "WT_SALES", "agg_function": "sum",
            "group_field": "region"})["rule"]
        rule_id = rule["rule_id"]

        # 2. 第一轮输入：A 的 8、12，B 的 5
        for payload in [
            {"source": "src_a", "partition": "p1", "fact_key": "A#order-1",
             "value": 8, "delivery_id": "d1"},
            {"source": "src_a", "partition": "p1", "fact_key": "A#order-2",
             "value": 12, "delivery_id": "d2"},
            {"source": "src_b", "partition": "p1", "fact_key": "B#order-1",
             "value": 5, "delivery_id": "d3"},
        ]:
            assert call("POST", "/api/metric-materialize/facts", payload)["ok"]

        # 3. 第一轮重算：打开 -> 读全部分区 -> 确认
        round1 = call("POST", f"/api/metric-materialize/rules/{rule_id}/rounds",
                      {})["round_id"]
        for item in [{"source": "src_a", "partition": "p1"},
                     {"source": "src_b", "partition": "p1"}]:
            call("POST", f"/api/metric-materialize/rounds/{round1}/reads", item)
        commit1 = call("POST", f"/api/metric-materialize/rounds/{round1}/commit")
        assert commit1["ok"] is True, commit1

        report1 = call("GET", f"/api/metric-materialize/rules/{rule_id}/report")
        values1 = {d["group_value"]: d["metric_value"] for d in report1["details"]}
        assert values1 == {"A": 20.0, "B": 5.0}, values1
        print("第一轮报表：", values1)

        # 4. 第二轮重算，读完 A 后迟到更正到达，且同一更正投递两次
        round2 = call(
            "POST", f"/api/metric-materialize/rules/{rule_id}/rounds",
            {"scope": [{"source": "src_a", "partition": "p1"},
                       {"source": "src_b", "partition": "p1"}]})["round_id"]
        read_a = call("POST", f"/api/metric-materialize/rounds/{round2}/reads",
                      {"source": "src_a", "partition": "p1"})
        assert read_a["read"]["partition_version"] == 2

        c1 = call("POST", "/api/metric-materialize/facts", {
            "source": "src_a", "partition": "p1", "fact_key": "A#order-2",
            "value": 14, "delivery_id": "correction-order-2"})
        c2 = call("POST", "/api/metric-materialize/facts", {
            "source": "src_a", "partition": "p1", "fact_key": "A#order-2",
            "value": 14, "delivery_id": "correction-order-2"})
        assert c1["deduplicated"] is False and c1["partition_version"] == 3
        assert c2["deduplicated"] is True and c2["partition_version"] == 3

        # 修订期间同事刷新报表：仍是第一轮 A=20、B=5
        mid_report = call("GET",
                          f"/api/metric-materialize/rules/{rule_id}/report")
        assert mid_report["round_id"] == round1
        mid_values = {d["group_value"]: d["metric_value"]
                      for d in mid_report["details"]}
        assert mid_values == {"A": 20.0, "B": 5.0}
        print("修订期间报表（锁定第一轮）：", mid_values)

        # 5. 读完 B 提交 -> 版本冲突拒绝发布
        call("POST", f"/api/metric-materialize/rounds/{round2}/reads",
             {"source": "src_b", "partition": "p1"})
        conflict = call("POST",
                        f"/api/metric-materialize/rounds/{round2}/commit")
        assert conflict["ok"] is False and conflict["status"] == "版本冲突"
        print("冲突提交结果：", conflict["conflicts"])

        # 6. 带自动重试的提交：重新读取最新分区范围并确认
        retry_commit = call(
            "POST", f"/api/metric-materialize/rounds/{round2}/commit",
            {"retry": True})
        assert retry_commit["ok"] is True, retry_commit
        assert retry_commit["retried_from_round"] == round2

        final_report = call("GET",
                            f"/api/metric-materialize/rules/{rule_id}/report")
        final_values = {d["group_value"]: d["metric_value"]
                        for d in final_report["details"]}
        assert final_values == {"A": 22.0, "B": 5.0}, final_values
        print("最终报表：", final_values)

        # 7. 复算核对最终轮次与历史第一轮
        verify = call("POST",
                      f"/api/metric-materialize/rounds/{final_report['round_id']}/verify")
        assert verify["ok"] is True, verify
        verify_old = call("POST",
                          f"/api/metric-materialize/rounds/{round1}/verify")
        assert verify_old["ok"] is True
        assert verify_old["recomputed_values"] == {"A": 20.0, "B": 5.0}
        assert verify_old["partition_drift"][0]["source"] == "src_a"

        # 8. 通用资源接口可以查到 A=22、B=5 的物化指标分组明细
        listed = call("GET", "/api/metric_detail")
        assert listed["total"] == 4  # 两轮各两个分组
        latest_rows = [row for row in listed["items"]
                       if row["确认轮次"] == final_report["round_id"]]
        assert {row["分组值"]: row["指标值"] for row in latest_rows} == {
            "A": 22.0, "B": 5.0}
        a_row = next(row for row in latest_rows if row["分组值"] == "A")
        assert a_row["依据事实数"] == 2 and "src_a:p1=3" in a_row["分区版本"]
        print("通用资源明细（最新轮）：",
              [(r["分组值"], r["指标值"], r["分区版本"]) for r in latest_rows])
    finally:
        proc.send_signal(signal.SIGINT)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()

    # 9. 模拟重启：新进程从落盘文件恢复，报表仍读到已确认结果
    assert os.path.exists(state_file)
    sys.path.insert(0, str(BACKEND_DIR))
    from app.metric_store import load_materialized_state  # noqa: E402
    from app.services.metric_materialize import MetricMaterializer  # noqa: E402

    fresh_engine = MetricMaterializer()
    assert load_materialized_state(state_file, engine=fresh_engine)
    restored_report = fresh_engine.get_report(rule_id)
    restored_values = {d["group_value"]: d["metric_value"]
                       for d in restored_report["details"]}
    assert restored_values == {"A": 22.0, "B": 5.0}, restored_values
    again = fresh_engine.ingest_fact(
        "src_a", "p1", "A#order-2", 14, delivery_id="correction-order-2")
    assert again["deduplicated"] is True
    print("重启恢复后报表：", restored_values)

    print("\n全部端到端断言通过")


if __name__ == "__main__":
    main()
