"""独立 HTTP 冒烟脚本：对运行中的服务验证完整业务故事（与 pytest 夹具无关）。

用法：先启动 uvicorn，再运行  python tests/manual_smoke.py
"""
from __future__ import annotations

import io
import sys

import os

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000")


def parquet_bytes(columns: dict, types: dict | None = None) -> bytes:
    arrays = {name: pa.array(values, type=(types or {}).get(name)) for name, values in columns.items()}
    buf = io.BytesIO()
    pq.write_table(pa.table(arrays), buf)
    return buf.getvalue()


def main() -> int:
    c = httpx.Client(base_url=BASE, timeout=10)
    assert c.get("/health").status_code == 200

    # 字段版 2：新增“浓度”
    v2 = c.post("/schema/indicators", json={"name": "浓度"}).json()["new_version"]
    print("version with 浓度:", v2)

    # 历史分片
    r = c.post(
        "/ingest/parquet",
        data={"schema_version": str(v2)},
        files={"file": ("h.parquet", parquet_bytes({
            "station": ["S1"],
            "sampled_at": ["2026-01-01T00:00:00Z"],
            "浓度": [11.5],
        }, {"sampled_at": pa.string()}), "application/vnd.apache.parquet")},
    )
    r.raise_for_status()
    print("historical shard:", r.json())

    # 改名 -> 硝酸盐（版 3）；再建“浓度”（版 4，新身份=磷酸盐）
    c.post("/schema/indicators/rename", json={"old_name": "浓度", "new_name": "硝酸盐"}).raise_for_status()
    v4 = c.post("/schema/indicators", json={"name": "浓度"}).json()["new_version"]
    print("current version:", v4)

    # 旧版 2 上传 -> 硝酸盐原身份
    c.post(
        "/ingest/parquet",
        data={"schema_version": str(v2)},
        files={"file": ("old.parquet", parquet_bytes({
            "station": ["S2"], "sampled_at": ["2026-01-02T00:00:00Z"], "浓度": [22.0],
        }, {"sampled_at": pa.string()}), "application/vnd.apache.parquet")},
    ).raise_for_status()

    # 新版 4 上传 -> 磷酸盐新身份
    c.post(
        "/ingest/parquet",
        data={"schema_version": str(v4)},
        files={"file": ("new.parquet", parquet_bytes({
            "station": ["S3"], "sampled_at": ["2026-01-03T00:00:00Z"], "浓度": [33.25],
        }, {"sampled_at": pa.string()}), "application/vnd.apache.parquet")},
    ).raise_for_status()

    page = c.get("/rows").json()
    snap = page["snapshot_id"]
    print("snapshot:", snap, "names:", page["indicator_names"], "rows:", page["total_rows"])
    for row in page["rows"]:
        print(" ", row)

    by = {r["station"]: r for r in page["rows"]}
    assert by["S1"]["硝酸盐"] == 11.5 and by["S1"]["浓度"] is None
    assert by["S2"]["硝酸盐"] == 22.0 and by["S2"]["浓度"] is None
    assert by["S3"]["浓度"] == 33.25 and by["S3"]["硝酸盐"] is None

    # 快照之后删指标 + 上传，快照不变
    c.delete("/schema/indicators/浓度").raise_for_status()
    frozen = c.get("/rows", params={"snapshot_id": snap}).json()
    assert frozen["indicator_names"] == ["硝酸盐", "浓度"]
    assert frozen["total_rows"] == 3
    print("snapshot still frozen after delete+upload:", frozen["indicator_names"], frozen["total_rows"])

    # 已删指标上传必须 409
    rejected = c.post(
        "/ingest/parquet",
        data={"schema_version": str(v4)},
        files={"file": ("x.parquet", parquet_bytes({
            "station": ["X"], "sampled_at": ["2026-01-04T00:00:00Z"], "浓度": [1.0],
        }, {"sampled_at": pa.string()}), "application/vnd.apache.parquet")},
    )
    assert rejected.status_code == 409, rejected.text
    print("deleted-indicator upload rejected:", rejected.json()["detail"])

    # ------------------------------------------------------------------
    # 校准区间 + 合格数据查询
    # ------------------------------------------------------------------
    ind = c.post("/schema/indicators", json={"name": "校准指标"}).json()
    ind_id, v = ind["indicator_id"], ind["new_version"]
    c.post(
        "/ingest/parquet",
        data={"schema_version": str(v)},
        files={"file": ("cal.parquet", parquet_bytes({
            "station": ["Z1", "Z1", "Z1", "Z2"],
            "sampled_at": [
                "2026-02-01T00:00:00Z",   # 校准期前 -> 隔离
                "2026-03-01T00:00:00Z",   # 期内 -> 原值
                "2026-03-15T00:00:00Z",   # 期内但 NULL -> calibrated_null
                "2026-03-01T00:00:00Z",   # 别的站点
            ],
            "校准指标": [99.0, 5.5, None, 7.5],
        }, {"sampled_at": pa.string()}), "application/vnd.apache.parquet")},
    ).raise_for_status()

    cal = c.get(f"/calibrations/Z1/{ind_id}").json()
    assert cal["revision"] == 0 and cal["intervals"] == []
    r = c.put(
        f"/calibrations/Z1/{ind_id}",
        json={"expected_revision": 0, "intervals": [
            {"valid_from": "2026-03-01T00:00:00Z", "valid_to": "2026-04-01T00:00:00Z"},
        ]},
    )
    assert r.status_code == 200, r.text
    assert r.json()["revision"] == 1

    # 过期修订号 -> 409 且状态不变
    stale = c.put(
        f"/calibrations/Z1/{ind_id}",
        json={"expected_revision": 0, "intervals": []},
    )
    assert stale.status_code == 409
    assert c.get(f"/calibrations/Z1/{ind_id}").json()["revision"] == 1

    q = c.get("/qualified-rows", params={"station": "Z1", "indicator_id": ind_id}).json()
    snap_q = q["snapshot_id"]
    print("qualified:", q["calibration_status"], q["calibration_revision"], "rows:", q["total_rows"])
    assert q["calibration_status"] == "configured"
    assert q["calibration_revision"] == 1
    assert q["total_rows"] == 2
    statuses = {row["sampled_at"]: row["status"] for row in q["rows"]}
    assert statuses == {
        "2026-03-01T00:00:00.000000Z": "calibrated",
        "2026-03-15T00:00:00.000000Z": "calibrated_null",
    }
    assert {row["value"] for row in q["rows"]} == {5.5, None}

    # 审计对照：期外值以 uncalibrated 出现
    audit = c.get(
        "/qualified-rows",
        params={"station": "Z1", "indicator_id": ind_id, "include_outside": "true"},
    ).json()
    assert len(audit["rows"]) == 3
    assert "uncalibrated" in {row["status"] for row in audit["rows"]}

    # 原始 /rows 仍返回全部原值，包括校准期外
    raw = c.get("/rows", params={"page_size": 100}).json()
    raw_vals = {(row["station"], row["sampled_at"]): row["校准指标"] for row in raw["rows"]}
    assert raw_vals[("Z1", "2026-02-01T00:00:00.000000Z")] == 99.0

    # 更正校准 + 改名后：旧查询完全冻结，新查询取最新状态
    c.put(
        f"/calibrations/Z1/{ind_id}",
        json={"expected_revision": 1, "intervals": [
            {"valid_from": "2026-02-01T00:00:00Z", "valid_to": "2026-03-10T00:00:00Z"},
        ]},
    ).raise_for_status()
    c.post(
        "/schema/indicators/rename",
        json={"old_name": "校准指标", "new_name": "校准指标改名"},
    ).raise_for_status()

    frozen_q = c.get("/qualified-rows", params={"snapshot_id": snap_q}).json()
    assert frozen_q["indicator_name"] == "校准指标"
    assert frozen_q["calibration_revision"] == 1
    assert frozen_q["total_rows"] == 2

    fresh_q = c.get("/qualified-rows", params={"station": "Z1", "indicator_id": ind_id}).json()
    assert fresh_q["indicator_name"] == "校准指标改名"
    assert fresh_q["calibration_revision"] == 2
    # 新窗口 [02-01, 03-10) 含 02-01(99.0) 与 03-01(5.5)；03-15 的 NULL 已在期外
    assert [row["value"] for row in fresh_q["rows"]] == [99.0, 5.5]
    assert all(row["status"] == "calibrated" for row in fresh_q["rows"])

    print("calibration edit/snapshot freeze verified; docs:",
          c.get("/docs").status_code)

    print("\nSMOKE OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
