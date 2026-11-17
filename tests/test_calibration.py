"""校准有效区间与合格数据查询的自测。

覆盖：
1. 区间整体替换、修订号乐观并发、非法/过期提交不改状态；
2. 有效期内返回原值；原值 NULL 与缺少有效校准分别标注；期外隔离；
3. 首次查询固化分片/指标身份/校准区间，之后更正/改名/同名新建/上传/重启
   都不改变旧查询，新查询读取最新完整状态；
4. 同名新指标不继承旧校准期；已删除指标不能新建合格数据查询；
5. 交错操作（并发）不混用前后版本。
"""
from __future__ import annotations

import threading

import pyarrow as pa
import pytest
from fastapi.testclient import TestClient

from app.db import init_db
from app.main import app


def upload_file(client, path, version: int):
    with open(path, "rb") as fh:
        return client.post(
            "/ingest/parquet",
            data={"schema_version": str(version)},
            files={"file": (path.name, fh.read(), "application/vnd.apache.parquet")},
        )


def make_indicator(client, name: str) -> tuple[int, int]:
    r = client.post("/schema/indicators", json={"name": name})
    assert r.status_code == 201, r.text
    return r.json()["indicator_id"], r.json()["new_version"]


def put_cal(client, station, indicator_id, expected_revision, intervals):
    return client.put(
        f"/calibrations/{station}/{indicator_id}",
        json={
            "expected_revision": expected_revision,
            "intervals": [
                {"valid_from": a, "valid_to": b} for a, b in intervals
            ],
        },
    )


def seed_rows(client, make_parquet, version, s_indicator_name, rows):
    """rows: [(station, sampled_at, value)]，一次上传一个分片。"""
    f = make_parquet(
        f"seed-{rows[0][0]}-{len(rows)}.parquet",
        {
            "station": [r[0] for r in rows],
            "sampled_at": [r[1] for r in rows],
            s_indicator_name: [r[2] for r in rows],
        },
        types={"sampled_at": pa.string()},
    )
    r = upload_file(client, f, version)
    assert r.status_code == 200, r.text
    return r.json()["shard_id"]


# ---------------------------------------------------------------------------
# 校准区间的写入语义
# ---------------------------------------------------------------------------

def test_calibration_replace_requires_matching_revision_and_is_atomic(client):
    ind, _ = make_indicator(client, "浓度")

    # 首次读取：revision 0，无区间
    r = client.get(f"/calibrations/站点A/{ind}")
    assert r.status_code == 200
    assert r.json()["revision"] == 0
    assert r.json()["intervals"] == []

    # revision=0 提交成功 -> revision 1
    r = put_cal(client, "站点A", ind, 0, [("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z")])
    assert r.status_code == 200, r.text
    assert r.json()["revision"] == 1
    assert len(r.json()["intervals"]) == 1

    # 过期修订号 -> 409，状态不变
    r = put_cal(client, "站点A", ind, 0, [("2026-03-01T00:00:00Z", "2026-04-01T00:00:00Z")])
    assert r.status_code == 409
    current = client.get(f"/calibrations/站点A/{ind}").json()
    assert current["revision"] == 1
    assert current["intervals"][0]["valid_from"] == "2026-01-01T00:00:00.000000Z"

    # 未来修订号同样拒绝
    r = put_cal(client, "站点A", ind, 99, [])
    assert r.status_code == 409
    assert client.get(f"/calibrations/站点A/{ind}").json()["revision"] == 1

    # 整体替换（不是追加）：两个区间 -> revision 2
    r = put_cal(
        client, "站点A", ind, 1,
        [
            ("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"),
            ("2026-03-01T00:00:00Z", "2026-04-01T00:00:00Z"),
        ],
    )
    assert r.status_code == 200, r.text
    assert r.json()["revision"] == 2
    assert len(r.json()["intervals"]) == 2

    # 替换为空列表 = 清除，仍推进修订号
    r = put_cal(client, "站点A", ind, 2, [])
    assert r.status_code == 200
    assert r.json()["revision"] == 3
    assert r.json()["intervals"] == []

    # 站点/指标身份隔离：另一个站点仍是 revision 0
    assert client.get(f"/calibrations/站点B/{ind}").json()["revision"] == 0


@pytest.mark.parametrize(
    "intervals",
    [
        [("2026-02-01T00:00:00Z", "2026-02-01T00:00:00Z")],          # 空区间
        [("2026-03-01T00:00:00Z", "2026-02-01T00:00:00Z")],          # 起点晚于终点
        [("2026-01-01T00:00:00Z", "2026-03-01T00:00:00Z"),
         ("2026-02-01T00:00:00Z", "2026-04-01T00:00:00Z")],          # 重叠
        [("2026-01-01T00:00:00Z", "2026-03-01T00:00:00Z"),
         ("2026-02-28T23:59:59Z", "2026-04-01T00:00:00Z")],          # 相差 1 秒也重叠
    ],
)
def test_invalid_intervals_rejected_without_state_change(client, intervals):
    ind, _ = make_indicator(client, "x")
    put_cal(client, "S", ind, 0, [("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z")])

    r = put_cal(client, "S", ind, 1, intervals)
    assert r.status_code == 409, r.text
    state = client.get(f"/calibrations/S/{ind}").json()
    assert state["revision"] == 1
    assert len(state["intervals"]) == 1


def test_adjacent_intervals_allowed_and_fully_covered(client):
    ind, _ = make_indicator(client, "x")
    # 首尾相接合法
    r = put_cal(
        client, "S", ind, 0,
        [
            ("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"),
            ("2026-02-01T00:00:00Z", "2026-03-01T00:00:00Z"),
        ],
    )
    assert r.status_code == 200, r.text
    assert [i["valid_from"] for i in r.json()["intervals"]] == [
        "2026-01-01T00:00:00.000000Z",
        "2026-02-01T00:00:00.000000Z",
    ]


def test_calibration_rejects_unknown_and_deleted_indicator(client, make_parquet):
    assert put_cal(client, "S", 999, 0, []).status_code == 404
    assert client.get("/calibrations/S/999").status_code == 404

    ind, v = make_indicator(client, "x")
    # 先有值、有校准
    seed_rows(client, make_parquet, v, "x", [("S", "2026-01-10T00:00:00Z", 5.0)])
    assert put_cal(client, "S", ind, 0, [("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z")]).status_code == 200
    snap = client.get("/qualified-rows", params={"station": "S", "indicator_id": ind}).json()

    # 删除后不能维护该校准，也不能新建合格数据查询
    assert client.delete("/schema/indicators/x").status_code == 200
    r = put_cal(client, "S", ind, 1, [])
    assert r.status_code == 409
    r = client.get("/qualified-rows", params={"station": "S", "indicator_id": ind})
    assert r.status_code == 409
    assert "已删除" in r.json()["detail"]

    # 删除后既有校准与旧查询仍可读（只读路径不解引用指标状态）
    assert client.get(f"/calibrations/S/{ind}").json()["revision"] == 1
    frozen = client.get("/qualified-rows", params={"snapshot_id": snap["snapshot_id"]}).json()
    assert frozen["total_rows"] == 1


# ---------------------------------------------------------------------------
# 合格数据查询语义
# ---------------------------------------------------------------------------

def test_qualified_query_isolates_outside_window_and_flags_null(client, make_parquet):
    ind, v = make_indicator(client, "浓度")
    seed_rows(client, make_parquet, v, "浓度", [
        ("A", "2025-12-31T00:00:00Z", 9.0),   # 早于校准期（0 秒边界外）
        ("A", "2026-01-01T00:00:00Z", 10.0),  # 起点，含
        ("A", "2026-01-15T00:00:00Z", None),  # 期内但原值 NULL
        ("A", "2026-02-01T00:00:00Z", 11.0),  # 终点，不含 -> 隔离
        ("B", "2026-01-15T00:00:00Z", 20.0),  # 别的站点
    ])
    put_cal(client, "A", ind, 0, [("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z")])

    body = client.get("/qualified-rows", params={"station": "A", "indicator_id": ind}).json()
    assert body["snapshot_created"] is True
    assert body["calibration_status"] == "configured"
    assert body["total_rows"] == 2
    assert body["total_pages"] == 1
    assert [r["sampled_at"] for r in body["rows"]] == [
        "2026-01-01T00:00:00.000000Z",
        "2026-01-15T00:00:00.000000Z",
    ]
    first, null_row = body["rows"]
    assert first["value"] == 10.0 and first["status"] == "calibrated"
    # 期内 NULL：单独状态，且 value 为 null
    assert null_row["value"] is None and null_row["status"] == "calibrated_null"
    assert null_row["value_present"] is False

    # 期外不出现
    assert all(r["sampled_at"] != "2025-12-31T00:00:00.000000Z" for r in body["rows"])
    assert all(r["sampled_at"] != "2026-02-01T00:00:00.000000Z" for r in body["rows"])

    # 审计对照：显式 include_outside=true，期外行以 uncalibrated 标注
    audit = client.get(
        "/qualified-rows",
        params={"station": "A", "indicator_id": ind, "include_outside": "true"},
    ).json()
    statuses = {r["sampled_at"]: r["status"] for r in audit["rows"]}
    assert len(audit["rows"]) == 4
    assert statuses["2025-12-31T00:00:00.000000Z"] == "uncalibrated"
    assert statuses["2026-02-01T00:00:00.000000Z"] == "uncalibrated"
    assert statuses["2026-01-15T00:00:00.000000Z"] == "calibrated_null"


def test_missing_calibration_is_distinct_from_null_value(client, make_parquet):
    ind, v = make_indicator(client, "m")
    seed_rows(client, make_parquet, v, "m", [("S", "2026-01-01T00:00:00Z", None)])

    # 完全没有校准：默认查询返回 0 行，但明确标注 missing
    body = client.get("/qualified-rows", params={"station": "S", "indicator_id": ind}).json()
    assert body["calibration_status"] == "missing"
    assert body["total_rows"] == 0
    assert body["calibration_intervals"] == []

    # 审计模式：原值 NULL 标记为 uncalibrated（不是 calibrated_null）
    audit = client.get(
        "/qualified-rows",
        params={"station": "S", "indicator_id": ind, "include_outside": "true"},
    ).json()
    assert audit["rows"][0]["value"] is None
    assert audit["rows"][0]["status"] == "uncalibrated"

    # 配好校准后，同一 NULL 变成 calibrated_null —— 两种“无值”可区分
    put_cal(client, "S", ind, 0, [("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")])
    body2 = client.get("/qualified-rows", params={"station": "S", "indicator_id": ind}).json()
    assert body2["calibration_status"] == "configured"
    assert body2["rows"][0]["status"] == "calibrated_null"


def test_qualified_query_station_and_indicator_are_required_on_first_call(client):
    ind, _ = make_indicator(client, "x")
    assert client.get("/qualified-rows", params={"indicator_id": ind}).status_code == 409
    assert client.get("/qualified-rows", params={"station": "S"}).status_code == 409
    r = client.get("/qualified-rows", params={"station": "S", "indicator_id": 999})
    assert r.status_code == 404


def test_qualified_pagination_and_snapshot_argument_freezing(client, make_parquet):
    ind, v = make_indicator(client, "p")
    seed_rows(client, make_parquet, v, "p", [
        ("S", f"2026-01-{day:02d}T00:00:00Z", float(day)) for day in range(1, 6)
    ])
    put_cal(client, "S", ind, 0, [("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z")])

    first = client.get(
        "/qualified-rows", params={"station": "S", "indicator_id": ind, "page_size": 2}
    ).json()
    snap = first["snapshot_id"]
    assert first["total_rows"] == 5 and first["total_pages"] == 3
    assert [r["value"] for r in first["rows"]] == [1.0, 2.0]

    second = client.get(
        "/qualified-rows", params={"snapshot_id": snap, "page": 2, "page_size": 2}
    ).json()
    assert [r["value"] for r in second["rows"]] == [3.0, 4.0]

    # 快照查询不接受冲突的站点/指标（以快照为准，避免误用）
    assert client.get(
        "/qualified-rows", params={"snapshot_id": snap, "page_size": 2}
    ).json()["station"] == "S"

    # 翻页时 include_outside 必须一致
    off = client.get(
        "/qualified-rows",
        params={"snapshot_id": snap, "page": 1, "page_size": 2, "include_outside": "true"},
    )
    assert off.status_code == 409

    # 非法 page 边界
    assert client.get(
        "/qualified-rows",
        params={"snapshot_id": snap, "page": 0, "page_size": 2},
    ).status_code == 422


# ---------------------------------------------------------------------------
# 快照固定：校准更正 / 改名 / 同名新建 / 上传 / 重启
# ---------------------------------------------------------------------------

def test_old_query_unchanged_by_calibration_correction_indicator_rename_and_upload(
    client, make_parquet
):
    ind, v = make_indicator(client, "浓度")
    seed_rows(client, make_parquet, v, "浓度", [
        ("S1", "2026-01-10T00:00:00Z", 1.0),
        ("S1", "2026-01-20T00:00:00Z", 2.0),
    ])
    put_cal(client, "S1", ind, 0, [("2026-01-01T00:00:00Z", "2026-01-15T00:00:00Z")])

    old = client.get("/qualified-rows", params={"station": "S1", "indicator_id": ind}).json()
    assert old["snapshot_created"] is True
    snap = old["snapshot_id"]
    assert old["total_rows"] == 1
    assert old["rows"][0]["value"] == 1.0
    assert old["indicator_name"] == "浓度"
    assert old["calibration_revision"] == 1
    assert old["calibration_intervals"] == [
        {"valid_from": "2026-01-01T00:00:00.000000Z", "valid_to": "2026-01-15T00:00:00.000000Z"}
    ]

    # 1) 校准更正：覆盖原窗口，并对 01-20 也开启校准
    assert put_cal(
        client, "S1", ind, 1,
        [("2026-01-20T00:00:00Z", "2026-02-01T00:00:00Z")],
    ).status_code == 200
    # 2) 指标改名
    assert client.post(
        "/schema/indicators/rename", json={"old_name": "浓度", "new_name": "硝酸盐"}
    ).status_code == 200
    # 3) 上传新分片
    cur = client.get("/schema").json()["current_version"]
    seed_rows(client, make_parquet, cur, "硝酸盐", [("S1", "2026-01-25T00:00:00Z", 3.0)])

    # 旧查询完全冻结：分片、指标身份/名称、校准区间、结果
    frozen = client.get(
        "/qualified-rows", params={"snapshot_id": snap, "page_size": 100}
    ).json()
    assert frozen["snapshot_created"] is False
    assert frozen["indicator_name"] == "浓度"
    assert frozen["indicator_id"] == ind
    assert frozen["calibration_revision"] == 1
    assert frozen["calibration_intervals"] == old["calibration_intervals"]
    assert frozen["total_rows"] == 1
    assert frozen["rows"][0]["value"] == 1.0 and frozen["rows"][0]["status"] == "calibrated"

    # 快照元数据接口同样冻结
    meta = client.get(f"/qualified-snapshots/{snap}").json()
    assert meta["indicator_name"] == "浓度"
    assert meta["calibration_revision"] == 1
    assert meta["shard_ids"] == [1]

    # 新查询采用最新完整状态：新名称、新校准（仅 01-20 窗口）、包含新分片
    fresh = client.get("/qualified-rows", params={"station": "S1", "indicator_id": ind}).json()
    assert fresh["snapshot_id"] != snap
    assert fresh["snapshot_created"] is True
    assert fresh["indicator_name"] == "硝酸盐"
    assert fresh["calibration_revision"] == 2
    assert fresh["total_rows"] == 2
    assert [(r["sampled_at"], r["value"]) for r in fresh["rows"]] == [
        ("2026-01-20T00:00:00.000000Z", 2.0),
        ("2026-01-25T00:00:00.000000Z", 3.0),
    ]

    # 原始 /rows 始终返回原值（含校准期外），且不受校准影响
    raw = client.get("/rows", params={"page_size": 100}).json()
    assert raw["total_rows"] == 3
    assert {r["sampled_at"]: r["硝酸盐"] for r in raw["rows"]} == {
        "2026-01-10T00:00:00.000000Z": 1.0,
        "2026-01-20T00:00:00.000000Z": 2.0,
        "2026-01-25T00:00:00.000000Z": 3.0,
    }


def test_new_indicator_with_same_name_does_not_inherit_calibration(client, make_parquet):
    old_ind, v_old = make_indicator(client, "浓度")
    seed_rows(client, make_parquet, v_old, "浓度", [("S", "2026-01-10T00:00:00Z", 7.0)])
    put_cal(client, "S", old_ind, 0, [("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z")])
    old_snap = client.get(
        "/qualified-rows", params={"station": "S", "indicator_id": old_ind}
    ).json()
    assert old_snap["total_rows"] == 1

    # 删除再同名新建：新身份
    client.delete("/schema/indicators/浓度")
    new_ind, v_new = make_indicator(client, "浓度")
    assert new_ind != old_ind
    seed_rows(client, make_parquet, v_new, "浓度", [("S", "2026-01-10T00:00:00Z", 70.0)])

    # 新身份没有任何校准
    assert client.get(f"/calibrations/S/{new_ind}").json()["revision"] == 0
    new_q = client.get(
        "/qualified-rows", params={"station": "S", "indicator_id": new_ind}
    ).json()
    assert new_q["calibration_status"] == "missing"
    assert new_q["total_rows"] == 0
    assert new_q["indicator_id"] == new_ind

    # 老身份的校准与老查询不受影响
    assert client.get(f"/calibrations/S/{old_ind}").json()["revision"] == 1
    old_frozen = client.get(
        "/qualified-rows", params={"snapshot_id": old_snap["snapshot_id"]}
    ).json()
    assert old_frozen["total_rows"] == 1
    assert old_frozen["rows"][0]["value"] == 7.0


def test_qualified_snapshot_survives_restart(client, make_parquet):
    ind, v = make_indicator(client, "x")
    seed_rows(client, make_parquet, v, "x", [("S", "2026-01-10T00:00:00Z", 1.5)])
    put_cal(client, "S", ind, 0, [("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z")])
    snap = client.get(
        "/qualified-rows", params={"station": "S", "indicator_id": ind}
    ).json()["snapshot_id"]

    # 重启后：校准被更正，但旧快照读到的仍是旧区间
    put_cal(client, "S", ind, 1, [("2026-03-01T00:00:00Z", "2026-04-01T00:00:00Z")])
    del client
    init_db()
    with TestClient(app) as client2:
        revived = client2.get(
            "/qualified-rows", params={"snapshot_id": snap}
        ).json()
        assert revived["total_rows"] == 1
        assert revived["rows"][0]["value"] == 1.5
        assert revived["calibration_intervals"][0]["valid_from"] == "2026-01-01T00:00:00.000000Z"

        # 新查询读到最新校准（样本已不在窗口内）
        fresh = client2.get(
            "/qualified-rows", params={"station": "S", "indicator_id": ind}
        ).json()
        assert fresh["calibration_revision"] == 2
        assert fresh["total_rows"] == 0


def test_snapshot_kinds_are_not_interchangeable(client, make_parquet):
    ind, v = make_indicator(client, "x")
    seed_rows(client, make_parquet, v, "x", [("S", "2026-01-10T00:00:00Z", 1.0)])
    raw_snap = client.get("/rows").json()["snapshot_id"]
    qual_snap = client.get(
        "/qualified-rows", params={"station": "S", "indicator_id": ind}
    ).json()["snapshot_id"]

    assert client.get("/rows", params={"snapshot_id": qual_snap}).status_code == 409
    assert client.get(
        "/qualified-rows", params={"snapshot_id": raw_snap}
    ).status_code == 409
    assert client.get("/snapshots/99999").status_code == 404
    assert client.get("/qualified-snapshots/99999").status_code == 404


def test_old_snapshots_query_ignores_other_stations_and_indicator_columns(client, make_parquet):
    """其他站点/其他指标的数据不得混入合格查询（按身份列读取）。"""
    ind_x, v = make_indicator(client, "x")
    ind_y, v2 = make_indicator(client, "y")
    f = make_parquet(
        "two.parquet",
        {
            "station": ["S", "S", "T"],
            "sampled_at": [
                "2026-01-10T00:00:00Z",
                "2026-01-11T00:00:00Z",
                "2026-01-10T00:00:00Z",
            ],
            "x": [1.0, 2.0, 3.0],
            "y": [10.0, 20.0, 30.0],
        },
        types={"sampled_at": pa.string()},
    )
    assert upload_file(client, f, v2).status_code == 200
    put_cal(client, "S", ind_x, 0, [("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z")])

    body = client.get(
        "/qualified-rows", params={"station": "S", "indicator_id": ind_x}
    ).json()
    assert [r["value"] for r in body["rows"]] == [1.0, 2.0]
    assert all(r["station"] == "S" for r in body["rows"])

    body_y = client.get(
        "/qualified-rows", params={"station": "S", "indicator_id": ind_y}
    ).json()
    # y 无校准，默认无行
    assert body_y["calibration_status"] == "missing"
    assert body_y["total_rows"] == 0


# ---------------------------------------------------------------------------
# 交错：校准更正与快照创建并发
# ---------------------------------------------------------------------------

def test_concurrent_calibration_updates_use_consistent_revisions(client, make_parquet):
    """并发更正同一校准：只有成功者推进 revision，最终状态完整可读。"""
    ind, v = make_indicator(client, "c")
    seed_rows(client, make_parquet, v, "c", [("S", "2026-01-10T00:00:00Z", 1.0)])
    put_cal(client, "S", ind, 0, [("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z")])

    successes: list[int] = []
    conflicts: list[int] = []
    errors: list[Exception] = []
    barrier = threading.Barrier(6)

    def worker(k: int):
        try:
            barrier.wait()
            r = put_cal(
                client, "S", ind, 1,
                [(f"2026-0{k + 1}-01T00:00:00Z", f"2026-0{k + 1}-15T00:00:00Z")],
            )
            if r.status_code == 200:
                successes.append(r.json()["revision"])
            elif r.status_code == 409:
                conflicts.append(1)
            else:
                errors.append(AssertionError(r.text))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(k,)) for k in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert len(successes) == 1 and successes[0] == 2
    assert len(conflicts) == 5

    state = client.get(f"/calibrations/S/{ind}").json()
    assert state["revision"] == 2
    assert len(state["intervals"]) == 1


def test_query_racing_calibration_change_never_mixes_versions(client, make_parquet):
    """查询与校准更正并发：每个快照的区间副本必须自洽（要么旧、要么新）。"""
    ind, v = make_indicator(client, "r")
    seed_rows(client, make_parquet, v, "r", [
        ("S", f"2026-01-{d:02d}T00:00:00Z", float(d)) for d in range(1, 11)
    ])
    put_cal(client, "S", ind, 0, [("2026-01-01T00:00:00Z", "2026-01-05T00:00:00Z")])

    stop = threading.Event()
    snapshots: list[dict] = []
    errors: list[Exception] = []

    def updater():
        revision = 1
        try:
            while not stop.is_set():
                start = 1 if revision % 2 == 0 else 6
                r = put_cal(
                    client, "S", ind, revision,
                    [(f"2026-01-{start:02d}T00:00:00Z", f"2026-01-{start + 4:02d}T00:00:00Z")],
                )
                assert r.status_code == 200, r.text
                revision = r.json()["revision"]
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def querier():
        try:
            for _ in range(20):
                body = client.get(
                    "/qualified-rows", params={"station": "S", "indicator_id": ind}
                ).json()
                snapshots.append(body)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    up = threading.Thread(target=updater)
    qs = [threading.Thread(target=querier) for _ in range(3)]
    up.start()
    for t in qs:
        t.start()
    for t in qs:
        t.join()
    stop.set()
    up.join()
    assert not errors

    for body in snapshots:
        # 每个快照内部自洽：单区间 [x, x+4 天)，样本每天一行 -> 恰好 4 行
        assert len(body["calibration_intervals"]) == 1
        start = body["calibration_intervals"][0]["valid_from"]
        expected = 4
        assert body["total_rows"] == expected, (start, body["rows"])
        assert all(r["status"] == "calibrated" for r in body["rows"])
        values = [r["value"] for r in body["rows"]]
        assert values == sorted(values)
        # 值与日期连续且互不越界：不会把两套区间的结果混在一起
        first_day = int(start[8:10])
        assert values == [float(first_day + i) for i in range(expected)]


# ---------------------------------------------------------------------------
# 旧库升级
# ---------------------------------------------------------------------------

def test_legacy_database_is_migrated_in_place(tmp_path, monkeypatch):
    """旧版 snapshots 表没有 kind/station 等列，升级后旧快照可按原始查询读取。"""
    import sqlite3

    import app.db as db

    legacy = tmp_path / "legacy.db"
    conn = sqlite3.connect(legacy)
    conn.executescript(
        """
        CREATE TABLE snapshots (
            snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        );
        INSERT INTO snapshots (snapshot_id) VALUES (1);
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(db, "DB_URL", f"file:{legacy}?uri=true")
    monkeypatch.setattr(db, "DB_PATH", legacy)
    db.init_db()

    conn = db.connect()
    try:
        columns = {r["name"] for r in conn.execute("PRAGMA table_info(snapshots)")}
        assert {
            "kind", "station", "indicator_id",
            "calibration_revision", "include_outside",
        } <= columns
        row = conn.execute(
            "SELECT kind, station FROM snapshots WHERE snapshot_id = 1"
        ).fetchone()
        assert row["kind"] == "rows" and row["station"] is None
        # 新表已建立
        names = {
            r["name"]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert {"calibration_sets", "calibration_intervals", "snapshot_calibrations"} <= names
    finally:
        conn.close()

    with TestClient(app) as legacy_client:
        # 旧快照仍是原始查询类型：空字段/空分片，可正常分页
        body = legacy_client.get("/rows", params={"snapshot_id": 1}).json()
        assert body["snapshot_id"] == 1
        assert body["total_rows"] == 0
        # 旧快照不能当合格查询用
        assert legacy_client.get(
            "/qualified-rows", params={"snapshot_id": 1}
        ).status_code == 409


def test_qualified_pagination_is_globally_time_ordered_across_shards(client, make_parquet):
    """分页顺序按采样时刻全局排序，不依赖上传/分片顺序。"""
    ind, v = make_indicator(client, "g")
    # 故意先传晚的时刻，再传早的时刻（两个分片）
    seed_rows(client, make_parquet, v, "g", [
        ("S", "2026-01-20T00:00:00Z", 20.0),
        ("S", "2026-01-30T00:00:00Z", 30.0),
    ])
    seed_rows(client, make_parquet, v, "g", [
        ("S", "2026-01-10T00:00:00Z", 10.0),
        ("S", "2026-02-05T00:00:00Z", 40.0),
    ])
    put_cal(client, "S", ind, 0, [("2026-01-01T00:00:00Z", "2026-02-10T00:00:00Z")])

    first = client.get(
        "/qualified-rows", params={"station": "S", "indicator_id": ind, "page_size": 3}
    ).json()
    assert [r["value"] for r in first["rows"]] == [10.0, 20.0, 30.0]
    second = client.get(
        "/qualified-rows",
        params={"snapshot_id": first["snapshot_id"], "page": 2, "page_size": 3},
    ).json()
    assert [r["value"] for r in second["rows"]] == [40.0]
