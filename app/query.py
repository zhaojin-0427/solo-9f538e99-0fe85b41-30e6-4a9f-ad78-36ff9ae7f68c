"""查询：首次查询固化字段与分片生成快照，之后按快照分页合读。

快照语义
========
* 首次查询（不带 ``snapshot_id``）把*当前字段名集合*与*当前全部分片*
  写入 ``snapshots`` / ``snapshot_fields`` / ``snapshot_shards``。
* 之后的分页、字段改名/删除/新增、新上传乃至进程重启，都不会改变该快照
  看到的字段与分片；读取完全以快照表为准。
* 合读按指标身份列对齐：新增指标在旧分片为 NULL；同名复用得到新身份，
  旧分片没有该身份列，同样为 NULL，不会继承旧指标的值。

合格数据查询（``/qualified-rows``）
===================================
* 快照额外固化站点、指标身份、指标名与**校准区间副本**
  （``snapshot_calibrations``）。此后校准区间更正、指标改名、同名新建、
  上传、重启都不改变旧查询；新查询按最新完整状态另建快照。
* 快照创建与校准替换在同一把写锁 + 立即事务内串行，交错操作不会混用版本。
"""
from __future__ import annotations

import sqlite3
from typing import List, Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from . import calibration as calibration_mod
from .config import FIXED_COLUMNS, INDICATOR_COLUMN_PREFIX, SAMPLED_AT_COLUMN, STATION_COLUMN
from .models import ConflictError, NotFound
from .timeutil import format_utc_micros, parse_utc_micros


def create_snapshot(conn: sqlite3.Connection) -> dict:
    """在调用方写事务内创建原始数据快照，固化当前字段与当前全部分片。"""
    cur = conn.execute("INSERT INTO snapshots (kind) VALUES ('rows')")
    snapshot_id = int(cur.lastrowid)

    fields = conn.execute(
        "SELECT indicator_id, name FROM indicators WHERE active = 1 ORDER BY indicator_id"
    ).fetchall()
    for position, row in enumerate(fields):
        conn.execute(
            "INSERT INTO snapshot_fields (snapshot_id, indicator_id, name, position) "
            "VALUES (?, ?, ?, ?)",
            (snapshot_id, row["indicator_id"], row["name"], position),
        )

    shards = conn.execute("SELECT shard_id FROM shards ORDER BY shard_id").fetchall()
    for position, row in enumerate(shards):
        conn.execute(
            "INSERT INTO snapshot_shards (snapshot_id, shard_id, position) VALUES (?, ?, ?)",
            (snapshot_id, row["shard_id"], position),
        )

    created_at = conn.execute(
        "SELECT created_at FROM snapshots WHERE snapshot_id = ?", (snapshot_id,)
    ).fetchone()["created_at"]

    return {
        "snapshot_id": snapshot_id,
        "created_at": created_at,
        "indicator_names": [r["name"] for r in fields],
        "indicator_ids": [r["indicator_id"] for r in fields],
        "shard_ids": [r["shard_id"] for r in shards],
    }


def create_qualified_snapshot(
    conn: sqlite3.Connection, station: str, indicator_id: int, include_outside: bool = False
) -> dict:
    """创建合格数据查询快照：固化分片、指标身份/名称与校准区间副本。

    调用方须持有写锁并处于立即事务内；与校准替换相互串行，保证“首次查询”
    看到的是一个完整已提交状态。
    """
    station = calibration_mod.normalize_station(station)
    indicator = conn.execute(
        "SELECT indicator_id, name, active FROM indicators WHERE indicator_id = ?",
        (indicator_id,),
    ).fetchone()
    if indicator is None:
        raise NotFound(f"指标 {indicator_id} 不存在")
    if not indicator["active"]:
        raise ConflictError(
            f"指标 {indicator_id}（{indicator['name']!r}）已删除，不能新建合格数据查询"
        )

    revision, intervals = calibration_mod.fetch_head_intervals(
        conn, station, indicator_id
    )
    # 区间在事务内已排好序；重新解析一次既做规范化也保留微秒精度
    interval_micros = calibration_mod.interval_micros(intervals)

    cur = conn.execute(
        "INSERT INTO snapshots (kind, station, indicator_id, calibration_revision, include_outside) "
        "VALUES ('qualified', ?, ?, ?, ?)",
        (station, indicator_id, revision, 1 if include_outside else 0),
    )
    snapshot_id = int(cur.lastrowid)

    conn.execute(
        "INSERT INTO snapshot_fields (snapshot_id, indicator_id, name, position) "
        "VALUES (?, ?, ?, 0)",
        (snapshot_id, indicator_id, indicator["name"]),
    )

    shards = conn.execute("SELECT shard_id FROM shards ORDER BY shard_id").fetchall()
    for position, row in enumerate(shards):
        conn.execute(
            "INSERT INTO snapshot_shards (snapshot_id, shard_id, position) VALUES (?, ?, ?)",
            (snapshot_id, row["shard_id"], position),
        )

    for position, (start, end) in enumerate(interval_micros):
        conn.execute(
            "INSERT INTO snapshot_calibrations "
            "(snapshot_id, indicator_id, revision, position, valid_from_us, valid_to_us) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (snapshot_id, indicator_id, revision, position, start, end),
        )

    created_at = conn.execute(
        "SELECT created_at FROM snapshots WHERE snapshot_id = ?", (snapshot_id,)
    ).fetchone()["created_at"]
    return {
        "snapshot_id": snapshot_id,
        "created_at": created_at,
        "kind": "qualified",
        "station": station,
        "indicator_id": indicator_id,
        "indicator_name": indicator["name"],
        "calibration_revision": revision,
        "calibration_intervals": intervals,
        "include_outside": bool(include_outside),
        "shard_ids": [r["shard_id"] for r in shards],
    }


def get_snapshot_meta(
    conn: sqlite3.Connection, snapshot_id: int, expected_kind: str = "rows"
) -> dict:
    snap = conn.execute(
        "SELECT snapshot_id, created_at, kind, station, indicator_id, "
        "       calibration_revision, include_outside "
        "FROM snapshots WHERE snapshot_id = ?",
        (snapshot_id,),
    ).fetchone()
    if snap is None:
        raise NotFound(f"快照 {snapshot_id} 不存在")
    if snap["kind"] != expected_kind:
        raise ConflictError(
            f"快照 {snapshot_id} 是 {snap['kind']!r} 类型，不能用于 {expected_kind!r} 查询"
        )
    fields = conn.execute(
        "SELECT indicator_id, name FROM snapshot_fields "
        "WHERE snapshot_id = ? ORDER BY position",
        (snapshot_id,),
    ).fetchall()
    shards = conn.execute(
        "SELECT shard_id FROM snapshot_shards WHERE snapshot_id = ? ORDER BY position",
        (snapshot_id,),
    ).fetchall()

    meta = {
        "snapshot_id": snapshot_id,
        "created_at": snap["created_at"],
        "kind": snap["kind"],
        "indicator_names": [r["name"] for r in fields],
        "indicator_ids": [r["indicator_id"] for r in fields],
        "shard_ids": [r["shard_id"] for r in shards],
    }
    if snap["kind"] == "qualified":
        if len(fields) != 1:
            raise ConflictError(f"快照 {snapshot_id} 的指标固化为空，数据已损坏")
        cal_rows = conn.execute(
            "SELECT revision, position, valid_from_us, valid_to_us "
            "FROM snapshot_calibrations WHERE snapshot_id = ? ORDER BY position",
            (snapshot_id,),
        ).fetchall()
        meta.update(
            {
                "station": snap["station"],
                "indicator_id": int(snap["indicator_id"]),
                "indicator_name": fields[0]["name"],
                "calibration_revision": int(snap["calibration_revision"] or 0),
                "include_outside": bool(snap["include_outside"]),
                "calibration_intervals": [
                    {
                        "valid_from": format_utc_micros(int(r["valid_from_us"])),
                        "valid_to": format_utc_micros(int(r["valid_to_us"])),
                    }
                    for r in cal_rows
                ],
            }
        )
    return meta


def _format_timestamp(value) -> str:
    # Arrow 转出来的是带 tzinfo 的 datetime（UTC），统一输出带微秒与 Z 后缀的 ISO-8601
    if value is None:
        return None
    return value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _read_shard_window(
    file_path: str,
    indicator_ids: List[int],
    skip: int,
    take: int,
) -> list[dict]:
    """读取单个分片的指定窗口，返回窗口行（已按站点、采样时刻稳定排序）。

    缺失的指标身份列补 NULL；只物化窗口区间，避免对无关分片全量读取。
    """
    table = pq.read_table(file_path)
    shard_total = table.num_rows

    # 固定列
    station = table.column(STATION_COLUMN).combine_chunks()
    sampled = table.column(SAMPLED_AT_COLUMN).combine_chunks()

    # 指标身份列；旧分片没有的身份 -> 全 NULL 列
    columns: dict[str, pa.ChunkedArray] = {
        STATION_COLUMN: pa.chunked_array([station]),
        SAMPLED_AT_COLUMN: pa.chunked_array([sampled]),
    }
    for ind_id in indicator_ids:
        canonical = f"{INDICATOR_COLUMN_PREFIX}{ind_id}"
        if canonical in table.column_names:
            columns[canonical] = table.column(canonical)
        else:
            columns[canonical] = pa.chunked_array(
                [pa.nulls(shard_total, type=pa.float64())]
            )

    aligned = pa.table(columns)

    # 稳定排序，保证分页确定性
    order = pa.compute.sort_indices(
        aligned,
        sort_keys=[(STATION_COLUMN, "ascending"), (SAMPLED_AT_COLUMN, "ascending")],
    )
    aligned = aligned.take(order)

    window = aligned.slice(skip, take) if skip < shard_total else aligned.slice(0, 0)

    rows: list[dict] = []
    stations = window.column(STATION_COLUMN).to_pylist()
    times = window.column(SAMPLED_AT_COLUMN).to_pylist()
    indicator_py = {
        ind_id: window.column(f"{INDICATOR_COLUMN_PREFIX}{ind_id}").to_pylist()
        for ind_id in indicator_ids
    }
    for i in range(window.num_rows):
        row = {
            STATION_COLUMN: stations[i],
            SAMPLED_AT_COLUMN: _format_timestamp(times[i]),
        }
        for ind_id in indicator_ids:
            row[INDICATOR_COLUMN_PREFIX + str(ind_id)] = indicator_py[ind_id][i]
        rows.append(row)
    return rows


def query_page(
    conn: sqlite3.Connection,
    snapshot_id: Optional[int],
    page: int,
    page_size: int,
) -> dict:
    """执行分页查询。``snapshot_id`` 为空时先创建快照。"""
    if page < 1:
        raise ConflictError("page 必须 >= 1")
    if page_size < 1:
        raise ConflictError("page_size 必须 >= 1")

    created = False
    if snapshot_id is None:
        meta = create_snapshot(conn)
        created = True
    else:
        meta = get_snapshot_meta(conn, snapshot_id)

    sid = meta["snapshot_id"]
    indicator_ids = meta["indicator_ids"]
    indicator_names = meta["indicator_names"]
    shard_ids = meta["shard_ids"]

    # 总分片行数（快照固化后是定值）
    if shard_ids:
        placeholders = ",".join("?" for _ in shard_ids)
        total_rows = conn.execute(
            f"SELECT COALESCE(SUM(row_count), 0) AS n FROM shards WHERE shard_id IN ({placeholders})",
            shard_ids,
        ).fetchone()["n"]
    else:
        total_rows = 0
    total_pages = (total_rows + page_size - 1) // page_size if total_rows else 0

    rows: list[dict] = []
    if total_rows:
        offset = (page - 1) * page_size
        remaining = page_size
        # 快照分片顺序固定；逐分片跳过/取窗
        cumulative = 0
        for shard_id in shard_ids:
            shard_path = conn.execute(
                "SELECT file_path, row_count FROM shards WHERE shard_id = ?",
                (shard_id,),
            ).fetchone()
            if shard_path is None:
                # 快照引用的分片理论上不会被删；防御性处理
                raise NotFound(f"快照引用的分片 {shard_id} 已丢失")
            shard_total = shard_path["row_count"]
            start = cumulative
            end = cumulative + shard_total
            cumulative = end

            if offset >= end or remaining <= 0:
                continue
            local_skip = max(offset - start, 0)
            local_take = min(remaining, shard_total - local_skip)
            part = _read_shard_window(
                shard_path["file_path"], indicator_ids, local_skip, local_take
            )
            rows.extend(part)
            remaining -= len(part)

    # 把身份列键替换为快照固化的当前指标名
    name_by_id = dict(zip(indicator_ids, indicator_names))
    named_rows: list[dict] = []
    for row in rows:
        named = {STATION_COLUMN: row[STATION_COLUMN], SAMPLED_AT_COLUMN: row[SAMPLED_AT_COLUMN]}
        for ind_id in indicator_ids:
            named[name_by_id[ind_id]] = row[INDICATOR_COLUMN_PREFIX + str(ind_id)]
        named_rows.append(named)

    return {
        "snapshot_id": sid,
        "snapshot_created": created,
        "page": page,
        "page_size": page_size,
        "total_rows": total_rows,
        "total_pages": total_pages,
        "fixed_columns": list(FIXED_COLUMNS),
        "indicator_names": indicator_names,
        "rows": named_rows,
    }


# ---------------------------------------------------------------------------
# 合格数据查询
# ---------------------------------------------------------------------------



# 行状态（互斥）：
#   calibrated      —— 采样时刻落在某个有效校准区间内，且原值非 NULL
#   calibrated_null —— 采样时刻落在有效校准区间内，但原值为 NULL（水样未测）
#   uncalibrated    —— 采样时刻不在任何有效校准区间内（校准期外，默认被隔离）
QUALIFIED_ROW_STATUSES = ("calibrated", "calibrated_null", "uncalibrated")


def _in_intervals(sampled_us: int, intervals: List[tuple[int, int]]) -> bool:
    # 区间数量受上限约束，线性判定即可；半开区间 [from, to)
    for start, end in intervals:
        if start <= sampled_us < end:
            return True
    return False


def _sorted_shard_columns(table: pa.Table) -> dict:
    """按 (station, sampled_at) 稳定排序后的站点、时刻（微秒）与排序索引。"""
    sampled_col = table.column(SAMPLED_AT_COLUMN).combine_chunks()
    order = pc.sort_indices(
        table.select([STATION_COLUMN]).append_column(SAMPLED_AT_COLUMN, sampled_col),
        sort_keys=[(STATION_COLUMN, "ascending"), (SAMPLED_AT_COLUMN, "ascending")],
    )
    stations = table.column(STATION_COLUMN).combine_chunks().take(order).to_pylist()
    if sampled_col.type == pa.timestamp("us", tz="UTC"):
        times_us = sampled_col.cast(pa.int64()).take(order).to_pylist()
    else:
        # 接入时已统一为 timestamp('us', tz='UTC')；这里防御性转换
        times_us = (
            sampled_col.cast(pa.timestamp("us", tz="UTC"))
            .cast(pa.int64())
            .take(order)
            .to_pylist()
        )
    return {"stations": stations, "times_us": times_us, "order": order}


def query_qualified_page(
    conn: sqlite3.Connection,
    snapshot_id: Optional[int],
    station: Optional[str],
    indicator_id: Optional[int],
    page: int,
    page_size: int,
    include_outside: Optional[bool] = None,
) -> dict:
    """按站点 + 指标 ID 分页查询合格数据。

    ``snapshot_id`` 为空时以“当前完整状态”（站点/指标身份/校准区间）新建快照；
    之后翻页完全依赖该快照固化的内容，任何后续变更都不影响结果。

    默认只返回“该指标确实存在于分片中、采样时刻落在有效校准区间内”的行。
    校准期外的化验值被隔离；``include_outside=true`` 仅用于审计对照，会把它们
    一并返回并显式标注 ``status='uncalibrated'``。原值为 NULL 的行单独标注，
    与“缺少有效校准”严格区分。
    """
    if page < 1:
        raise ConflictError("page 必须 >= 1")
    if page_size < 1:
        raise ConflictError("page_size 必须 >= 1")

    if snapshot_id is None:
        if station is None or not station.strip():
            raise ConflictError("首次查询必须提供 station")
        if indicator_id is None:
            raise ConflictError("首次查询必须提供 indicator_id")
        meta = create_qualified_snapshot(
            conn, station, indicator_id, include_outside=bool(include_outside)
        )
        created = True
    else:
        meta = get_snapshot_meta(conn, snapshot_id, expected_kind="qualified")
        created = False
        if include_outside is not None and include_outside != meta["include_outside"]:
            raise ConflictError(
                "include_outside 在翻页时必须与首次查询一致；"
                f"该快照为 {meta['include_outside']}，本次提交 {include_outside}"
            )
        # 翻页时如显式带上了站点/指标，必须与快照一致，避免把 A 站的页码用到 B 站
        if station is not None and station.strip() != meta["station"]:
            raise ConflictError(
                f"快照 {snapshot_id} 的站点为 {meta['station']!r}，与本次提交的 {station!r} 不一致"
            )
        if indicator_id is not None and indicator_id != meta["indicator_id"]:
            raise ConflictError(
                f"快照 {snapshot_id} 的指标为 {meta['indicator_id']}，与本次提交的 {indicator_id} 不一致"
            )

    sid = meta["snapshot_id"]
    station = meta["station"]
    indicator_id = meta["indicator_id"]
    indicator_name = meta["indicator_name"]
    include_outside = meta["include_outside"]
    intervals = [
        (parse_utc_micros(i["valid_from"]), parse_utc_micros(i["valid_to"]))
        for i in meta["calibration_intervals"]
    ]
    canonical = f"{INDICATOR_COLUMN_PREFIX}{indicator_id}"
    shard_ids = meta["shard_ids"]

    selected: list[dict] = []
    for shard_id in shard_ids:
        # 只扫描确实含有该指标身份列的分片：旧分片没有该列表示“该指标在此分片
        # 不存在”，不是“原值为 NULL”。分片集合由快照固化。
        shard_row = conn.execute(
            "SELECT s.file_path FROM shards s "
            "JOIN shard_indicators si ON si.shard_id = s.shard_id "
            "WHERE s.shard_id = ? AND si.indicator_id = ?",
            (shard_id, indicator_id),
        ).fetchone()
        if shard_row is None:
            continue
        table = pq.read_table(shard_row["file_path"])
        shard_total = table.num_rows
        if not shard_total or canonical not in table.column_names:
            continue

        sorted_cols = _sorted_shard_columns(table)
        stations = sorted_cols["stations"]
        times_us = sorted_cols["times_us"]
        order = sorted_cols["order"]
        values = table.column(canonical).combine_chunks().take(order).to_pylist()

        for i in range(shard_total):
            if stations[i] != station:
                continue
            sampled_us = times_us[i]
            in_window = _in_intervals(sampled_us, intervals)
            if not in_window and not include_outside:
                # 校准期外的化验值从分析供数中隔离（原始记录仍在 /rows 与分片中）
                continue
            value = values[i]
            if not in_window:
                status = "uncalibrated"
            elif value is None:
                status = "calibrated_null"
            else:
                status = "calibrated"
            selected.append(
                {
                    "sampled_us": sampled_us,
                    STATION_COLUMN: stations[i],
                    SAMPLED_AT_COLUMN: format_utc_micros(sampled_us),
                    "value": value,
                    "status": status,
                    "in_calibration_window": in_window,
                    "value_present": value is not None,
                }
            )

    # 跨分片按采样时刻全局稳定排序：分页边界不依赖上传顺序
    selected.sort(key=lambda item: (item["sampled_us"], item[STATION_COLUMN]))
    for item in selected:
        del item["sampled_us"]

    total_rows = len(selected)
    total_pages = (total_rows + page_size - 1) // page_size if total_rows else 0
    offset = (page - 1) * page_size
    window_rows = selected[offset : offset + page_size]

    return {
        "snapshot_id": sid,
        "snapshot_created": created,
        "kind": "qualified",
        "station": station,
        "indicator_id": indicator_id,
        "indicator_name": indicator_name,
        "calibration_revision": meta["calibration_revision"],
        "calibration_intervals": meta["calibration_intervals"],
        "calibration_status": "configured" if meta["calibration_intervals"] else "missing",
        "include_outside_calibration": include_outside,
        "page": page,
        "page_size": page_size,
        "total_rows": total_rows,
        "total_pages": total_pages,
        "rows": window_rows,
    }
