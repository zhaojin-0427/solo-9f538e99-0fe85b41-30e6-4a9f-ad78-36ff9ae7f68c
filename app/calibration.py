"""站点 × 指标 ID 的 UTC 校准有效区间。

语义
====
* 身份键是 ``(station, indicator_id)``，与指标当前名字无关：指标改名不影响
  已有校准；同名新建的指标是**新身份**，不会继承旧指标的校准期。
* 一次 PUT 替换该组合的**全部**区间（不是增量修改），并产生一个新的
  ``revision``。请求必须带上调用方上次看到的修订号（``expected_revision``，
  从未设置过为 0），修订号不匹配（过期提交）直接 409，不改任何状态。
* 区间按半开区间 ``[valid_from, valid_to)`` 解释，允许首尾相接，不允许重叠，
  且 ``valid_from < valid_to``；任何非法区间整次拒绝，原状态不变。
* 历史 revision 的区间保留在库中；查询快照会把首次查询时的区间**复制**一份，
  因此后续更正不会改变旧查询。
"""
from __future__ import annotations

import sqlite3
from typing import Iterable, List, Optional, Sequence

from . import catalog
from .models import ConflictError, NotFound
from .timeutil import format_utc_micros, parse_utc_micros

# 防御性上限：单个站点×指标组合最多保存的有效区间数量
MAX_CALIBRATION_INTERVALS = 1000


def normalize_station(raw: str) -> str:
    """站点标识：去首尾空白，不允许为空。"""
    if not isinstance(raw, str):
        raise ConflictError("station 必须是字符串")
    station = raw.strip()
    if not station:
        raise ConflictError("station 不能为空")
    return station


def _load_intervals(
    conn: sqlite3.Connection, station: str, indicator_id: int, revision: int
) -> List[dict]:
    if revision <= 0:
        return []
    rows = conn.execute(
        "SELECT position, valid_from_us, valid_to_us FROM calibration_intervals "
        "WHERE station = ? AND indicator_id = ? AND revision = ? ORDER BY position",
        (station, indicator_id, revision),
    ).fetchall()
    return [
        {
            "position": int(r["position"]),
            "valid_from": format_utc_micros(int(r["valid_from_us"])),
            "valid_to": format_utc_micros(int(r["valid_to_us"])),
        }
        for r in rows
    ]


def _head(conn: sqlite3.Connection, station: str, indicator_id: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT revision, updated_at FROM calibration_sets "
        "WHERE station = ? AND indicator_id = ?",
        (station, indicator_id),
    ).fetchone()


def get_calibration(
    conn: sqlite3.Connection, station_raw: str, indicator_id: int
) -> dict:
    """读取当前校准状态；从未设置时 revision=0、intervals=[]。"""
    station = normalize_station(station_raw)
    indicator = catalog.indicator_by_id(conn, indicator_id)
    if indicator is None:
        raise NotFound(f"指标 {indicator_id} 不存在")
    head = _head(conn, station, indicator_id)
    revision = int(head["revision"]) if head else 0
    updated_at = head["updated_at"] if head else None
    intervals = _load_intervals(conn, station, indicator_id, revision)
    return {
        "station": station,
        "indicator_id": indicator_id,
        "indicator_name": indicator["name"],
        "indicator_active": bool(indicator["active"]),
        "revision": revision,
        "updated_at": updated_at,
        "intervals": intervals,
    }


def _validate_intervals(raw_intervals: Sequence[dict]) -> List[tuple[int, int]]:
    """解析并校验区间：半开、首尾可相接、不得重叠。"""
    if len(raw_intervals) > MAX_CALIBRATION_INTERVALS:
        raise ConflictError(
            f"单个站点×指标最多允许 {MAX_CALIBRATION_INTERVALS} 个校准区间"
        )
    parsed: List[tuple[int, int]] = []
    for index, item in enumerate(raw_intervals):
        try:
            start = parse_utc_micros(item["valid_from"])
            end = parse_utc_micros(item["valid_to"])
        except (KeyError, TypeError) as exc:
            raise ConflictError(f"第 {index + 1} 个校准区间缺少 valid_from / valid_to") from exc
        except ValueError as exc:
            raise ConflictError(f"第 {index + 1} 个校准区间时间非法：{exc}") from exc
        if start >= end:
            raise ConflictError(
                f"第 {index + 1} 个校准区间必须满足 valid_from < valid_to"
            )
        parsed.append((start, end))

    parsed.sort(key=lambda pair: (pair[0], pair[1]))
    for previous, current in zip(parsed, parsed[1:]):
        # 半开区间：previous.end == current.start 合法；严格大于起点则重叠
        if current[0] < previous[1]:
            raise ConflictError(
                "校准区间不得重叠："
                f"[{format_utc_micros(previous[0])}, {format_utc_micros(previous[1])}) 与 "
                f"[{format_utc_micros(current[0])}, {format_utc_micros(current[1])})"
            )
    return parsed


def replace_calibration(
    conn: sqlite3.Connection,
    station_raw: str,
    indicator_id: int,
    expected_revision: int,
    raw_intervals: Sequence[dict],
) -> dict:
    """整体替换某站点×指标的校准区间。

    调用方必须持有进程写锁并处于立即事务内。所有校验在写库之前完成；
    修订号冲突或区间非法时异常退出，由事务回滚，原状态不变。
    """
    station = normalize_station(station_raw)
    indicator = catalog.indicator_by_id(conn, indicator_id)
    if indicator is None:
        raise NotFound(f"指标 {indicator_id} 不存在")
    if not indicator["active"]:
        raise ConflictError(
            f"指标 {indicator_id} 已删除，不能维护校准区间；请先新建指标（新身份）"
        )

    head = _head(conn, station, indicator_id)
    current_revision = int(head["revision"]) if head else 0
    if expected_revision != current_revision:
        raise ConflictError(
            f"校准修订号冲突：提交的 expected_revision={expected_revision}，"
            f"当前 revision={current_revision}；请重新读取后再提交"
        )

    parsed = _validate_intervals(raw_intervals)
    new_revision = current_revision + 1

    conn.execute(
        "INSERT INTO calibration_sets (station, indicator_id, revision) VALUES (?, ?, ?) "
        "ON CONFLICT(station, indicator_id) DO UPDATE SET "
        "revision = excluded.revision, "
        "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')",
        (station, indicator_id, new_revision),
    )
    for position, (start, end) in enumerate(parsed):
        conn.execute(
            "INSERT INTO calibration_intervals "
            "(station, indicator_id, revision, position, valid_from_us, valid_to_us) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (station, indicator_id, new_revision, position, start, end),
        )

    return get_calibration(conn, station, indicator_id)


def list_calibrations(
    conn: sqlite3.Connection, station_raw: Optional[str] = None
) -> List[dict]:
    """列出校准状态；可选按站点过滤。用于运维核对。"""
    params: list = []
    where = ""
    if station_raw is not None:
        station = normalize_station(station_raw)
        where = "WHERE cs.station = ?"
        params.append(station)
    rows = conn.execute(
        "SELECT cs.station, cs.indicator_id, cs.revision, cs.updated_at, "
        "       i.name AS indicator_name, i.active AS indicator_active "
        "FROM calibration_sets cs JOIN indicators i ON i.indicator_id = cs.indicator_id "
        f"{where} ORDER BY cs.station, cs.indicator_id",
        params,
    ).fetchall()
    return [
        {
            "station": r["station"],
            "indicator_id": int(r["indicator_id"]),
            "indicator_name": r["indicator_name"],
            "indicator_active": bool(r["indicator_active"]),
            "revision": int(r["revision"]),
            "updated_at": r["updated_at"],
            "intervals": _load_intervals(
                conn, r["station"], int(r["indicator_id"]), int(r["revision"])
            ),
        }
        for r in rows
    ]


def fetch_head_intervals(
    conn: sqlite3.Connection, station: str, indicator_id: int
) -> tuple[int, List[dict]]:
    """取当前 head 修订号与区间（供创建查询快照时复制）。"""
    head = _head(conn, station, indicator_id)
    revision = int(head["revision"]) if head else 0
    return revision, _load_intervals(conn, station, indicator_id, revision)


def interval_micros(intervals: Iterable[dict]) -> List[tuple[int, int]]:
    """把对外区间（字符串）转为比较用的 UTC 微秒区间，按起点排序。"""
    parsed = [
        (parse_utc_micros(i["valid_from"]), parse_utc_micros(i["valid_to"]))
        for i in intervals
    ]
    parsed.sort()
    return parsed
