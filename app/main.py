"""FastAPI 入口：化验表字段管理、Parquet 上传与快照分页查询。

并发模型
========
进程内一把可重入锁 ``write_lock`` 保护所有变更（字段改名/删除/新增、上传、
快照创建）。锁内再以 ``BEGIN IMMEDIATE`` 事务裁决，保证字段变更与上传交错时
依据同一个已提交字段状态；任何失败都回滚，不开放半份结果。
"""
from __future__ import annotations

import threading
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import Depends, FastAPI, File, Form, Query, Request, UploadFile
from fastapi.responses import JSONResponse

from . import calibration, catalog, ingest, query
from .config import FIXED_COLUMNS, MAX_INDICATORS
from .db import connect, immediate_transaction, init_db
from .models import (
    CalibrationListOut,
    CalibrationOut,
    CalibrationReplaceRequest,
    CreateIndicatorRequest,
    IndicatorChangeResponse,
    IndicatorOut,
    NotFound,
    QualifiedPage,
    QualifiedSnapshotMeta,
    RenameRequest,
    RowPage,
    SchemaOut,
    SnapshotMeta,
    UploadResponse,
    VersionOut,
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    app.state.write_lock = threading.RLock()
    yield


app = FastAPI(
    title="水质数仓化验表 API",
    version="1.0.0",
    description=(
        "固定站点 + UTC 采样时刻，至多 8 个可空浮点指标的化验表服务。\n\n"
        "- 指标有稳定身份，改名不换身份；同名复用是全新指标，不继承旧值。\n"
        "- 上传 Parquet 必须指定字段版；含已删指标或未知列整次拒绝。\n"
        "- 首次查询固化字段与分片并返回快照号，之后任何变更/上传/重启都不影响该快照。"
    ),
    lifespan=lifespan,
)


def get_lock(request: Request) -> threading.RLock:
    return request.app.state.write_lock


def get_conn():
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


@app.exception_handler(NotFound)
async def not_found_handler(_request: Request, exc: NotFound):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.get("/health", tags=["运维"], summary="健康检查")
def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# 字段目录
# ---------------------------------------------------------------------------

@app.get("/schema", response_model=SchemaOut, tags=["字段管理"], summary="查询当前字段状态")
def get_schema(conn=Depends(get_conn)):
    indicators = [
        IndicatorOut(indicator_id=r["indicator_id"], name=r["name"], active=bool(r["active"]))
        for r in catalog.active_indicators(conn)
    ]
    return SchemaOut(
        current_version=catalog.latest_version(conn),
        fixed_columns=list(FIXED_COLUMNS),
        indicators=indicators,
    )


@app.get(
    "/schema/versions/{version_id}",
    response_model=VersionOut,
    tags=["字段管理"],
    summary="查询某个字段版的不可变映射（含已删指标）",
)
def get_version(version_id: int, conn=Depends(get_conn)):
    rows = catalog.version_mapping(conn, version_id)
    note = catalog.version_note(conn, version_id)  # 不存在会抛 404
    created = conn.execute(
        "SELECT created_at FROM schema_versions WHERE version_id = ?", (version_id,)
    ).fetchone()
    return VersionOut(
        version_id=version_id,
        created_at=created["created_at"],
        change_note=note,
        indicators=[
            {"indicator_id": r["indicator_id"], "name": r["name"], "active": bool(r["active"])}
            for r in rows
        ],
    )


@app.post(
    "/schema/indicators",
    response_model=IndicatorChangeResponse,
    status_code=201,
    tags=["字段管理"],
    summary="新增指标（同名复用为全新身份，不继承旧值）",
)
def create_indicator(
    payload: CreateIndicatorRequest,
    request: Request,
    conn=Depends(get_conn),
    lock=Depends(get_lock),
):
    with lock, immediate_transaction(conn):
        indicator_id, name, new_version = catalog.create_indicator(conn, payload.name)
    return IndicatorChangeResponse(
        indicator_id=indicator_id, name=name, active=True, new_version=new_version
    )


@app.post(
    "/schema/indicators/rename",
    response_model=IndicatorChangeResponse,
    tags=["字段管理"],
    summary="指标改名（身份不变，产生新字段版）",
)
def rename_indicator(
    payload: RenameRequest,
    request: Request,
    conn=Depends(get_conn),
    lock=Depends(get_lock),
):
    with lock, immediate_transaction(conn):
        indicator_id, name, new_version = catalog.rename_indicator(
            conn, payload.old_name, payload.new_name
        )
    return IndicatorChangeResponse(
        indicator_id=indicator_id, name=name, active=True, new_version=new_version
    )


@app.delete(
    "/schema/indicators/{name}",
    response_model=IndicatorChangeResponse,
    tags=["字段管理"],
    summary="删除指标（历史分片保留；再上传含该指标的文件会被整次拒绝）",
)
def delete_indicator(
    name: str,
    request: Request,
    conn=Depends(get_conn),
    lock=Depends(get_lock),
):
    with lock, immediate_transaction(conn):
        indicator_id, new_version = catalog.delete_indicator(conn, name)
    return IndicatorChangeResponse(
        indicator_id=indicator_id, name=name, active=False, new_version=new_version
    )


# ---------------------------------------------------------------------------
# Parquet 上传
# ---------------------------------------------------------------------------

@app.post(
    "/ingest/parquet",
    response_model=UploadResponse,
    tags=["数据接入"],
    summary="上传 Parquet 分片（须指定字段版）",
)
async def upload_parquet(
    request: Request,
    schema_version: int = Form(..., description="该文件遵循的字段版号"),
    file: UploadFile = File(..., description="Parquet 文件，须含 station、sampled_at 两列"),
    conn=Depends(get_conn),
    lock=Depends(get_lock),
):
    file_bytes = await file.read()
    if not file_bytes:
        return JSONResponse(status_code=409, content={"detail": "上传文件为空"})
    with lock, immediate_transaction(conn):
        result = ingest.ingest_upload(conn, file_bytes, schema_version, file.filename or "unknown.parquet")
    return UploadResponse(**result)


# ---------------------------------------------------------------------------
# 查询
# ---------------------------------------------------------------------------

@app.get(
    "/rows",
    response_model=RowPage,
    tags=["查询"],
    summary="分页查询化验数据（首次不带 snapshot_id 即创建快照）",
)
def get_rows(
    request: Request,
    snapshot_id: Optional[int] = Query(None, description="快照号；首次查询留空，服务端创建并返回"),
    page: int = Query(1, ge=1, description="页码，从 1 开始"),
    page_size: int = Query(100, ge=1, le=10000, description="每页行数，1~10000"),
    conn=Depends(get_conn),
    lock=Depends(get_lock),
):
    # 快照创建是写操作，纳入同一把锁；纯翻页（带 snapshot_id）也走锁以获得确定视图
    with lock, immediate_transaction(conn):
        result = query.query_page(conn, snapshot_id, page, page_size)
    return result


@app.get(
    "/snapshots/{snapshot_id}",
    response_model=SnapshotMeta,
    tags=["查询"],
    summary="查看快照固化的字段与分片",
)
def get_snapshot(snapshot_id: int, conn=Depends(get_conn)):
    meta = query.get_snapshot_meta(conn, snapshot_id, expected_kind="rows")
    return SnapshotMeta(
        snapshot_id=meta["snapshot_id"],
        created_at=meta["created_at"],
        indicator_names=meta["indicator_names"],
        shard_ids=meta["shard_ids"],
    )


# ---------------------------------------------------------------------------
# 校准区间（站点 × 指标 ID）
# ---------------------------------------------------------------------------

@app.get(
    "/calibrations",
    response_model=CalibrationListOut,
    tags=["校准期"],
    summary="列出校准状态（可按站点过滤）",
)
def list_calibrations(station: Optional[str] = Query(None), conn=Depends(get_conn)):
    return CalibrationListOut(
        station=station, items=calibration.list_calibrations(conn, station)
    )


@app.get(
    "/calibrations/{station:path}/{indicator_id}",
    response_model=CalibrationOut,
    tags=["校准期"],
    summary="读取某站点 × 指标 ID 的当前校准区间",
)
def get_calibration(station: str, indicator_id: int, conn=Depends(get_conn)):
    return calibration.get_calibration(conn, station, indicator_id)


@app.put(
    "/calibrations/{station:path}/{indicator_id}",
    response_model=CalibrationOut,
    tags=["校准期"],
    summary="整体替换某站点 × 指标 ID 的全部校准区间（须带所见修订号）",
)
def replace_calibration(
    station: str,
    indicator_id: int,
    payload: CalibrationReplaceRequest,
    request: Request,
    conn=Depends(get_conn),
    lock=Depends(get_lock),
):
    with lock, immediate_transaction(conn):
        result = calibration.replace_calibration(
            conn,
            station,
            indicator_id,
            payload.expected_revision,
            [i.model_dump() for i in payload.intervals],
        )
    return result


# ---------------------------------------------------------------------------
# 合格数据查询（校准期内的原值）
# ---------------------------------------------------------------------------

@app.get(
    "/qualified-rows",
    response_model=QualifiedPage,
    tags=["查询"],
    summary="按站点 × 指标 ID 分页查询合格数据（首次不带 snapshot_id 即固化快照）",
)
def get_qualified_rows(
    request: Request,
    station: Optional[str] = Query(None, description="站点；首次查询必填，翻页时可省略"),
    indicator_id: Optional[int] = Query(None, description="指标身份 ID；首次查询必填，翻页时可省略"),
    snapshot_id: Optional[int] = Query(None, description="快照号；首次查询留空，服务端创建并返回"),
    include_outside: Optional[bool] = Query(
        None, description="是否附带校准期外的值用于审计对照（默认 false；翻页须与首次一致）"
    ),
    page: int = Query(1, ge=1, description="页码，从 1 开始"),
    page_size: int = Query(100, ge=1, le=10000, description="每页行数，1~10000"),
    conn=Depends(get_conn),
    lock=Depends(get_lock),
):
    # 快照创建、校准替换、上传共用同一把写锁 + 立即事务：
    # 首次查询看到的是一个完整已提交状态，不会与并发的校准更正交错。
    with lock, immediate_transaction(conn):
        result = query.query_qualified_page(
            conn, snapshot_id, station, indicator_id, page, page_size, include_outside
        )
    return result


@app.get(
    "/qualified-snapshots/{snapshot_id}",
    response_model=QualifiedSnapshotMeta,
    tags=["查询"],
    summary="查看合格数据查询快照固化的指标身份与校准区间",
)
def get_qualified_snapshot(snapshot_id: int, conn=Depends(get_conn)):
    meta = query.get_snapshot_meta(conn, snapshot_id, expected_kind="qualified")
    return QualifiedSnapshotMeta(
        snapshot_id=meta["snapshot_id"],
        created_at=meta["created_at"],
        station=meta["station"],
        indicator_id=meta["indicator_id"],
        indicator_name=meta["indicator_name"],
        calibration_revision=meta["calibration_revision"],
        calibration_intervals=meta["calibration_intervals"],
        calibration_status=(
            "configured" if meta["calibration_intervals"] else "missing"
        ),
        include_outside_calibration=meta["include_outside"],
        shard_ids=meta["shard_ids"],
    )


@app.get("/", include_in_schema=False)
def root():
    return {
        "service": "water-quality-warehouse",
        "docs": "/docs",
        "max_indicators": MAX_INDICATORS,
        "fixed_columns": list(FIXED_COLUMNS),
    }
