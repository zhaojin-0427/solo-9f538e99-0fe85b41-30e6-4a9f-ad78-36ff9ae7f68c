"""Pydantic 响应/请求模型与统一错误类型。"""
from __future__ import annotations

from typing import List, Literal, Optional

from fastapi import HTTPException
from pydantic import BaseModel, Field


class ConflictError(HTTPException):
    """业务规则冲突（409）：请求本身合法，但违反数仓不变量。"""

    def __init__(self, detail: str) -> None:
        super().__init__(status_code=409, detail=detail)


class NotFound(HTTPException):
    """资源不存在（404）。"""

    def __init__(self, detail: str) -> None:
        super().__init__(status_code=404, detail=detail)


class IndicatorOut(BaseModel):
    indicator_id: int = Field(..., description="指标稳定身份 ID；改名不变，同名复用会得到新 ID")
    name: str = Field(..., description="当前指标名")
    active: bool


class SchemaOut(BaseModel):
    current_version: int = Field(..., description="当前字段版号")
    fixed_columns: List[str] = Field(..., description="固定字段")
    indicators: List[IndicatorOut] = Field(..., description="当前存续指标（按指标 ID 排序）")


class VersionIndicatorOut(BaseModel):
    indicator_id: int
    name: str
    active: bool


class VersionOut(BaseModel):
    version_id: int
    created_at: str
    change_note: str
    indicators: List[VersionIndicatorOut]


class RenameRequest(BaseModel):
    old_name: str = Field(..., min_length=1, description="当前指标名（例：浓度）")
    new_name: str = Field(..., min_length=1, description="新指标名（例：硝酸盐）")


class CreateIndicatorRequest(BaseModel):
    name: str = Field(..., min_length=1, description="新指标名；若与历史指标同名则视为全新指标，不继承旧值")


class IndicatorChangeResponse(BaseModel):
    indicator_id: int
    name: str
    active: bool
    new_version: int = Field(..., description="本次变更产生的字段版号")


class UploadResponse(BaseModel):
    shard_id: int
    schema_version: int = Field(..., description="本次上传指定并裁决所用的字段版")
    row_count: int
    stored_columns: List[str] = Field(..., description="分片实际落盘的指标身份列")
    file_path: str


class SnapshotMeta(BaseModel):
    snapshot_id: int
    created_at: str
    indicator_names: List[str]
    shard_ids: List[int]


class RowPage(BaseModel):
    snapshot_id: int
    snapshot_created: bool = Field(..., description="本次调用是否新建了快照（首次查询为 true）")
    page: int
    page_size: int
    total_rows: int
    total_pages: int
    fixed_columns: List[str]
    indicator_names: List[str] = Field(..., description="快照固化的当前指标名；旧分片没有的指标为 null")
    rows: List[dict]


# ---------------------------------------------------------------------------
# 校准区间
# ---------------------------------------------------------------------------

class CalibrationInterval(BaseModel):
    """UTC 半开区间 ``[valid_from, valid_to)``；首尾可相接，不可重叠。"""

    valid_from: str = Field(..., description="区间起点（含），ISO-8601，UTC；如 2026-01-01T00:00:00Z")
    valid_to: str = Field(..., description="区间终点（不含），ISO-8601，UTC；必须晚于起点")


class CalibrationReplaceRequest(BaseModel):
    """整体替换某站点×指标的全部校准区间。"""

    expected_revision: int = Field(
        ...,
        ge=0,
        description="提交方所见修订号（从未设置过为 0）；与当前不一致即 409，且不改变状态",
    )
    intervals: List[CalibrationInterval] = Field(
        default_factory=list,
        description="完整区间列表；空列表表示清除该校准，仍会推进修订号",
    )


class CalibrationOut(BaseModel):
    station: str
    indicator_id: int
    indicator_name: str = Field(..., description="指标当前名（仅供展示，身份以 indicator_id 为准）")
    indicator_active: bool
    revision: int = Field(..., description="当前修订号；从未设置时为 0")
    updated_at: Optional[str] = None
    intervals: List[CalibrationInterval]


class CalibrationListOut(BaseModel):
    station: Optional[str] = None
    items: List[CalibrationOut]


# ---------------------------------------------------------------------------
# 合格数据查询
# ---------------------------------------------------------------------------

QualifiedStatus = Literal["calibrated", "calibrated_null", "uncalibrated"]


class QualifiedRow(BaseModel):
    station: str
    sampled_at: str
    value: Optional[float] = Field(
        None, description="原值；未测为 null。注意用 status 区分“原值 NULL”与“缺少有效校准”"
    )
    status: QualifiedStatus = Field(
        ...,
        description=(
            "calibrated=有效期内且有值；calibrated_null=有效期内但原值为 NULL；"
            "uncalibrated=校准期外（默认不返回，仅审计对照时出现）"
        ),
    )
    in_calibration_window: bool
    value_present: bool = Field(..., description="原值是否为非 NULL（等价于 value is not null）")


class QualifiedPage(BaseModel):
    snapshot_id: int
    snapshot_created: bool
    kind: Literal["qualified"] = "qualified"
    station: str
    indicator_id: int
    indicator_name: str
    calibration_revision: int
    calibration_intervals: List[CalibrationInterval]
    calibration_status: Literal["configured", "missing"] = Field(
        ..., description="missing 表示该站点×指标在查询时没有任何有效校准区间"
    )
    include_outside_calibration: bool
    page: int
    page_size: int
    total_rows: int
    total_pages: int
    rows: List[QualifiedRow]


class QualifiedSnapshotMeta(BaseModel):
    snapshot_id: int
    created_at: str
    kind: Literal["qualified"] = "qualified"
    station: str
    indicator_id: int
    indicator_name: str
    calibration_revision: int
    calibration_intervals: List[CalibrationInterval]
    calibration_status: Literal["configured", "missing"]
    include_outside_calibration: bool
    shard_ids: List[int]
