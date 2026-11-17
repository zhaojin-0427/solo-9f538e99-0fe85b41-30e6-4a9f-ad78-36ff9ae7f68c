"""UTC 时间的解析与规范化。

校准区间与采样时刻必须用同一种表示比较，这里统一成“UTC 纪元微秒”
（``int``）。对外输出统一为 ``YYYY-MM-DDTHH:MM:SS.ffffffZ``。

约定
====
* 接受带 ``Z`` / ``z`` 后缀或带时区偏移量的 ISO-8601 字符串；
* 不带时区的值按 UTC 解释（与 Parquet 接入的约定一致）；
* 精度为微秒，超出部分由 :func:`datetime.fromisoformat` 截断；
* 仅接受字符串，避免 ``true`` / 数字等歧义输入。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def parse_utc_micros(value: Any) -> int:
    """把 ISO-8601 时间字符串解析为 UTC 纪元微秒。

    非法输入抛 ``ValueError``，由上层转成业务错误（不修改任何状态）。
    """
    if not isinstance(value, str):
        raise ValueError(f"时间必须是 ISO-8601 字符串，实际为 {type(value).__name__}")
    text = value.strip()
    if not text:
        raise ValueError("时间字符串不能为空")
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"非法 ISO-8601 时间：{value!r}") from exc
    if dt.tzinfo is None:
        # 与数据接入约定一致：无时区即 UTC
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    delta = dt - EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def format_utc_micros(us: int) -> str:
    """UTC 纪元微秒 -> 固定宽度 ISO-8601（带 Z）。"""
    dt = EPOCH + timedelta(microseconds=us)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def datetime_to_utc_micros(dt: datetime) -> int:
    """把带/不带时区的 datetime 规范化为 UTC 纪元微秒。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    delta = dt - EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def format_datetime_utc(dt: datetime) -> str:
    """把 datetime 输出为与 ``/rows`` 一致的 UTC 字符串。"""
    return (EPOCH + timedelta(microseconds=datetime_to_utc_micros(dt))).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )
