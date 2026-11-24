"""请求/响应模型 (OpenAPI 文档驱动 /docs 录入)。"""
from __future__ import annotations

from pydantic import BaseModel, Field


class ScanCreate(BaseModel):
    scan_id: str = Field(..., min_length=1, description="本次扫码的唯一编号",
                         examples=["S001"])
    part_id: str = Field(..., min_length=1, description="工件号", examples=["P100"])
    station: str = Field(..., min_length=1, description="扫码工位",
                         examples=["A"])
    ts_utc: str = Field(
        ...,
        description="UTC 时刻, ISO8601 (如 2026-11-24T01:30:00Z 或带偏移量)",
        examples=["2026-11-24T01:30:00Z"],
    )


class ScanUpdate(BaseModel):
    station: str | None = Field(
        None, min_length=1, description="新工位; 不传表示不改工位"
    )
    ts_utc: str | None = Field(
        None,
        description="新 UTC 时刻 (ISO8601); 不传表示不改时刻",
    )


class ScanOut(BaseModel):
    scan_id: str
    part_id: str
    station: str
    ts_utc: str
    day_utc: str


class WithdrawOut(ScanOut):
    withdrawn: bool


class FlowCount(BaseModel):
    from_station: str
    to_station: str
    part_count: int = Field(..., description="工件数 (同件同流向只计一次)")
    event_count: int = Field(..., description="流向发生次数 (含同件重复)")


class FlowIndexRow(BaseModel):
    part_id: str
    day_utc: str
    from_station: str
    to_station: str
    ref_count: int


class FlowEventRow(BaseModel):
    next_scan_id: str = Field(..., description="后一次扫码编号")
    part_id: str
    day_utc: str = Field(..., description="归入的 UTC 日期 (后一次扫码)")
    from_station: str
    to_station: str
    prev_scan_id: str = Field(..., description="前一次扫码编号")
    prev_ts_utc: str
    next_ts_utc: str


class TraceEndpoint(BaseModel):
    prev_scan_id: str = Field(..., description="前一次 (来源) 扫码编号")
    prev_ts_utc: str = Field(..., description="前一次扫码 UTC 时刻 (可早于查询日)")
    next_scan_id: str = Field(..., description="后一次 (目的) 扫码编号")
    next_ts_utc: str


class FlowDirectionTrace(BaseModel):
    from_station: str
    to_station: str
    count: int = Field(..., description="该件该 UTC 日期该方向的实际流转次数")
    first: TraceEndpoint = Field(..., description="按 (后一次时刻, 编号) 的首笔流转")
    last: TraceEndpoint = Field(..., description="按 (后一次时刻, 编号) 的末笔流转")


class FlowTrace(BaseModel):
    part_id: str
    day_utc: str
    directions: list[FlowDirectionTrace]
