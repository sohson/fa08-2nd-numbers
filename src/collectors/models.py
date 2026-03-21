from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class PriceSnapshot:
    ticker: str
    date: str
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: float | None
    source: str = "yahoo"


@dataclass(frozen=True)
class NaverStockMeta:
    ticker: str
    company: str | None = None
    market: str | None = None
    sector: str | None = None
    industry: str | None = None
    shares_outstanding: float | None = None
    float_rate: float | None = None
    foreign_ratio: float | None = None
    major_holder_ratio: float | None = None
    treasury_ratio: float | None = None
    current_price: float | None = None
    current_volume: float | None = None
    source_main_url: str | None = None
    source_coinfo_url: str | None = None
    source_wisereport_url: str | None = None


@dataclass(frozen=True)
class DartMajorHolder:
    ticker: str
    corp_code: str | None = None
    report_date: str | None = None
    holder_name: str | None = None
    major_holder_ratio: float | None = None
    treasury_ratio: float | None = None
    source: str = "dart"


@dataclass
class CollectionResult:
    source: str
    run_at: datetime
    requested: int
    succeeded: int
    failed: int
    rows: list[dict[str, Any]] = field(default_factory=list)
    errors: list[dict[str, str]] = field(default_factory=list)
