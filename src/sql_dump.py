from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd


TABLE_COLUMNS: dict[str, list[str]] = {
    "feature_krx": [
        "period",
        "ticker",
        "avg_mktcap",
        "float_ratio",
        "gics_sector",
        "krx_group",
        "period_rank",
        "turnover_ratio",
    ],
    "filter_flag": [
        "ticker",
        "managed_date",
        "warning_date",
        "is_managed",
        "is_warning",
        "flag_date",
    ],
    "foreign_holding": [
        "ym",
        "ticker",
        "foreign_holding_qty",
        "foreign_holding_ratio",
        "foreign_limit_qty",
        "foreign_limit_exhaustion_rate",
    ],
    "kospi_friday_daily": [
        "date",
        "ticker",
        "company",
        "close",
        "volume",
        "trading_value",
        "mktcap",
        "shares",
        "mktcap_rank",
    ],
    "labels": [
        "period",
        "ticker",
        "was_member",
        "label_in",
        "label_out",
        "actual_rank",
        "is_member",
    ],
    "major_holder": [
        "period",
        "ticker",
        "major_holder_shares",
        "major_holder_ratio",
        "treasury_shares",
        "treasury_ratio",
        "non_float_ratio",
        "float_rate",
    ],
    "period": ["period", "period_start", "period_end"],
    "sector_map": [
        "ksic_sector",
        "gics_sector_pre2023",
        "gics_sector_2023",
        "krx_group",
    ],
    "stock_meta": [
        "ticker",
        "list_date",
        "is_not_common",
        "is_reits",
        "ksic_sector",
    ],
    "predictions": [
        "id",
        "run_date",
        "period",
        "ticker",
        "company",
        "score",
        "pred_rank",
        "period_rank",
        "pred_top200",
        "strong_in",
        "strong_out",
        "prev_member",
        "model_version",
    ],
}


NUMERIC_TABLE_COLUMNS: dict[str, list[str]] = {
    "feature_krx": ["avg_mktcap", "float_ratio", "period_rank", "turnover_ratio"],
    "filter_flag": ["is_managed", "is_warning"],
    "foreign_holding": [
        "ym",
        "foreign_holding_qty",
        "foreign_holding_ratio",
        "foreign_limit_qty",
        "foreign_limit_exhaustion_rate",
    ],
    "kospi_friday_daily": [
        "date",
        "close",
        "volume",
        "trading_value",
        "mktcap",
        "shares",
        "mktcap_rank",
    ],
    "labels": ["was_member", "label_in", "label_out", "actual_rank", "is_member"],
    "major_holder": [
        "major_holder_shares",
        "major_holder_ratio",
        "treasury_shares",
        "treasury_ratio",
        "non_float_ratio",
        "float_rate",
    ],
    "stock_meta": ["is_not_common", "is_reits"],
    "predictions": [
        "id",
        "score",
        "pred_rank",
        "period_rank",
        "pred_top200",
        "strong_in",
        "strong_out",
        "prev_member",
    ],
}


DATE_TABLE_COLUMNS: dict[str, list[str]] = {
    "filter_flag": ["managed_date", "warning_date"],
    "period": ["period_start", "period_end"],
    "stock_meta": ["list_date"],
    "predictions": ["run_date"],
}


def _split_tuple_chunks(values_blob: str) -> list[str]:
    chunks: list[str] = []
    in_quotes = False
    escape = False
    depth = 0
    start = None

    for idx, char in enumerate(values_blob):
        if escape:
            escape = False
            continue
        if char == "\\":
            escape = True
            continue
        if char == "'":
            in_quotes = not in_quotes
            continue
        if in_quotes:
            continue
        if char == "(":
            if depth == 0:
                start = idx + 1
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0 and start is not None:
                chunks.append(values_blob[start:idx])
                start = None
    return chunks


def _parse_row(chunk: str) -> list[Any]:
    fields: list[tuple[str, bool]] = []
    current: list[str] = []
    in_quotes = False
    escape = False
    field_was_quoted = False

    for char in chunk:
        if escape:
            current.append(char)
            escape = False
            continue
        if char == "\\" and in_quotes:
            escape = True
            continue
        if char == "'":
            in_quotes = not in_quotes
            field_was_quoted = True
            continue
        if char == "," and not in_quotes:
            fields.append(("".join(current), field_was_quoted))
            current = []
            field_was_quoted = False
            continue
        current.append(char)

    fields.append(("".join(current), field_was_quoted))

    parsed: list[Any] = []
    for raw_value, was_quoted in fields:
        value = raw_value.strip()
        if value.upper() == "NULL" and not was_quoted:
            parsed.append(None)
            continue
        if was_quoted:
            parsed.append(value)
            continue
        if re.fullmatch(r"-?\d+", value):
            parsed.append(int(value))
            continue
        if re.fullmatch(r"-?\d+\.\d+", value):
            parsed.append(float(value))
            continue
        parsed.append(value)
    return parsed


@lru_cache(maxsize=1)
def _load_sql_text(sql_dump_path: str) -> str:
    return Path(sql_dump_path).read_text(encoding="utf-8")


def load_table(sql_dump_path: Path, table_name: str) -> pd.DataFrame:
    sql_text = _load_sql_text(str(sql_dump_path))
    pattern = rf"INSERT INTO `{re.escape(table_name)}` VALUES (.*?);\r?\n"
    matches = re.findall(pattern, sql_text, flags=re.S)
    if not matches:
        return pd.DataFrame(columns=TABLE_COLUMNS[table_name])

    rows: list[list[Any]] = []
    for values_blob in matches:
        chunks = _split_tuple_chunks(values_blob)
        rows.extend(_parse_row(chunk) for chunk in chunks)
    frame = pd.DataFrame(rows, columns=TABLE_COLUMNS[table_name])

    for column in NUMERIC_TABLE_COLUMNS.get(table_name, []):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in DATE_TABLE_COLUMNS.get(table_name, []):
        frame[column] = pd.to_datetime(frame[column], errors="coerce")

    return frame


@dataclass
class DataBundle:
    feature_krx: pd.DataFrame
    filter_flag: pd.DataFrame
    foreign_holding: pd.DataFrame
    kospi_friday_daily: pd.DataFrame
    labels: pd.DataFrame
    major_holder: pd.DataFrame
    period: pd.DataFrame
    sector_map: pd.DataFrame
    stock_meta: pd.DataFrame
    predictions: pd.DataFrame
    macro: pd.DataFrame
