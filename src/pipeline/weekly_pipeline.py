from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
import json

import pandas as pd

from src.collectors import DartCollector, NaverCollector, YahooCollector
from src.config import load_config
from src.pipeline.feature_builder import build_weekly_features
from src.sql_dump import load_table


def get_default_tickers(project_root: Path) -> list[str]:
    sql_path = project_root / "data" / "raw" / "kospi_db_full_20260320.sql"
    feature = load_table(sql_path, "feature_krx")
    if feature.empty:
        return []
    feature["ticker"] = feature["ticker"].astype(str).str.zfill(6)
    feature["period"] = feature["period"].astype(str)
    latest_period = sorted(
        feature["period"].dropna().unique().tolist(),
        key=lambda value: (int(str(value).split("_")[0]), 1 if str(value).endswith("H1") else 2),
    )[-1]
    tickers = (
        feature.loc[feature["period"] == latest_period, "ticker"]
        .drop_duplicates()
        .tolist()
    )
    return tickers


def _apply_sql_fallback_meta(project_root: Path, tickers: list[str], naver_df: pd.DataFrame) -> pd.DataFrame:
    sql_path = project_root / "data" / "raw" / "kospi_db_full_20260320.sql"
    latest_daily = load_table(sql_path, "kospi_friday_daily")
    feature = load_table(sql_path, "feature_krx")

    if latest_daily.empty or feature.empty:
        return naver_df

    latest_daily["ticker"] = latest_daily["ticker"].astype(str).str.zfill(6)
    latest_daily["date"] = pd.to_numeric(latest_daily["date"], errors="coerce")
    latest_daily = (
        latest_daily.sort_values(["ticker", "date"])
        .groupby("ticker", as_index=False)
        .tail(1)
    )

    feature["ticker"] = feature["ticker"].astype(str).str.zfill(6)
    feature["period"] = feature["period"].astype(str)
    latest_period = sorted(
        feature["period"].dropna().unique().tolist(),
        key=lambda value: (int(str(value).split("_")[0]), 1 if str(value).endswith("H1") else 2),
    )[-1]
    feature_latest = feature.loc[feature["period"] == latest_period].copy()
    feature_latest = feature_latest[["ticker", "float_ratio", "gics_sector", "krx_group"]]

    merged = pd.DataFrame({"ticker": tickers})
    if naver_df.empty:
        merged = merged.merge(pd.DataFrame(columns=[
            "ticker", "company", "market", "sector", "industry",
            "shares_outstanding", "float_rate", "current_price", "current_volume",
            "source_main_url", "source_coinfo_url", "source_wisereport_url",
        ]), on="ticker", how="left")
    else:
        merged = merged.merge(naver_df, on="ticker", how="left")
    merged = merged.merge(
        latest_daily[["ticker", "company", "close", "volume", "shares"]],
        on="ticker",
        how="left",
        suffixes=("", "_daily"),
    )
    merged = merged.merge(feature_latest, on="ticker", how="left")

    missing_mask = (
        (
            merged["company"].isna()
            | merged["shares_outstanding"].isna()
            | merged["float_rate"].isna()
            | merged["current_price"].isna()
        )
        & merged["close"].notna()
    )
    if missing_mask.any():
        merged.loc[missing_mask, "company"] = merged.loc[missing_mask, "company_daily"]
        merged.loc[missing_mask, "market"] = "코스피"
        merged.loc[missing_mask, "sector"] = merged.loc[missing_mask, "gics_sector"]
        merged.loc[missing_mask, "industry"] = merged.loc[missing_mask, "krx_group"]
        merged.loc[missing_mask, "shares_outstanding"] = merged.loc[missing_mask, "shares"]
        merged.loc[missing_mask, "float_rate"] = pd.to_numeric(
            merged.loc[missing_mask, "float_ratio"], errors="coerce"
        ) * 100.0
        merged.loc[missing_mask, "current_price"] = merged.loc[missing_mask, "close"]
        merged.loc[missing_mask, "current_volume"] = merged.loc[missing_mask, "volume"]

    return merged[[
        "ticker",
        "company",
        "market",
        "sector",
        "industry",
        "shares_outstanding",
        "float_rate",
        "foreign_ratio",
        "major_holder_ratio",
        "treasury_ratio",
        "current_price",
        "current_volume",
        "source_main_url",
        "source_coinfo_url",
        "source_wisereport_url",
    ]]


def _append_auto_foreign_history(output_dir: Path, price_df: pd.DataFrame, meta_df: pd.DataFrame) -> Path:
    foreign_path = output_dir / "naver_foreign_holding_weekly.csv"
    if price_df.empty or meta_df.empty or "foreign_ratio" not in meta_df.columns:
        return foreign_path

    snapshot_date = pd.to_datetime(price_df["date"], errors="coerce").dropna()
    if snapshot_date.empty:
        return foreign_path
    as_of_date = snapshot_date.max().strftime("%Y-%m-%d")

    foreign_frame = meta_df[["ticker", "foreign_ratio"]].copy()
    foreign_frame["ticker"] = foreign_frame["ticker"].astype(str).str.zfill(6)
    foreign_frame["foreign_holding_ratio"] = pd.to_numeric(foreign_frame["foreign_ratio"], errors="coerce")
    foreign_frame["foreign_limit_exhaustion_rate"] = pd.NA
    foreign_frame["date"] = as_of_date
    foreign_frame = foreign_frame.drop(columns=["foreign_ratio"])
    foreign_frame = foreign_frame.dropna(subset=["foreign_holding_ratio"])

    if foreign_frame.empty:
        return foreign_path

    if foreign_path.exists():
        existing = pd.read_csv(foreign_path, dtype={"ticker": str})
        combined = pd.concat([existing, foreign_frame], ignore_index=True)
    else:
        combined = foreign_frame

    combined["ticker"] = combined["ticker"].astype(str).str.zfill(6)
    combined = combined.drop_duplicates(subset=["date", "ticker"], keep="last")
    combined = combined.sort_values(["date", "ticker"]).reset_index(drop=True)
    combined.to_csv(foreign_path, index=False, encoding="utf-8-sig")
    return foreign_path


def run_weekly_collection(limit: int | None = None) -> dict[str, object]:
    config = load_config()
    tickers = get_default_tickers(config.project_root)
    if limit is not None:
        tickers = tickers[:limit]

    naver = NaverCollector().collect(tickers)
    naver_df = pd.DataFrame(naver.rows)
    naver_df = _apply_sql_fallback_meta(config.project_root, tickers, naver_df)
    market_by_ticker = {}
    naver_price_fallback = {}
    if not naver_df.empty:
        market_by_ticker = dict(zip(naver_df["ticker"], naver_df["market"]))
        naver_price_fallback = {
            row["ticker"]: {"current_price": row.get("current_price"), "current_volume": row.get("current_volume")}
            for row in naver_df.to_dict("records")
        }

    yahoo = YahooCollector().collect(
        tickers,
        market_by_ticker=market_by_ticker,
        naver_price_fallback=naver_price_fallback,
    )
    dart = DartCollector(config.open_dart_api_key).collect(tickers)

    output_dir = config.project_root / "data" / "incoming" / "auto"
    output_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(yahoo.rows).to_csv(output_dir / "yahoo_price_daily.csv", index=False, encoding="utf-8-sig")
    naver_df.to_csv(output_dir / "naver_stock_meta_weekly.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(dart.rows).to_csv(output_dir / "dart_major_holder_weekly.csv", index=False, encoding="utf-8-sig")
    foreign_history_path = _append_auto_foreign_history(output_dir, pd.DataFrame(yahoo.rows), naver_df)

    summary = {
        "tickers": len(tickers),
        "yahoo": asdict(yahoo),
        "naver": asdict(naver),
        "dart": asdict(dart),
        "foreign_history_path": str(foreign_history_path),
    }
    try:
        summary["feature_build"] = build_weekly_features()
    except Exception as exc:
        summary["feature_build"] = {"error": str(exc)}
    (output_dir / "weekly_collection_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return summary
