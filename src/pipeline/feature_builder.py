from __future__ import annotations

from pathlib import Path
import pickle

import pandas as pd

from src.config import load_config
from src.sql_dump import load_table


def _period_sort_key(period_value: str) -> tuple[int, int]:
    year_text, half_text = str(period_value).split("_")
    half_rank = 1 if half_text == "H1" else 2
    return int(year_text), half_rank


def get_active_period(period_frame: pd.DataFrame, as_of: pd.Timestamp | None = None) -> str:
    if period_frame.empty:
        raise ValueError("period table is empty")

    current_ts = pd.Timestamp.now(tz="Asia/Seoul").normalize() if as_of is None else pd.Timestamp(as_of).normalize()
    current_naive = current_ts.tz_localize(None) if current_ts.tzinfo else current_ts

    candidates = period_frame.copy()
    candidates["period_start"] = pd.to_datetime(candidates["period_start"], errors="coerce")
    candidates["period_end"] = pd.to_datetime(candidates["period_end"], errors="coerce")

    active = candidates[
        (candidates["period_start"].notna())
        & (candidates["period_end"].notna())
        & (candidates["period_start"] <= current_naive)
        & (candidates["period_end"] >= current_naive)
    ]
    if not active.empty:
        active = active.sort_values("period", key=lambda s: s.map(_period_sort_key))
        return str(active.iloc[-1]["period"])

    candidates = candidates.sort_values("period", key=lambda s: s.map(_period_sort_key))
    return str(candidates.iloc[-1]["period"])


def _load_auto_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _get_previous_period(period_frame: pd.DataFrame, target_period: str) -> str | None:
    ordered = sorted(period_frame["period"].astype(str).tolist(), key=_period_sort_key)
    if target_period not in ordered:
        return ordered[-1] if ordered else None
    index = ordered.index(target_period)
    return ordered[index - 1] if index > 0 else None


def _load_model_package(model_path: Path) -> dict[str, object]:
    with model_path.open("rb") as handle:
        return pickle.load(handle)


def _period_month_bounds(period_row: pd.Series) -> tuple[int, int]:
    start = pd.to_datetime(period_row["period_start"], errors="coerce")
    end = pd.to_datetime(period_row["period_end"], errors="coerce")
    if pd.isna(start) or pd.isna(end):
        raise ValueError("invalid period bounds")
    return int(start.strftime("%Y%m")), int(end.strftime("%Y%m"))


def _build_foreign_aggregate(
    foreign_holding: pd.DataFrame,
    foreign_holding_auto: pd.DataFrame,
    period_frame: pd.DataFrame,
    target_period: str,
    prev_period: str | None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    if not foreign_holding.empty:
        foreign = foreign_holding.copy()
        foreign["ticker"] = foreign["ticker"].astype(str).str.zfill(6)
        foreign["obs_date"] = pd.to_datetime(
            pd.to_numeric(foreign["ym"], errors="coerce").astype("Int64").astype(str) + "01",
            format="%Y%m%d",
            errors="coerce",
        )
        foreign["foreign_holding_ratio"] = pd.to_numeric(foreign["foreign_holding_ratio"], errors="coerce")
        foreign["foreign_limit_exhaustion_rate"] = pd.to_numeric(
            foreign["foreign_limit_exhaustion_rate"], errors="coerce"
        )
        frames.append(
            foreign[["ticker", "obs_date", "foreign_holding_ratio", "foreign_limit_exhaustion_rate"]]
        )

    if not foreign_holding_auto.empty:
        foreign_auto = foreign_holding_auto.copy()
        foreign_auto["ticker"] = foreign_auto["ticker"].astype(str).str.zfill(6)
        foreign_auto["obs_date"] = pd.to_datetime(foreign_auto["date"], errors="coerce")
        foreign_auto["foreign_holding_ratio"] = pd.to_numeric(foreign_auto["foreign_holding_ratio"], errors="coerce")
        foreign_auto["foreign_limit_exhaustion_rate"] = pd.to_numeric(
            foreign_auto.get("foreign_limit_exhaustion_rate"), errors="coerce"
        )
        frames.append(
            foreign_auto[["ticker", "obs_date", "foreign_holding_ratio", "foreign_limit_exhaustion_rate"]]
        )

    if not frames:
        return pd.DataFrame(columns=["ticker", "avg_foreign_ratio", "last_foreign_ratio", "avg_exhaustion_rate", "foreign_change"])
    foreign = pd.concat(frames, ignore_index=True)
    foreign = foreign.dropna(subset=["obs_date"]).sort_values(["ticker", "obs_date"])

    period_lookup = period_frame.set_index("period")
    current_period = period_lookup.loc[target_period]
    current_start = pd.to_datetime(current_period["period_start"], errors="coerce")
    current_end = pd.to_datetime(current_period["period_end"], errors="coerce")
    current = foreign[(foreign["obs_date"] >= current_start) & (foreign["obs_date"] <= current_end)].copy()

    current_agg = pd.DataFrame(columns=["ticker", "avg_foreign_ratio", "last_foreign_ratio", "avg_exhaustion_rate"])
    if not current.empty:
        current_sorted = current.sort_values(["ticker", "obs_date"])
        current_agg = (
            current_sorted.groupby("ticker")
            .agg(
                avg_foreign_ratio=("foreign_holding_ratio", "mean"),
                last_foreign_ratio=("foreign_holding_ratio", "last"),
                avg_exhaustion_rate=("foreign_limit_exhaustion_rate", "mean"),
            )
            .reset_index()
        )

    prev_agg = pd.DataFrame(columns=["ticker", "prev_avg_foreign_ratio"])
    if prev_period and prev_period in period_lookup.index:
        prev_period_row = period_lookup.loc[prev_period]
        prev_start = pd.to_datetime(prev_period_row["period_start"], errors="coerce")
        prev_end = pd.to_datetime(prev_period_row["period_end"], errors="coerce")
        prev = foreign[(foreign["obs_date"] >= prev_start) & (foreign["obs_date"] <= prev_end)].copy()
        if not prev.empty:
            prev_agg = (
                prev.groupby("ticker")
                .agg(prev_avg_foreign_ratio=("foreign_holding_ratio", "mean"))
                .reset_index()
            )

    merged = current_agg.merge(prev_agg, on="ticker", how="left")
    merged["foreign_change"] = merged["avg_foreign_ratio"] - merged["prev_avg_foreign_ratio"].fillna(0.0)
    return merged.drop(columns=["prev_avg_foreign_ratio"], errors="ignore")


def build_weekly_features(as_of: pd.Timestamp | None = None) -> dict[str, object]:
    config = load_config()
    project_root = config.project_root
    sql_path = config.sql_dump_path
    auto_dir = project_root / "data" / "incoming" / "auto"

    price_df = _load_auto_frame(auto_dir / "yahoo_price_daily.csv")
    meta_df = _load_auto_frame(auto_dir / "naver_stock_meta_weekly.csv")
    dart_df = _load_auto_frame(auto_dir / "dart_major_holder_weekly.csv")
    foreign_auto_df = _load_auto_frame(auto_dir / "naver_foreign_holding_weekly.csv")

    if price_df.empty or meta_df.empty:
        raise ValueError("weekly raw data is missing; run weekly collection first")

    model_package = _load_model_package(config.model_pkl_path)

    stock_meta = load_table(sql_path, "stock_meta")
    sector_map = load_table(sql_path, "sector_map")
    period_df = load_table(sql_path, "period")
    feature_krx_hist = load_table(sql_path, "feature_krx")
    major_holder_hist = load_table(sql_path, "major_holder")
    foreign_holding = load_table(sql_path, "foreign_holding")
    labels = load_table(sql_path, "labels")

    target_period = get_active_period(period_df, as_of=as_of)
    prev_period = _get_previous_period(period_df, target_period)

    price_df["ticker"] = price_df["ticker"].astype(str).str.zfill(6)
    meta_df["ticker"] = meta_df["ticker"].astype(str).str.zfill(6)
    if not dart_df.empty:
        dart_df["ticker"] = dart_df["ticker"].astype(str).str.zfill(6)
    stock_meta["ticker"] = stock_meta["ticker"].astype(str).str.zfill(6)
    if not major_holder_hist.empty:
        major_holder_hist["ticker"] = major_holder_hist["ticker"].astype(str).str.zfill(6)
        major_holder_hist["period"] = major_holder_hist["period"].astype(str)

    merged = price_df.merge(meta_df, on="ticker", how="left", suffixes=("_price", "_meta"))
    merged = merged.merge(
        stock_meta[["ticker", "list_date", "is_not_common", "is_reits", "ksic_sector"]],
        on="ticker",
        how="left",
    )
    if not sector_map.empty:
        merged = merged.merge(
            sector_map[["ksic_sector", "gics_sector_2023", "gics_sector_pre2023", "krx_group"]],
            on="ksic_sector",
            how="left",
        )

    merged["market"] = merged["market"].fillna("")
    merged = merged[merged["market"].eq("코스피")].copy()
    merged = merged[merged["close"].notna() & merged["shares_outstanding"].notna()].copy()
    merged = merged[merged["is_not_common"].fillna(0).eq(0)]
    merged = merged[merged["is_reits"].fillna(0).eq(0)]

    fallback_period = prev_period
    if fallback_period is None and not major_holder_hist.empty:
        fallback_period = sorted(major_holder_hist["period"].dropna().unique().tolist(), key=_period_sort_key)[-1]

    if fallback_period and not major_holder_hist.empty:
        sql_float = (
            major_holder_hist.loc[major_holder_hist["period"] == fallback_period, ["ticker", "float_rate"]]
            .rename(columns={"float_rate": "sql_float_rate"})
            .copy()
        )
        sql_float["sql_float_rate"] = pd.to_numeric(sql_float["sql_float_rate"], errors="coerce")
        merged = merged.merge(sql_float, on="ticker", how="left")
    else:
        merged["sql_float_rate"] = pd.NA

    merged["float_rate"] = pd.to_numeric(merged["float_rate"], errors="coerce") / 100.0
    low_float_mask = merged["float_rate"].isna() | (merged["float_rate"] <= 0.0)
    suspicious_float_mask = (
        merged["float_rate"].notna()
        & merged["sql_float_rate"].notna()
        & (merged["float_rate"] < 0.10)
        & (merged["sql_float_rate"] >= 0.10)
    )
    merged.loc[low_float_mask, "float_rate"] = pd.to_numeric(merged.loc[low_float_mask, "sql_float_rate"], errors="coerce")
    merged.loc[suspicious_float_mask, "float_rate"] = pd.to_numeric(
        merged.loc[suspicious_float_mask, "sql_float_rate"], errors="coerce"
    )
    merged["avg_mktcap"] = pd.to_numeric(merged["close"], errors="coerce") * pd.to_numeric(
        merged["shares_outstanding"], errors="coerce"
    )
    merged["turnover_ratio"] = pd.to_numeric(merged["volume"], errors="coerce") / pd.to_numeric(
        merged["shares_outstanding"], errors="coerce"
    )
    merged["period_rank"] = merged["avg_mktcap"].rank(method="first", ascending=False).astype(int)
    merged["gics_sector"] = (
        merged["gics_sector_2023"]
        .fillna(merged["gics_sector_pre2023"])
        .fillna(merged["sector"])
        .fillna("기타")
    )
    merged["krx_group"] = merged["krx_group"].fillna(merged["industry"]).fillna("기타")
    merged["period"] = target_period
    merged["float_mktcap"] = merged["avg_mktcap"] * merged["float_rate"].fillna(0.0)

    if prev_period:
        prev_feature = feature_krx_hist[feature_krx_hist["period"].astype(str) == prev_period].copy()
        prev_feature["ticker"] = prev_feature["ticker"].astype(str).str.zfill(6)
        prev_feature = prev_feature[["ticker", "period_rank"]].rename(columns={"period_rank": "prev_rank"})
        merged = merged.merge(prev_feature, on="ticker", how="left")
    else:
        merged["prev_rank"] = 0
    merged["prev_rank"] = pd.to_numeric(merged["prev_rank"], errors="coerce").fillna(0)

    foreign_agg = _build_foreign_aggregate(foreign_holding, foreign_auto_df, period_df, target_period, prev_period)
    if not foreign_agg.empty:
        merged = merged.merge(foreign_agg, on="ticker", how="left")
    else:
        merged["avg_foreign_ratio"] = 0.0
        merged["last_foreign_ratio"] = 0.0
        merged["avg_exhaustion_rate"] = 0.0
        merged["foreign_change"] = 0.0

    merged["avg_foreign_ratio"] = pd.to_numeric(merged.get("avg_foreign_ratio"), errors="coerce").fillna(0.0)
    merged["last_foreign_ratio"] = pd.to_numeric(merged.get("last_foreign_ratio"), errors="coerce").fillna(0.0)
    merged["avg_exhaustion_rate"] = pd.to_numeric(merged.get("avg_exhaustion_rate"), errors="coerce").fillna(0.0)
    merged["foreign_change"] = pd.to_numeric(merged.get("foreign_change"), errors="coerce").fillna(0.0)

    naver_major = pd.to_numeric(merged.get("major_holder_ratio"), errors="coerce")
    naver_treasury = pd.to_numeric(merged.get("treasury_ratio"), errors="coerce")
    dart_major = pd.Series(pd.NA, index=merged.index, dtype="object")
    dart_treasury = pd.Series(pd.NA, index=merged.index, dtype="object")
    if not dart_df.empty:
        dart_merge = dart_df[["ticker", "major_holder_ratio", "treasury_ratio"]].copy()
        dart_merge = dart_merge.rename(
            columns={
                "major_holder_ratio": "dart_major_holder_ratio",
                "treasury_ratio": "dart_treasury_ratio",
            }
        )
        merged = merged.merge(dart_merge, on="ticker", how="left")
        dart_major = pd.to_numeric(merged.get("dart_major_holder_ratio"), errors="coerce")
        dart_treasury = pd.to_numeric(merged.get("dart_treasury_ratio"), errors="coerce")

    merged["major_holder_ratio"] = naver_major.combine_first(dart_major).fillna(0.0) / 100.0
    merged["treasury_ratio"] = naver_treasury.combine_first(dart_treasury).fillna(0.0) / 100.0

    merged["sector_rank"] = merged.groupby("gics_sector")["period_rank"].rank(method="first").astype(int)
    sector_count = merged.groupby("gics_sector")["ticker"].transform("count")
    merged["sector_relative_rank"] = merged["sector_rank"] / sector_count

    le_gics = model_package["le_gics"]
    known_gics = set(le_gics.classes_)
    default_sector = "기타"
    merged["gics_sector_clean"] = merged["gics_sector"].fillna(default_sector).astype(str).apply(
        lambda value: value if value in known_gics else default_sector
    )
    merged["gics_sector_enc"] = le_gics.transform(merged["gics_sector_clean"])
    merged["sector_member_score"] = merged["gics_sector_enc"].map(model_package.get("sector_in_map", {})).fillna(0.5)

    prev_members: set[str] = set()
    if prev_period:
        prev_labels = labels[labels["period"].astype(str) == prev_period].copy()
        if not prev_labels.empty:
            prev_labels["ticker"] = prev_labels["ticker"].astype(str).str.zfill(6)
            if "is_member" in prev_labels.columns:
                prev_members = set(
                    prev_labels.loc[pd.to_numeric(prev_labels["is_member"], errors="coerce").fillna(0).eq(1), "ticker"]
                    .astype(str)
                    .tolist()
                )
            elif "was_member" in prev_labels.columns:
                prev_members = set(
                    prev_labels.loc[pd.to_numeric(prev_labels["was_member"], errors="coerce").fillna(0).eq(1), "ticker"]
                    .astype(str)
                    .tolist()
                )

    if not prev_members and prev_period:
        prev_members = set(model_package.get("actual_members", {}).get(prev_period, []))

    merged["prev_was_member"] = merged["ticker"].astype(str).apply(lambda ticker: int(str(ticker).zfill(6) in prev_members))

    feature_frame = merged[
        ["period", "ticker", "avg_mktcap", "float_rate", "gics_sector", "krx_group", "period_rank", "turnover_ratio"]
    ].rename(columns={"float_rate": "float_ratio"})

    major_holder_frame = merged[["ticker", "shares_outstanding", "major_holder_ratio", "treasury_ratio", "float_rate"]].copy()
    major_holder_frame["major_holder_shares"] = (
        pd.to_numeric(major_holder_frame["shares_outstanding"], errors="coerce")
        * major_holder_frame["major_holder_ratio"].fillna(0.0)
    )
    major_holder_frame["treasury_shares"] = (
        pd.to_numeric(major_holder_frame["shares_outstanding"], errors="coerce")
        * major_holder_frame["treasury_ratio"].fillna(0.0)
    )
    major_holder_frame["non_float_ratio"] = major_holder_frame["major_holder_ratio"].fillna(0.0) + major_holder_frame[
        "treasury_ratio"
    ].fillna(0.0)
    major_holder_frame["period"] = target_period
    major_holder_frame = major_holder_frame[
        [
            "period",
            "ticker",
            "major_holder_shares",
            "major_holder_ratio",
            "treasury_shares",
            "treasury_ratio",
            "non_float_ratio",
            "float_rate",
        ]
    ]

    model_input = merged[
        [
            "period",
            "ticker",
            "avg_mktcap",
            "float_rate",
            "gics_sector",
            "krx_group",
            "period_rank",
            "turnover_ratio",
            "float_mktcap",
            "prev_rank",
            "major_holder_ratio",
            "treasury_ratio",
            "sector_rank",
            "sector_relative_rank",
            "sector_member_score",
            "foreign_change",
            "avg_foreign_ratio",
            "last_foreign_ratio",
            "avg_exhaustion_rate",
            "prev_was_member",
            "company",
        ]
    ].copy()

    auto_dir.mkdir(parents=True, exist_ok=True)
    feature_path = auto_dir / f"feature_krx_{target_period}.csv"
    major_holder_path = auto_dir / f"major_holder_{target_period}.csv"
    model_input_path = auto_dir / f"model_input_{target_period}.csv"
    feature_frame.to_csv(feature_path, index=False, encoding="utf-8-sig")
    major_holder_frame.to_csv(major_holder_path, index=False, encoding="utf-8-sig")
    model_input.to_csv(model_input_path, index=False, encoding="utf-8-sig")

    required_features = [str(col) for col in model_package.get("features", [])]
    available_features = [col for col in required_features if col in model_input.columns]
    missing_features = [col for col in required_features if col not in model_input.columns]

    return {
        "period": target_period,
        "prev_period": prev_period,
        "feature_rows": int(len(feature_frame)),
        "major_holder_rows": int(len(major_holder_frame)),
        "model_input_rows": int(len(model_input)),
        "feature_path": str(feature_path),
        "major_holder_path": str(major_holder_path),
        "model_input_path": str(model_input_path),
        "available_model_features": available_features,
        "missing_model_features": missing_features,
    }
