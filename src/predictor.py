from __future__ import annotations

import datetime as dt
import pickle
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def load_model_package(model_path: Path) -> dict[str, Any]:
    with model_path.open("rb") as handle:
        return pickle.load(handle)


def prepare_features(
    raw_frame: pd.DataFrame,
    package: dict[str, Any],
    period: str,
    prev_members: set[str] | None = None,
) -> pd.DataFrame:
    frame = raw_frame.copy()

    if prev_members is not None:
        normalized_prev_members = {str(ticker).zfill(6) for ticker in prev_members}
        frame["prev_was_member"] = frame["ticker"].astype(str).str.zfill(6).apply(
            lambda ticker: int(ticker in normalized_prev_members)
        )
    elif "prev_was_member" in frame.columns:
        frame["prev_was_member"] = frame["prev_was_member"].fillna(0).astype(int)
    elif "prev_member" in frame.columns:
        frame["prev_was_member"] = frame["prev_member"].fillna(0).astype(int)
    elif "was_member" in frame.columns:
        frame["prev_was_member"] = frame["was_member"].fillna(0).astype(int)
    else:
        if prev_members is None:
            period_order = package.get("period_order", [])
            if period in period_order:
                index = period_order.index(period)
                prev_period = period_order[index - 1] if index > 0 else None
                prev_members = set(package.get("actual_members", {}).get(prev_period, []))
            else:
                prev_members = set()
        frame["prev_was_member"] = frame["ticker"].apply(lambda ticker: int(ticker in prev_members))

    le_gics = package["le_gics"]
    le_krx = package["le_krx"]
    sector_dict_gics = package["sector_dict_gics"]
    sector_dict_krx = package["sector_dict_krx"]

    if "gics_sector" not in frame.columns and "ksic_sector" in frame.columns:
        frame["gics_sector"] = frame["ksic_sector"].map(sector_dict_gics)
        frame["krx_group"] = frame["ksic_sector"].map(sector_dict_krx)

    default_sector = "기타"
    gics_classes = set(le_gics.classes_)
    krx_classes = set(le_krx.classes_)
    frame["gics_sector_clean"] = (
        frame["gics_sector"].fillna(default_sector).astype(str).apply(
            lambda value: value if value in gics_classes else default_sector
        )
    )
    frame["krx_group_clean"] = (
        frame["krx_group"].fillna(default_sector).astype(str).apply(
            lambda value: value if value in krx_classes else default_sector
        )
    )
    frame["gics_sector_enc"] = le_gics.transform(frame["gics_sector_clean"])
    frame["krx_group_enc"] = le_krx.transform(frame["krx_group_clean"])

    for column in ["prev_rank", "rank_change", "mktcap_change", "foreign_change", "turnover_change"]:
        if column not in frame.columns:
            frame[column] = 0

    if "float_mktcap" not in frame.columns and "avg_mktcap" in frame.columns:
        frame["float_mktcap"] = frame["avg_mktcap"] * frame["float_rate"].fillna(0)

    if "float_mktcap_rank" not in frame.columns and "float_mktcap" in frame.columns:
        frame["float_mktcap_rank"] = frame["float_mktcap"].rank(ascending=False, method="first").astype(int)

    if "dist_from_200" not in frame.columns and "period_rank" in frame.columns:
        frame["dist_from_200"] = frame["period_rank"] - 200

    if "float_dist_from_200" not in frame.columns and "float_mktcap_rank" in frame.columns:
        frame["float_dist_from_200"] = frame["float_mktcap_rank"] - 200

    if "consecutive_member" not in frame.columns:
        frame["consecutive_member"] = 0

    if "sector_rank" not in frame.columns and "gics_sector_enc" in frame.columns:
        frame["sector_rank"] = frame.groupby("gics_sector_enc")["period_rank"].rank(method="first").astype(int)
        sector_count = frame.groupby("gics_sector_enc")["ticker"].transform("count")
        frame["sector_relative_rank"] = frame["sector_rank"] / sector_count

    if "rank_acceleration" not in frame.columns:
        frame["rank_acceleration"] = 0

    if "sector_member_score" not in frame.columns:
        sector_map = package.get("sector_in_map", {})
        frame["sector_member_score"] = frame["gics_sector_enc"].map(sector_map).fillna(0.5)

    if "foreign_acceleration" not in frame.columns:
        frame["foreign_acceleration"] = 0

    return frame


def apply_filters(frame: pd.DataFrame, period_end_date: pd.Timestamp | None = None) -> pd.DataFrame:
    if period_end_date is None:
        period_end_date = pd.Timestamp.now()

    excluded_not_common = frame.get("is_not_common", pd.Series(0, index=frame.index)).fillna(0) == 1
    excluded_low_float = (frame["float_rate"] < 0.10) & frame["float_rate"].notna()
    excluded_reits = frame.get("is_reits", pd.Series(0, index=frame.index)).fillna(0) == 1

    excluded_recent_listing = pd.Series(False, index=frame.index)
    if "list_date" in frame.columns:
        listing_date = pd.to_datetime(frame["list_date"], errors="coerce")
        months = (period_end_date.year - listing_date.dt.year) * 12 + (
            period_end_date.month - listing_date.dt.month
        )
        excluded_recent_listing = (months < 6) & months.notna()

    excluded = excluded_not_common | excluded_low_float | excluded_reits | excluded_recent_listing
    return frame.loc[~excluded].copy()


def refresh_post_filter_features(frame: pd.DataFrame, package: dict[str, Any]) -> pd.DataFrame:
    refreshed = frame.copy()

    if "float_mktcap" not in refreshed.columns and "avg_mktcap" in refreshed.columns:
        refreshed["float_mktcap"] = refreshed["avg_mktcap"] * refreshed["float_rate"].fillna(0)
    else:
        refreshed["float_mktcap"] = refreshed["avg_mktcap"] * refreshed["float_rate"].fillna(0)

    if "float_mktcap" in refreshed.columns:
        refreshed["float_mktcap_rank"] = refreshed["float_mktcap"].rank(ascending=False, method="first").astype(int)

    if "period_rank" in refreshed.columns:
        refreshed["dist_from_200"] = refreshed["period_rank"] - 200

    if "float_mktcap_rank" in refreshed.columns:
        refreshed["float_dist_from_200"] = refreshed["float_mktcap_rank"] - 200

    if "gics_sector_enc" in refreshed.columns and "period_rank" in refreshed.columns:
        refreshed["sector_rank"] = (
            refreshed.groupby("gics_sector_enc")["period_rank"].rank(method="first").astype(int)
        )
        sector_count = refreshed.groupby("gics_sector_enc")["ticker"].transform("count")
        refreshed["sector_relative_rank"] = refreshed["sector_rank"] / sector_count

    if "sector_member_score" not in refreshed.columns:
        sector_map = package.get("sector_in_map", {})
        refreshed["sector_member_score"] = refreshed["gics_sector_enc"].map(sector_map).fillna(0.5)

    return refreshed


def run_prediction(
    period_frame: pd.DataFrame,
    package: dict[str, Any],
    period: str,
    period_end_date: pd.Timestamp | None = None,
) -> dict[str, Any]:
    model = package["model"]
    features = package["features"]
    ticker_to_name = package.get("ticker_to_name", {})
    period_order = package.get("period_order", [])
    if period in period_order:
        index = period_order.index(period)
        prev_period = period_order[index - 1] if index > 0 else None
        prev_members = set(package.get("actual_members", {}).get(prev_period, [])) if prev_period else None
    else:
        prev_members = None

    prepared = prepare_features(period_frame, package, period, prev_members=prev_members)
    prepared = apply_filters(prepared, period_end_date=period_end_date)
    prepared = refresh_post_filter_features(prepared, package)

    missing_features = [feature for feature in features if feature not in prepared.columns]
    for feature in missing_features:
        prepared[feature] = 0

    matrix = prepared[features].fillna(0)
    scores = model.predict_proba(matrix)[:, 1]
    prepared["score"] = scores
    prepared["score_pct"] = prepared["score"] * 100
    prepared["pred_rank"] = prepared["score"].rank(ascending=False, method="first").astype(int)
    prepared["company"] = prepared["ticker"].map(ticker_to_name).fillna(prepared.get("company", ""))

    prepared = prepared.sort_values(["pred_rank", "period_rank"], ascending=[True, True]).reset_index(drop=True)
    top200 = set(prepared.nlargest(200, "score")["ticker"].astype(str).tolist())
    prev_member_series = prepared["prev_was_member"].fillna(0).astype(int)
    candidate_201_220 = prepared["pred_rank"].between(201, 220)

    prepared["pred_top200"] = prepared["ticker"].astype(str).apply(lambda ticker: int(ticker in top200))
    prepared["strong_in"] = (
        (prev_member_series == 0) & prepared["ticker"].astype(str).isin(top200)
    ).astype(int)
    prepared["strong_out"] = (
        (prev_member_series == 1) & candidate_201_220
    ).astype(int)

    strong_in = prepared.loc[prepared["strong_in"] == 1].sort_values("pred_rank", ascending=True).head(20).copy()
    strong_out = prepared.loc[prepared["strong_out"] == 1].sort_values("pred_rank", ascending=True).head(20).copy()

    prepared["strong_in"] = prepared["ticker"].astype(str).isin(set(strong_in["ticker"].astype(str))).astype(int)
    prepared["strong_out"] = prepared["ticker"].astype(str).isin(set(strong_out["ticker"].astype(str))).astype(int)

    return {
        "period": period,
        "scored": prepared,
        "top200": top200,
        "pred_in": set(strong_in["ticker"].astype(str)),
        "pred_out": set(strong_out["ticker"].astype(str)),
        "missing_members": set(),
        "strong_in": strong_in,
        "strong_out": strong_out,
        "summary": {
            "run_date": dt.date.today().isoformat(),
            "model_label": f"{package.get('method', 'model')} - {package.get('model_name', 'unknown')}",
            "model_version": package.get("model_version", "unknown"),
            "feature_count": len(features),
            "candidate_count": len(prepared),
        },
    }


def compare_with_actual(
    prediction_result: dict[str, Any],
    actual_members_current: set[str],
    prev_members: set[str],
) -> dict[str, float]:
    predicted_top200 = prediction_result["top200"]
    actual_in = actual_members_current - prev_members
    actual_out = prev_members - actual_members_current
    predicted_in = predicted_top200 - prev_members
    predicted_out = prev_members - predicted_top200
    in_hit = predicted_in & actual_in
    out_hit = predicted_out & actual_out

    return {
        "top200_accuracy": len(predicted_top200 & actual_members_current) / 200 if actual_members_current else 0.0,
        "in_precision": len(in_hit) / len(predicted_in) if predicted_in else 0.0,
        "in_recall": len(in_hit) / len(actual_in) if actual_in else 0.0,
        "out_precision": len(out_hit) / len(predicted_out) if predicted_out else 0.0,
        "out_recall": len(out_hit) / len(actual_out) if actual_out else 0.0,
    }


def build_feature_story(row: pd.Series) -> list[dict[str, str]]:
    story: list[dict[str, str]] = []

    def add(label: str, value: Any, tone: str) -> None:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return
        story.append({"label": label, "value": str(value), "tone": tone})

    if pd.notna(row.get("period_rank")):
        rank = int(row["period_rank"])
        add("시가총액 순위", f"{rank}위", "positive" if rank <= 200 else "negative")
    if pd.notna(row.get("float_rate")):
        rate = float(row["float_rate"]) * 100
        add("유동비율", f"{rate:.1f}%", "positive" if rate >= 10 else "negative")
    if pd.notna(row.get("avg_foreign_ratio")):
        add("외국인 평균 보유율", f"{float(row['avg_foreign_ratio']):.2f}%", "positive")
    if pd.notna(row.get("turnover_ratio")):
        add("거래대금 회전율", f"{float(row['turnover_ratio']):.3f}", "positive")
    if pd.notna(row.get("sector_relative_rank")):
        add("섹터 내 상대 순위", f"{float(row['sector_relative_rank']):.2f}", "positive")
    if pd.notna(row.get("dist_from_200")):
        distance = int(row["dist_from_200"])
        add("200위 기준 거리", f"{distance:+d}", "positive" if distance <= 0 else "negative")

    return story[:6]
