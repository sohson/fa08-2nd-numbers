from __future__ import annotations

from pathlib import Path
import json

import pandas as pd

from src.config import load_config
from src.pipeline.feature_builder import build_weekly_features
from src.predictor import load_model_package, run_prediction
from src.sql_dump import load_table


def run_weekly_prediction(as_of: pd.Timestamp | None = None) -> dict[str, object]:
    config = load_config()
    project_root = config.project_root
    auto_dir = project_root / "data" / "incoming" / "auto"

    build_summary = build_weekly_features(as_of=as_of)
    period = str(build_summary["period"])
    model_input_path = Path(str(build_summary["model_input_path"]))

    model_input = pd.read_csv(model_input_path)
    if model_input.empty:
        raise ValueError(f"model_input is empty for {period}")

    package = load_model_package(config.model_pkl_path)
    period_frame = load_table(config.sql_dump_path, "period")
    period_row = period_frame[period_frame["period"].astype(str) == period]
    period_end_date = None
    if not period_row.empty:
        period_end_date = pd.to_datetime(period_row.iloc[0]["period_end"], errors="coerce")

    prediction = run_prediction(
        model_input,
        package,
        period=period,
        period_end_date=period_end_date,
    )

    scored = prediction["scored"].copy()
    strong_in = prediction["strong_in"].copy()
    strong_out = prediction["strong_out"].copy()

    scored_path = auto_dir / f"weekly_predictions_{period}.csv"
    strong_in_path = auto_dir / f"weekly_strong_in_{period}.csv"
    strong_out_path = auto_dir / f"weekly_strong_out_{period}.csv"

    scored.to_csv(scored_path, index=False, encoding="utf-8-sig")
    strong_in.to_csv(strong_in_path, index=False, encoding="utf-8-sig")
    strong_out.to_csv(strong_out_path, index=False, encoding="utf-8-sig")

    result = {
        "period": period,
        "model_input_rows": int(len(model_input)),
        "scored_rows": int(len(scored)),
        "strong_in_rows": int(len(strong_in)),
        "strong_out_rows": int(len(strong_out)),
        "scored_path": str(scored_path),
        "strong_in_path": str(strong_in_path),
        "strong_out_path": str(strong_out_path),
        "summary": prediction["summary"],
    }
    summary_path = auto_dir / "weekly_prediction_summary.json"
    summary_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    result["summary_path"] = str(summary_path)
    return result
