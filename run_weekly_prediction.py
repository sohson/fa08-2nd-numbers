from __future__ import annotations

import argparse
import json

from src.pipeline.weekly_predictor import run_weekly_prediction


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of", type=str, default=None, help="Override date in YYYY-MM-DD format")
    args = parser.parse_args()

    summary = run_weekly_prediction(as_of=args.as_of)
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
