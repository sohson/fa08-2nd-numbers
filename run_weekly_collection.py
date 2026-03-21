from __future__ import annotations

import argparse
import json

from src.pipeline.weekly_pipeline import run_weekly_collection


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="테스트용 수집 종목 수 제한")
    args = parser.parse_args()

    summary = run_weekly_collection(limit=args.limit)
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
