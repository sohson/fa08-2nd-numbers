from __future__ import annotations

import argparse
from pathlib import Path

from src.collector import (
    CollectionTargets,
    MissingApiKeyError,
    collect_holder_data,
    collect_macro_data,
    collect_market_data,
    ensure_collection_ready,
)
from src.config import load_config


def build_targets(period: str) -> CollectionTargets:
    project_root = Path(__file__).resolve().parent
    output_dir = project_root / "data" / "incoming" / period
    output_dir.mkdir(parents=True, exist_ok=True)
    return CollectionTargets(period=period, output_dir=output_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description="Future-period data collection pipeline")
    parser.add_argument("--period", required=True, help="예: 2026_H2")
    args = parser.parse_args()

    config = load_config()
    targets = build_targets(args.period)

    try:
        ensure_collection_ready(config)
        collect_market_data(config, targets)
        collect_holder_data(config, targets)
        collect_macro_data(config, targets)
    except MissingApiKeyError as error:
        raise SystemExit(str(error)) from error
    except NotImplementedError as error:
        raise SystemExit(
            f"{error}\n"
            f"현재는 키와 파이프라인 골격만 구성했습니다. "
            f"실제 자동 수집 구현을 끝내려면 API 문서 또는 응답 샘플이 추가로 필요합니다."
        ) from error


if __name__ == "__main__":
    main()
