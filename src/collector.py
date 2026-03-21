from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.config import AppConfig


class MissingApiKeyError(RuntimeError):
    pass


@dataclass(frozen=True)
class CollectionTargets:
    period: str
    output_dir: Path


def validate_runtime_keys(config: AppConfig) -> dict[str, bool]:
    return {
        "krxdata": bool(config.krxdata_api_key),
        "dart": bool(config.open_dart_api_key),
        "ecos": bool(config.ecos_api_key),
    }


def ensure_collection_ready(config: AppConfig) -> None:
    status = validate_runtime_keys(config)
    missing = [name for name, enabled in status.items() if not enabled]
    if missing:
        joined = ", ".join(missing)
        raise MissingApiKeyError(
            f"실데이터 자동 수집을 시작하려면 다음 API 키가 필요합니다: {joined}. "
            "현재 프로젝트는 대시보드 실행은 가능하지만, 미래 반기용 원천 데이터 자동 갱신은 아직 시작할 수 없습니다."
        )


def collect_market_data(config: AppConfig, targets: CollectionTargets) -> None:
    if not config.krxdata_api_key:
        raise MissingApiKeyError("KRXDATA_API_KEY가 없어 시장 데이터를 자동 수집할 수 없습니다.")
    raise NotImplementedError(
        "KRXDATA 엔드포인트 사양이 확정되면 이 함수에 반기별 시가총액/거래대금/상장 정보를 연결합니다."
    )


def collect_holder_data(config: AppConfig, targets: CollectionTargets) -> None:
    if not config.open_dart_api_key:
        raise MissingApiKeyError("OPEN_DART_API_KEY가 없어 대주주/자사주 데이터를 자동 수집할 수 없습니다.")
    raise NotImplementedError(
        "DART 공시 스펙이 확정되면 이 함수에 유동비율 계산용 원천 수집 로직을 연결합니다."
    )


def collect_macro_data(config: AppConfig, targets: CollectionTargets) -> None:
    if not config.ecos_api_key:
        raise MissingApiKeyError("ECOS_API_KEY가 없어 매크로 데이터를 자동 수집할 수 없습니다.")
    raise NotImplementedError(
        "ECOS 시리즈 코드가 확정되면 이 함수에 macro.csv 업데이트 로직을 연결합니다."
    )
