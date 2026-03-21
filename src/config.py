from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


ENV_CANDIDATES = [
    Path(__file__).resolve().parent.parent / ".env",
    Path("C:/Users/Admin/numbers/.env"),
]

for env_path in ENV_CANDIDATES:
    if env_path.exists():
        load_dotenv(env_path, override=False)


@dataclass(frozen=True)
class AppConfig:
    project_root: Path
    sql_dump_path: Path
    macro_csv_path: Path
    model_pkl_path: Path
    historical_predictions_dir: Path
    auto_output_dir: Path
    krxdata_api_key: str | None
    naver_client_id: str | None
    naver_client_secret: str | None
    open_dart_api_key: str | None
    ecos_api_key: str | None


def _resolve_path(project_root: Path, raw_value: str, default_relative: str) -> Path:
    value = raw_value or default_relative
    path = Path(value)
    if not path.is_absolute():
        path = project_root / path
    return path


def load_config() -> AppConfig:
    project_root = Path(__file__).resolve().parent.parent
    sql_dump_path = _resolve_path(
        project_root,
        os.getenv("APP_SQL_DUMP", ""),
        "data/raw/kospi_db_full_20260320.sql",
    )
    macro_csv_path = _resolve_path(
        project_root,
        os.getenv("APP_MACRO_CSV", ""),
        "data/raw/macro.csv",
    )
    model_pkl_path = _resolve_path(
        project_root,
        os.getenv("APP_MODEL_PKL", ""),
        "data/raw/model_package.pkl",
    )
    historical_predictions_dir = _resolve_path(
        project_root,
        os.getenv("APP_HISTORICAL_PREDICTIONS_DIR", ""),
        "data/raw/historical_predictions",
    )
    auto_output_dir = _resolve_path(
        project_root,
        os.getenv("APP_AUTO_OUTPUT_DIR", ""),
        "data/incoming/auto",
    )
    return AppConfig(
        project_root=project_root,
        sql_dump_path=sql_dump_path,
        macro_csv_path=macro_csv_path,
        model_pkl_path=model_pkl_path,
        historical_predictions_dir=historical_predictions_dir,
        auto_output_dir=auto_output_dir,
        krxdata_api_key=os.getenv("KRXDATA_API_KEY") or None,
        naver_client_id=os.getenv("NAVER_CLIENT_ID") or None,
        naver_client_secret=os.getenv("NAVER_CLIENT_SECRET") or None,
        open_dart_api_key=os.getenv("OPEN_DART_API_KEY") or None,
        ecos_api_key=os.getenv("ECOS_API_KEY") or None,
    )
