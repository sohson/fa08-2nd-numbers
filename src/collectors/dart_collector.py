from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from io import BytesIO
import xml.etree.ElementTree as ET
import zipfile
from typing import Iterable

import requests

from src.collectors.models import CollectionResult, DartMajorHolder


class DartCollector:
    corp_code_url = "https://opendart.fss.or.kr/api/corpCode.xml"
    major_stock_url = "https://opendart.fss.or.kr/api/majorstock.json"

    def __init__(self, api_key: str | None) -> None:
        self.api_key = api_key
        self._corp_code_map: dict[str, str] | None = None

    def _ensure_api_key(self) -> None:
        if not self.api_key:
            raise ValueError("OPEN_DART_API_KEY is not configured")

    def fetch_corp_code_map(self) -> dict[str, str]:
        self._ensure_api_key()
        if self._corp_code_map is not None:
            return self._corp_code_map

        response = requests.get(
            self.corp_code_url,
            params={"crtfc_key": self.api_key},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=20,
        )
        response.raise_for_status()
        with zipfile.ZipFile(BytesIO(response.content)) as zf:
            xml_name = zf.namelist()[0]
            xml_bytes = zf.read(xml_name)

        root = ET.fromstring(xml_bytes)
        corp_map: dict[str, str] = {}
        for item in root.findall("list"):
            stock_code = (item.findtext("stock_code") or "").strip()
            corp_code = (item.findtext("corp_code") or "").strip()
            if stock_code and corp_code:
                corp_map[stock_code.zfill(6)] = corp_code

        self._corp_code_map = corp_map
        return corp_map

    @staticmethod
    def _safe_float(value: object) -> float | None:
        if value in (None, "", "-"):
            return None
        try:
            return float(str(value).replace(",", ""))
        except Exception:
            return None

    def fetch_major_holder(self, ticker: str) -> DartMajorHolder:
        self._ensure_api_key()
        corp_code = self.fetch_corp_code_map().get(str(ticker).zfill(6))
        if not corp_code:
            return DartMajorHolder(ticker=str(ticker).zfill(6))

        response = requests.get(
            self.major_stock_url,
            params={"crtfc_key": self.api_key, "corp_code": corp_code},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        rows = payload.get("list") or []
        if not rows:
            return DartMajorHolder(ticker=str(ticker).zfill(6), corp_code=corp_code)

        latest = rows[0]
        holder_name = latest.get("nm") or latest.get("report_resn") or latest.get("rcept_no")
        ratio = None
        for key in ("stkrt", "trmend_posesn_stock_qota_rt", "posesn_stock_qota_rt"):
            ratio = self._safe_float(latest.get(key))
            if ratio is not None:
                break

        report_date = latest.get("rcept_dt")
        if report_date and len(report_date) == 8:
            report_date = f"{report_date[:4]}-{report_date[4:6]}-{report_date[6:8]}"

        return DartMajorHolder(
            ticker=str(ticker).zfill(6),
            corp_code=corp_code,
            report_date=report_date,
            holder_name=holder_name,
            major_holder_ratio=ratio,
            treasury_ratio=None,
        )

    def collect(self, tickers: Iterable[str]) -> CollectionResult:
        rows: list[dict[str, object]] = []
        errors: list[dict[str, str]] = []
        tickers = [str(t).zfill(6) for t in tickers]

        for ticker in tickers:
            try:
                snapshot = self.fetch_major_holder(ticker)
                rows.append(asdict(snapshot))
            except Exception as exc:
                errors.append({"ticker": ticker, "message": str(exc)})

        return CollectionResult(
            source="dart",
            run_at=datetime.now(),
            requested=len(tickers),
            succeeded=len(rows),
            failed=len(errors),
            rows=rows,
            errors=errors,
        )
