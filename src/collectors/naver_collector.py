from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
import re
from typing import Iterable

import requests

from src.collectors.models import CollectionResult, NaverStockMeta


class NaverCollector:
    main_url = "https://finance.naver.com/item/main.naver?code={ticker}"
    coinfo_url = "https://finance.naver.com/item/coinfo.naver?code={ticker}"
    wisereport_base = "https://navercomp.wisereport.co.kr"

    title_suffix_pattern = r"Npay\s*증권"
    market_names_pattern = r"(코스피|코스닥|코넥스)"
    issued_shares_label = "발행주식수"
    float_ratio_label = "유동비율"
    foreign_ratio_label = "외국인지분율"
    price_label = "주가/전일대비/수익률"
    volume_label = "거래량/거래대금"
    national_pension_label = "국민연금공단"
    treasury_labels = ("자사주", "자기주식")

    def fetch_html(self, url: str) -> str:
        response = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://finance.naver.com/"},
            timeout=10,
        )
        response.raise_for_status()
        return response.text

    @staticmethod
    def _clean_text(value: object) -> str | None:
        if value is None:
            return None
        text = re.sub(r"\s+", " ", str(value)).strip()
        return text or None

    @staticmethod
    def _parse_number(value: object) -> float | None:
        text = NaverCollector._clean_text(value)
        if not text:
            return None
        match = re.search(r"([\d,]+(?:\.\d+)?)", text)
        if not match:
            return None
        return float(match.group(1).replace(",", ""))

    @staticmethod
    def _strip_tags(value: str) -> str:
        text = re.sub(r"<[^>]+>", " ", value)
        return re.sub(r"\s+", " ", text).strip()

    def parse_main_page(self, ticker: str, html: str) -> dict[str, object]:
        title_match = re.search(
            rf"<title>\s*([^:<]+?)\s*:\s*{self.title_suffix_pattern}\s*</title>",
            html,
            re.I,
        )
        market_match = re.search(
            rf"{re.escape(str(ticker).zfill(6))}\s+{self.market_names_pattern}",
            html,
        )
        return {
            "company": self._clean_text(title_match.group(1)) if title_match else None,
            "market": self._clean_text(market_match.group(1)) if market_match else None,
        }

    def extract_wisereport_url(self, html: str) -> str | None:
        match = re.search(r'<iframe[^>]+id="coinfo_cp"[^>]+src="([^"]+)"', html, re.I)
        if not match:
            return None
        src = match.group(1).strip()
        if src.startswith("http://") or src.startswith("https://"):
            return src
        return f"{self.wisereport_base}{src}"

    def parse_wisereport_page(self, html: str) -> dict[str, object]:
        market = None
        sector = None
        industry = None

        market_line = re.search(r'<dt class="line-left">(KOSPI|KOSDAQ|KONEX)\s*:\s*([^<]+)</dt>', html, re.I)
        if market_line:
            market_code = market_line.group(1).upper()
            market = {"KOSPI": "코스피", "KOSDAQ": "코스닥", "KONEX": "코넥스"}.get(market_code, market_code)
            sector_text = self._clean_text(market_line.group(2))
            if sector_text:
                sector = re.sub(r"^(코스피|코스닥|코넥스)\s+", "", sector_text, flags=re.I)

        industry_line = re.search(r'<dt class="line-left">WICS\s*:\s*([^<]+)</dt>', html, re.I)
        if industry_line:
            industry = self._clean_text(industry_line.group(1))

        def extract_row_value(label: str) -> str | None:
            pattern = rf"<th[^>]*>\s*{re.escape(label)}\s*</th>\s*<td[^>]*>\s*(.*?)\s*</td>"
            match = re.search(pattern, html, re.I | re.S)
            if not match:
                return None
            return self._strip_tags(match.group(1))

        shares = None
        float_rate = None
        foreign_ratio = None
        major_holder_ratio = None
        treasury_ratio = None
        current_price = None
        current_volume = None

        shares_value = extract_row_value(f"{self.issued_shares_label}/{self.float_ratio_label}")
        if shares_value:
            parts = [part.strip() for part in shares_value.split("/")]
            if parts:
                shares = self._parse_number(parts[0])
            if len(parts) > 1:
                float_rate = self._parse_number(parts[1])

        foreign_value = extract_row_value(self.foreign_ratio_label)
        if foreign_value:
            foreign_ratio = self._parse_number(foreign_value)

        price_value = extract_row_value(self.price_label)
        if price_value:
            parts = [part.strip() for part in price_value.split("/")]
            if parts:
                current_price = self._parse_number(parts[0])

        volume_value = extract_row_value(self.volume_label)
        if volume_value:
            parts = [part.strip() for part in volume_value.split("/")]
            if parts:
                current_volume = self._parse_number(parts[0])

        major_section_match = re.search(
            r'<caption class="blind">주요주주명, 보유주식수\(보통주\), 보유지분\(%\) 목록</caption>(.*?)</table>',
            html,
            re.I | re.S,
        )
        if major_section_match:
            rows_html = major_section_match.group(1)
            row_matches = re.findall(r"<tr class=\"p_sJJ[^\"]*\">(.*?)</tr>", rows_html, re.I | re.S)
            major_sum = 0.0
            for row_html in row_matches:
                cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, re.I | re.S)
                if len(cells) < 3:
                    continue
                name = self._strip_tags(cells[0])
                ratio = self._parse_number(self._strip_tags(cells[2]))
                if not name or ratio is None:
                    continue
                if self.national_pension_label in name:
                    continue
                if any(label in name for label in self.treasury_labels):
                    treasury_ratio = ratio
                    continue
                major_sum += ratio
            if major_sum > 0:
                major_holder_ratio = major_sum

        return {
            "market": market,
            "sector": sector,
            "industry": industry,
            "shares_outstanding": shares,
            "float_rate": float_rate,
            "foreign_ratio": foreign_ratio,
            "major_holder_ratio": major_holder_ratio,
            "treasury_ratio": treasury_ratio,
            "current_price": current_price,
            "current_volume": current_volume,
        }

    def fetch_stock_meta(self, ticker: str) -> NaverStockMeta:
        normalized = str(ticker).zfill(6)
        main_url = self.main_url.format(ticker=normalized)
        coinfo_url = self.coinfo_url.format(ticker=normalized)
        wisereport_url = None

        main_html = self.fetch_html(main_url)
        main_data = self.parse_main_page(normalized, main_html)

        meta_data: dict[str, object] = {}
        try:
            coinfo_html = self.fetch_html(coinfo_url)
            wisereport_url = self.extract_wisereport_url(coinfo_html)
            if wisereport_url:
                wisereport_html = self.fetch_html(wisereport_url)
                meta_data = self.parse_wisereport_page(wisereport_html)
        except Exception:
            meta_data = {}

        return NaverStockMeta(
            ticker=normalized,
            company=main_data.get("company"),
            market=main_data.get("market") or meta_data.get("market"),
            sector=meta_data.get("sector"),
            industry=meta_data.get("industry"),
            shares_outstanding=meta_data.get("shares_outstanding"),
            float_rate=meta_data.get("float_rate"),
            foreign_ratio=meta_data.get("foreign_ratio"),
            major_holder_ratio=meta_data.get("major_holder_ratio"),
            treasury_ratio=meta_data.get("treasury_ratio"),
            current_price=meta_data.get("current_price"),
            current_volume=meta_data.get("current_volume"),
            source_main_url=main_url,
            source_coinfo_url=coinfo_url,
            source_wisereport_url=wisereport_url,
        )

    def collect(self, tickers: Iterable[str]) -> CollectionResult:
        rows: list[dict[str, object]] = []
        errors: list[dict[str, str]] = []
        tickers = [str(t).zfill(6) for t in tickers]

        for ticker in tickers:
            try:
                meta = self.fetch_stock_meta(ticker)
                rows.append(asdict(meta))
            except Exception as exc:
                errors.append({"ticker": ticker, "message": str(exc)})

        return CollectionResult(
            source="naver",
            run_at=datetime.now(),
            requested=len(tickers),
            succeeded=len(rows),
            failed=len(errors),
            rows=rows,
            errors=errors,
        )
