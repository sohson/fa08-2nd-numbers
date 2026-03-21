from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from typing import Iterable

import pandas as pd
import requests

from src.collectors.models import CollectionResult, PriceSnapshot


class YahooCollector:
    base_url = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

    @staticmethod
    def to_symbol_candidates(ticker: str, market: str | None = None) -> list[str]:
        normalized = str(ticker).zfill(6)
        if market == "코스닥":
            return [f"{normalized}.KQ", f"{normalized}.KS"]
        if market == "코스피":
            return [f"{normalized}.KS", f"{normalized}.KQ"]
        return [f"{normalized}.KS", f"{normalized}.KQ"]

    def fetch_daily_snapshot(self, ticker: str, market: str | None = None) -> PriceSnapshot:
        last_error: Exception | None = None
        for symbol in self.to_symbol_candidates(ticker, market):
            try:
                response = requests.get(
                    self.base_url.format(symbol=symbol),
                    params={"interval": "1d", "range": "5d"},
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=10,
                )
                response.raise_for_status()
                payload = response.json()
                result = payload.get("chart", {}).get("result", [])
                if not result:
                    raise ValueError(f"Yahoo chart result is empty for {ticker} ({symbol})")

                chart = result[0]
                timestamps = chart.get("timestamp") or []
                quote = ((chart.get("indicators") or {}).get("quote") or [{}])[0]
                frame = pd.DataFrame(
                    {
                        "timestamp": timestamps,
                        "open": quote.get("open") or [],
                        "high": quote.get("high") or [],
                        "low": quote.get("low") or [],
                        "close": quote.get("close") or [],
                        "volume": quote.get("volume") or [],
                    }
                ).dropna(subset=["timestamp", "close"])
                if frame.empty:
                    raise ValueError(f"Yahoo price frame is empty for {ticker} ({symbol})")

                latest = frame.iloc[-1]
                date_value = (
                    pd.to_datetime(int(latest["timestamp"]), unit="s", utc=True)
                    .tz_convert("Asia/Seoul")
                    .strftime("%Y-%m-%d")
                )
                return PriceSnapshot(
                    ticker=str(ticker).zfill(6),
                    date=date_value,
                    open=float(latest["open"]) if pd.notna(latest["open"]) else None,
                    high=float(latest["high"]) if pd.notna(latest["high"]) else None,
                    low=float(latest["low"]) if pd.notna(latest["low"]) else None,
                    close=float(latest["close"]) if pd.notna(latest["close"]) else None,
                    volume=float(latest["volume"]) if pd.notna(latest["volume"]) else None,
                )
            except Exception as exc:
                last_error = exc
        raise last_error or ValueError(f"Yahoo lookup failed for {ticker}")

    def collect(
        self,
        tickers: Iterable[str],
        market_by_ticker: dict[str, str] | None = None,
        naver_price_fallback: dict[str, dict[str, object]] | None = None,
    ) -> CollectionResult:
        rows: list[dict[str, object]] = []
        errors: list[dict[str, str]] = []
        tickers = [str(t).zfill(6) for t in tickers]
        market_by_ticker = market_by_ticker or {}
        naver_price_fallback = naver_price_fallback or {}

        for ticker in tickers:
            try:
                snapshot = self.fetch_daily_snapshot(ticker, market_by_ticker.get(ticker))
                rows.append(asdict(snapshot))
            except Exception as exc:
                fallback = naver_price_fallback.get(ticker) or {}
                close = fallback.get("current_price")
                volume = fallback.get("current_volume")
                if close is not None:
                    rows.append(
                        asdict(
                            PriceSnapshot(
                                ticker=ticker,
                                date=datetime.now().strftime("%Y-%m-%d"),
                                open=None,
                                high=None,
                                low=None,
                                close=float(close),
                                volume=float(volume) if volume is not None else None,
                                source="naver_fallback",
                            )
                        )
                    )
                else:
                    errors.append({"ticker": ticker, "message": str(exc)})

        return CollectionResult(
            source="yahoo",
            run_at=datetime.now(),
            requested=len(tickers),
            succeeded=len(rows),
            failed=len(errors),
            rows=rows,
            errors=errors,
        )
