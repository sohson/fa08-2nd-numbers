from __future__ import annotations

import base64
from dataclasses import dataclass
from pathlib import Path
from datetime import date, timedelta
from io import StringIO
from typing import Any
from collections import Counter
from email.utils import parsedate_to_datetime
import html
import math
import os
import re

import numpy as np
import pandas as pd
import plotly.express as px
import requests
import shap
import streamlit as st

from src.config import AppConfig, load_config
from src.predictor import build_feature_story, compare_with_actual, load_model_package, run_prediction
from src.sql_dump import DataBundle, load_table


def period_sort_key(period: str) -> tuple[int, int]:
    year_text, half = period.split("_")
    return int(year_text), 1 if half == "H1" else 2


def normalize_ticker(value: Any) -> Any:
    if pd.isna(value):
        return value
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.zfill(6)


@st.cache_data(show_spinner=False)
def encode_image_to_data_uri(image_path: str) -> str:
    path = Path(image_path)
    if not path.exists():
        return ""
    suffix = path.suffix.lower()
    mime = "image/png" if suffix == ".png" else "image/jpeg"
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return f"data:{mime};base64,{encoded}"


def normalize_ticker_columns(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    for column in ["ticker"]:
        if column in normalized.columns:
            normalized[column] = normalized[column].apply(normalize_ticker)
    return normalized


EMAIL_PATTERN = re.compile(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$", re.IGNORECASE)


def get_subscriber_csv_path() -> Path:
    return Path(__file__).resolve().parent / "data" / "subscriptions" / "subscribers.csv"


def validate_email(email: str) -> bool:
    return bool(EMAIL_PATTERN.match(email.strip()))


def save_subscriber_email(email: str) -> tuple[bool, str]:
    normalized_email = email.strip().lower()
    if not normalized_email:
        return False, "?대찓?쇱쓣 ?낅젰??二쇱꽭??"
    if not validate_email(normalized_email):
        return False, "?щ컮瑜??대찓???뺤떇???꾨떃?덈떎."

    csv_path = get_subscriber_csv_path()
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    if csv_path.exists():
        existing = pd.read_csv(csv_path)
    else:
        existing = pd.DataFrame(columns=["email", "subscribed_at"])

    if "email" in existing.columns and normalized_email in existing["email"].astype(str).str.lower().tolist():
        return True, "?대? 援щ룆 ?좎껌???대찓?쇱엯?덈떎."

    new_row = pd.DataFrame(
        [{"email": normalized_email, "subscribed_at": pd.Timestamp.now(tz="Asia/Seoul").isoformat()}]
    )
    updated = pd.concat([existing, new_row], ignore_index=True)
    updated.to_csv(csv_path, index=False, encoding="utf-8-sig")
    return True, f"援щ룆 ?대찓?쇱씠 ??λ릺?덉뒿?덈떎. ????뚯씪: {csv_path.name}"


def to_number_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(",", "", regex=False), errors="coerce")


@st.cache_data(show_spinner=False, ttl=30)
def fetch_krx_json(url: str, api_key: str, bas_dd: str) -> dict[str, Any] | None:
    try:
        response = requests.get(
            url,
            params={"basDd": bas_dd},
            headers={"AUTH_KEY": api_key},
            timeout=10,
        )
        response.raise_for_status()
        return response.json()
    except Exception:
        return None


@st.cache_data(show_spinner=False, ttl=30)
def fetch_latest_kospi_market_snapshot(api_key: str | None) -> tuple[pd.DataFrame, str | None]:
    if not api_key:
        return pd.DataFrame(), None

    url = "https://data-dbg.krx.co.kr/svc/apis/sto/stk_bydd_trd"
    for offset in range(0, 14):
        bas_dd = (date.today() - timedelta(days=offset)).strftime("%Y%m%d")
        payload = fetch_krx_json(url, api_key, bas_dd)
        rows = (payload or {}).get("OutBlock_1") or []
        frame = pd.DataFrame(rows)
        if frame.empty or "MKT_NM" not in frame.columns:
            continue
        frame = frame.loc[frame["MKT_NM"].astype(str).str.upper() == "KOSPI"].copy()
        if frame.empty:
            continue
        rename_map = {
            "ISU_CD": "ticker",
            "ISU_NM": "company",
            "MKT_NM": "market",
            "TDD_CLSPRC": "close",
            "CMPPREVDD_PRC": "change",
            "FLUC_RT": "change_rate",
            "ACC_TRDVOL": "volume",
            "ACC_TRDVAL": "trading_value",
            "MKTCAP": "mktcap",
            "LIST_SHRS": "shares",
        }
        frame = frame.rename(columns=rename_map)
        for column in ["close", "change", "change_rate", "volume", "trading_value", "mktcap", "shares"]:
            if column in frame.columns:
                frame[column] = to_number_series(frame[column])
        frame["ticker"] = frame["ticker"].apply(normalize_ticker)
        frame["rank"] = frame["mktcap"].rank(ascending=False, method="first").astype(int)
        frame = frame.sort_values("rank").reset_index(drop=True)
        return frame, bas_dd
    return pd.DataFrame(), None


@st.cache_data(show_spinner=False, ttl=30)
def fetch_latest_kospi_index(api_key: str | None) -> tuple[dict[str, Any] | None, pd.DataFrame]:
    if not api_key:
        return None, pd.DataFrame()

    url = "https://data-dbg.krx.co.kr/svc/apis/idx/kospi_dd_trd"
    history_rows: list[dict[str, Any]] = []
    latest_row: dict[str, Any] | None = None

    for offset in range(0, 14):
        bas_dd = (date.today() - timedelta(days=offset)).strftime("%Y%m%d")
        payload = fetch_krx_json(url, api_key, bas_dd)
        rows = (payload or {}).get("OutBlock_1") or []
        frame = pd.DataFrame(rows)
        if frame.empty or "IDX_NM" not in frame.columns:
            continue
        if "IDX_CLSS" in frame.columns:
            target = frame.loc[frame["IDX_CLSS"].astype(str).str.upper() == "KOSPI"].copy()
        else:
            target = frame.loc[frame["IDX_NM"].astype(str).str.contains("코스피", na=False)].copy()
        if target.empty:
            continue
        exact = target.loc[target["IDX_NM"].astype(str).str.strip() == "코스피"].copy()
        if not exact.empty:
            target = exact
        else:
            target = target.loc[
                ~target["IDX_NM"].astype(str).str.contains("200|중형|소형|대형|금융|헬스|운송", na=False)
            ].copy()
            if target.empty:
                target = frame.loc[frame["IDX_NM"].astype(str).str.contains("코스피", na=False)].head(1).copy()
        row = target.iloc[0].to_dict()
        row["BAS_DD"] = bas_dd
        history_rows.append(row)
        if latest_row is None:
            latest_row = row
        if len(history_rows) >= 10:
            break

    history = pd.DataFrame(history_rows)
    if not history.empty:
        history["date"] = pd.to_datetime(history["BAS_DD"], format="%Y%m%d", errors="coerce")
        history["value"] = to_number_series(history["CLSPRC_IDX"])
        history = history.sort_values("date")

    if latest_row is None:
        return None, history

    latest = {
        "date": latest_row.get("BAS_DD"),
        "name": latest_row.get("IDX_NM"),
        "value": pd.to_numeric(str(latest_row.get("CLSPRC_IDX", "")).replace(",", ""), errors="coerce"),
        "change": pd.to_numeric(str(latest_row.get("CMPPREVDD_IDX", "")).replace(",", ""), errors="coerce"),
        "change_rate": pd.to_numeric(str(latest_row.get("FLUC_RT", "")).replace(",", ""), errors="coerce"),
        "mktcap": pd.to_numeric(str(latest_row.get("MKTCAP", "")).replace(",", ""), errors="coerce"),
    }
    return latest, history


@st.cache_data(show_spinner=False, ttl=30)
def fetch_yahoo_kospi_index() -> tuple[dict[str, Any] | None, pd.DataFrame]:
    url = "https://query1.finance.yahoo.com/v8/finance/chart/%5EKS11"
    try:
        response = requests.get(
            url,
            params={"interval": "1wk", "range": "5y"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
        result = payload.get("chart", {}).get("result", [])
        if not result:
            return None, pd.DataFrame()
        chart = result[0]
        timestamps = chart.get("timestamp") or []
        quote = ((chart.get("indicators") or {}).get("quote") or [{}])[0]
        closes = quote.get("close") or []
        opens = quote.get("open") or []

        history = pd.DataFrame({"timestamp": timestamps, "close": closes})
        history = history.dropna(subset=["timestamp", "close"]).copy()
        if history.empty:
            return None, pd.DataFrame()
        history["date"] = pd.to_datetime(history["timestamp"], unit="s", utc=True).dt.tz_convert("Asia/Seoul").dt.tz_localize(None)
        history["value"] = pd.to_numeric(history["close"], errors="coerce")
        history = history.dropna(subset=["value"]).sort_values("date")
        latest_value = float(history["value"].iloc[-1])
        prev_value = float(history["value"].iloc[-2]) if len(history) > 1 else None
        latest_open = pd.to_numeric(pd.Series(opens), errors="coerce").dropna()
        open_value = float(latest_open.iloc[-1]) if not latest_open.empty else prev_value
        base_value = prev_value if prev_value is not None else open_value
        change_value = latest_value - base_value if base_value is not None else None
        change_rate = (change_value / base_value * 100) if base_value not in (None, 0) else None
        latest = {
            "date": history["date"].iloc[-1].strftime("%Y-%m-%d"),
            "name": "KOSPI",
            "value": latest_value,
            "change": change_value,
            "change_rate": change_rate,
        }
        return latest, history[["date", "value"]].copy()
    except Exception:
        return None, pd.DataFrame()


def get_yahoo_symbol(ticker: str) -> str:
    normalized = normalize_ticker(ticker)
    return f"{normalized}.KS"


@st.cache_data(show_spinner=False, ttl=3600)
def fetch_yahoo_stock_history(ticker: str) -> pd.DataFrame:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{get_yahoo_symbol(ticker)}"
    try:
        response = requests.get(
            url,
            params={"interval": "1wk", "range": "5y"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
        result = payload.get("chart", {}).get("result", [])
        if not result:
            return pd.DataFrame()
        chart = result[0]
        timestamps = chart.get("timestamp") or []
        quote = ((chart.get("indicators") or {}).get("quote") or [{}])[0]
        closes = quote.get("close") or []
        history = pd.DataFrame({"timestamp": timestamps, "close": closes})
        history = history.dropna(subset=["timestamp", "close"]).copy()
        if history.empty:
            return pd.DataFrame()
        history["date"] = (
            pd.to_datetime(history["timestamp"], unit="s", utc=True)
            .dt.tz_convert("Asia/Seoul")
            .dt.tz_localize(None)
        )
        history["close"] = pd.to_numeric(history["close"], errors="coerce")
        return history.dropna(subset=["date", "close"])[["date", "close"]].sort_values("date")
    except Exception:
        return pd.DataFrame()


def _flatten_columns(columns: Any) -> list[str]:
    flattened: list[str] = []
    for column in columns:
        if isinstance(column, tuple):
            parts = [str(part).strip() for part in column if str(part).strip() and str(part).lower() != "nan"]
            flattened.append(" ".join(parts).strip())
        else:
            flattened.append(str(column).strip())
    return flattened


@st.cache_data(show_spinner=False, ttl=120)
def fetch_naver_market_sum_snapshot() -> tuple[pd.DataFrame, str | None]:
    page_frames: list[pd.DataFrame] = []
    fetched_at = pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M")

    for page in range(1, 5):
        try:
            response = requests.get(
                "https://finance.naver.com/sise/sise_market_sum.naver",
                params={"sosok": "0", "page": page},
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://finance.naver.com/"},
                timeout=10,
            )
            response.raise_for_status()
            html_text = response.text
            tables = pd.read_html(StringIO(html_text))
        except Exception:
            continue

        quote_table = None
        for table in tables:
            table.columns = _flatten_columns(table.columns)
            column_text = " ".join(table.columns)
            if "종목명" in column_text and "현재가" in column_text and "등락률" in column_text:
                quote_table = table
                break
        if quote_table is None:
            continue

        ticker_pairs = re.findall(
            r'href="/item/main\.naver\?code=(\d{6})"[^>]*>([^<]+)</a>',
            html_text,
            flags=re.I,
        )
        if not ticker_pairs:
            continue

        company_col = next((col for col in quote_table.columns if "종목명" in col), None)
        price_col = next((col for col in quote_table.columns if "현재가" in col), None)
        rate_col = next((col for col in quote_table.columns if "등락률" in col), None)
        if not company_col or not price_col or not rate_col:
            continue

        frame = quote_table[[company_col, price_col, rate_col]].copy()
        frame = frame.rename(columns={company_col: "company", price_col: "close", rate_col: "change_rate"})
        frame["company"] = frame["company"].astype(str).str.strip()
        frame = frame.loc[
            frame["company"].ne("")
            & frame["company"].ne("N")
            & frame["company"].ne("종목명")
            & ~frame["company"].str.contains("선택", na=False)
        ].copy()

        ticker_map = {name.strip(): code for code, name in ticker_pairs}
        frame["ticker"] = frame["company"].map(ticker_map)
        frame = frame.dropna(subset=["ticker"]).copy()
        frame["ticker"] = frame["ticker"].astype(str).apply(normalize_ticker)
        frame["close"] = to_number_series(frame["close"])
        frame["change_rate"] = pd.to_numeric(
            frame["change_rate"].astype(str).str.replace("%", "", regex=False).str.replace("+", "", regex=False),
            errors="coerce",
        )
        page_frames.append(frame[["ticker", "company", "close", "change_rate"]])

    if not page_frames:
        return pd.DataFrame(), None

    snapshot = pd.concat(page_frames, ignore_index=True)
    snapshot = snapshot.drop_duplicates(subset=["ticker"], keep="first").reset_index(drop=True)
    return snapshot, fetched_at


def get_future_prediction_basis_text(config: AppConfig) -> str:
    summary_path = config.auto_output_dir / "weekly_collection_summary.json"
    if summary_path.exists():
        try:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
            yahoo_run_at = ((payload or {}).get("yahoo") or {}).get("run_at")
            if yahoo_run_at:
                return str(yahoo_run_at)
        except Exception:
            pass

    yahoo_daily_path = config.auto_output_dir / "yahoo_price_daily.csv"
    if yahoo_daily_path.exists():
        try:
            frame = pd.read_csv(yahoo_daily_path)
            if "date" in frame.columns and not frame["date"].dropna().empty:
                latest_date = pd.to_datetime(frame["date"], errors="coerce").dropna().max()
                if pd.notna(latest_date):
                    return latest_date.strftime("%Y-%m-%d")
        except Exception:
            pass

    return "확인 불가"


@st.cache_data(show_spinner=False, ttl=180)
def fetch_live_kospi_market_snapshot(auto_output_dir: str) -> tuple[pd.DataFrame, str | None]:
    auto_dir = Path(auto_output_dir)
    meta_path = auto_dir / "naver_stock_meta_weekly.csv"
    if not meta_path.exists():
        return pd.DataFrame(), None

    try:
        meta = pd.read_csv(meta_path, dtype={"ticker": str})
    except Exception:
        return pd.DataFrame(), None

    if meta.empty:
        return pd.DataFrame(), None

    meta["ticker"] = meta["ticker"].apply(normalize_ticker)
    if "market" in meta.columns:
        market_text = meta["market"].fillna("").astype(str)
        meta = meta.loc[
            market_text.str.contains("KOSPI", case=False, na=False)
            | market_text.str.contains("코스피", na=False)
            | market_text.str.contains("유가증권", na=False)
        ].copy()
    if meta.empty:
        return pd.DataFrame(), None

    meta["shares_outstanding"] = pd.to_numeric(meta.get("shares_outstanding"), errors="coerce")
    meta = meta.dropna(subset=["shares_outstanding"]).copy()
    if meta.empty:
        return pd.DataFrame(), None

    quote_df, quote_basis = fetch_naver_market_sum_snapshot()
    if quote_df.empty:
        return pd.DataFrame(), None

    frame = meta.merge(quote_df, on="ticker", how="left", suffixes=("_meta", ""))
    if "company" not in frame.columns:
        if "company_meta" in frame.columns:
            frame["company"] = frame["company_meta"]
    elif "company_meta" in frame.columns:
        blank_company = frame["company"].fillna("").astype(str).str.strip().eq("")
        frame.loc[blank_company, "company"] = frame.loc[blank_company, "company_meta"]
    frame["current_price"] = pd.to_numeric(frame.get("current_price"), errors="coerce")
    frame["close"] = pd.to_numeric(frame.get("close"), errors="coerce").fillna(frame["current_price"])
    frame = frame.dropna(subset=["close", "shares_outstanding"]).copy()
    if frame.empty:
        return pd.DataFrame(), None

    frame["mktcap"] = frame["close"] * frame["shares_outstanding"]
    frame["rank"] = frame["mktcap"].rank(ascending=False, method="first").astype(int)
    frame = frame.sort_values("rank").reset_index(drop=True)
    return frame, quote_basis


def strip_html_tags(text: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", text or "")
    cleaned = html.unescape(cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def normalize_news_tokens(text: str) -> list[str]:
    return re.findall(r"[가-힣A-Za-z0-9]{2,}", (text or "").lower())


def dedupe_news_items(items: list[dict[str, Any]], company: str, limit: int = 5) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    company_tokens = set(normalize_news_tokens(company))
    for item in items:
        title_tokens = set(normalize_news_tokens(item.get("title", ""))) - company_tokens
        if not title_tokens:
            title_tokens = set(normalize_news_tokens(item.get("title", "")))
        is_duplicate = False
        for picked in selected:
            picked_tokens = set(normalize_news_tokens(picked.get("title", ""))) - company_tokens
            if not picked_tokens:
                picked_tokens = set(normalize_news_tokens(picked.get("title", "")))
            union = title_tokens | picked_tokens
            overlap = title_tokens & picked_tokens
            if union and (len(overlap) / len(union)) >= 0.5:
                is_duplicate = True
                break
        if not is_duplicate:
            selected.append(item)
        if len(selected) >= limit:
            break
    return selected


def score_news_item(item: dict[str, Any], company: str, ticker: str, sector: str | None = None) -> float:
    title = item.get("title", "")
    description = item.get("description", "")
    link = item.get("link", "")
    combined = f"{title} {description}".lower()
    company_lower = company.lower()
    ticker_norm = normalize_ticker(ticker).lower()
    sector_lower = (sector or "").lower()

    score = 0.0
    if company_lower and company_lower in combined:
        score += 6.0
    if ticker_norm and ticker_norm in combined:
        score += 2.5
    if sector_lower and sector_lower in combined:
        score += 1.0

    for token in normalize_news_tokens(company):
        if token in combined:
            score += 1.2

    trusted_domains = ("newsis.com", "yna.co.kr", "edaily.co.kr", "news.mt.co.kr", "sedaily.com", "hankyung.com")
    if any(domain in link for domain in trusted_domains):
        score += 0.8

    pub_date = item.get("pub_date")
    if pub_date is not None:
        try:
            age_days = max(
                0.0,
                (pd.Timestamp.now(tz="Asia/Seoul") - pd.Timestamp(pub_date).tz_convert("Asia/Seoul")).total_seconds() / 86400,
            )
            score += max(0.0, 2.5 - min(age_days, 30) / 12)
        except Exception:
            pass

    return score


def extract_news_keywords(items: list[dict[str, Any]], company: str, ticker: str, limit: int = 5) -> list[str]:
    stopwords = {
        "증시", "시장", "주가", "종목", "코스피", "코스닥", "관련", "뉴스", "기사", "오늘",
        "오전", "오후", "기자", "단독", "속보", "전망", "실적", "발표", "공시", "기준",
        "분석", "투자", "상승", "하락", "매수", "매도", "업종", "섹터",
    }
    company_tokens = set(normalize_news_tokens(company)) | {normalize_ticker(ticker).lower()}
    counter: Counter[str] = Counter()
    for item in items:
        joined_text = f"{item.get('title', '')} {item.get('description', '')}"
        for token in normalize_news_tokens(joined_text):
            if token in stopwords or token in company_tokens:
                continue
            if len(token) < 2 or token.isdigit():
                continue
            counter[token] += 1
    return [token for token, _ in counter.most_common(limit)]


@st.cache_data(show_spinner=False, ttl=1800)
def fetch_naver_news(
    company: str,
    ticker: str,
    sector: str | None,
    client_id: str | None,
    client_secret: str | None,
) -> dict[str, Any]:
    if not client_id or not client_secret:
        return {"items": [], "keywords": [], "status": "missing_key"}

    try:
        raw_items: list[dict[str, Any]] = []
        search_queries = [company, f"{company} {normalize_ticker(ticker)}", f"{company} 관련주"]
        if sector:
            search_queries.append(f"{company} {sector}")
        for query in search_queries:
            response = requests.get(
                "https://openapi.naver.com/v1/search/news.json",
                params={"query": query, "display": 25, "sort": "date"},
                headers={
                    "X-Naver-Client-Id": client_id,
                    "X-Naver-Client-Secret": client_secret,
                },
                timeout=10,
            )
            response.raise_for_status()
            payload = response.json()
            raw_items.extend(payload.get("items", []))

        parsed_items: list[dict[str, Any]] = []
        cutoff = pd.Timestamp.now(tz="Asia/Seoul") - pd.Timedelta(days=90)
        for item in raw_items:
            title = strip_html_tags(item.get("title", ""))
            description = strip_html_tags(item.get("description", ""))
            link = item.get("originallink") or item.get("link") or ""
            pub_date_raw = item.get("pubDate", "")
            pub_date = None
            if pub_date_raw:
                try:
                    pub_date = parsedate_to_datetime(pub_date_raw)
                    if pub_date.tzinfo is None:
                        pub_date = pub_date.replace(tzinfo=pd.Timestamp.now(tz="Asia/Seoul").tz)
                except Exception:
                    pub_date = None
            parsed_items.append(
                {
                    "title": title,
                    "description": description,
                    "link": link,
                    "type": "뉴스",
                    "pub_date": pub_date,
                    "pub_date_text": pub_date.astimezone().strftime("%Y-%m-%d") if pub_date else "",
                }
            )

        filtered = [item for item in parsed_items if company in item["title"] or company in item["description"]]
        if not filtered:
            filtered = parsed_items
        for item in filtered:
            item["relevance"] = score_news_item(item, company, ticker, sector)
        filtered = sorted(
            filtered,
            key=lambda item: (
                item.get("relevance", 0),
                item.get("pub_date") or pd.Timestamp.min.tz_localize("Asia/Seoul"),
            ),
            reverse=True,
        )
        recent_items = [
            item for item in filtered
            if item["pub_date"] is not None and pd.Timestamp(item["pub_date"]).tz_convert("Asia/Seoul") >= cutoff
        ]
        issue_items = dedupe_news_items(recent_items or filtered, company, limit=5)
        keywords = extract_news_keywords(recent_items or filtered, company, ticker)
        return {"items": issue_items, "keywords": keywords, "status": "ok"}
    except Exception:
        return {"items": [], "keywords": [], "status": "error"}


FEATURE_LABEL_MAP = {
    "period_rank": "시총 순위",
    "treasury_ratio": "자사주 비율",
    "sector_relative_rank": "섹터 내 상대 순위",
    "prev_rank": "전기 순위",
    "major_holder_ratio": "주요주주 지분율",
    "sector_rank": "업종 내 순위",
    "foreign_change": "외국인 지분 변화",
    "turnover_ratio": "거래 회전율",
    "sector_member_score": "섹터 편입 점수",
    "float_mktcap": "유동 시가총액",
    "float_rate": "유동 비율",
    "avg_mktcap": "평균 시가총액",
}


def compute_shap_rows(selected_row: pd.Series, model_package: dict[str, Any]) -> list[dict[str, Any]]:
    features = model_package.get("features", [])
    model = model_package.get("model")
    if model is None or not features:
        return []

    matrix = pd.DataFrame([selected_row]).reindex(columns=features).fillna(0)
    try:
        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(matrix)
        if isinstance(shap_values, list):
            values = np.asarray(shap_values[-1])[0]
        else:
            values = np.asarray(shap_values)
            if values.ndim == 3:
                values = values[0, :, -1]
            elif values.ndim == 2:
                values = values[0]
        values = np.asarray(values, dtype=float)
    except Exception:
        fallback_rows = []
        for item in build_feature_story(selected_row):
            fallback_rows.append(
                {
                    "label": item["label"],
                    "value": item["value"],
                    "tone": "positive" if item["tone"] == "positive" else "negative",
                    "width": 50.0,
                }
            )
        return fallback_rows

    order = np.argsort(np.abs(values))[::-1][:6]
    top_values = values[order]
    max_abs = np.max(np.abs(top_values)) if len(top_values) else 1.0
    rows: list[dict[str, Any]] = []
    for idx in order:
        value = float(values[idx])
        rows.append(
            {
                "label": FEATURE_LABEL_MAP.get(features[idx], features[idx]),
                "value": f"{value:+.3f}",
                "tone": "positive" if value >= 0 else "negative",
                "width": max(14.0, (abs(value) / max_abs) * 100) if max_abs else 14.0,
            }
        )
    return rows


@st.cache_data(show_spinner=False)
def load_bundle(config: AppConfig) -> DataBundle:
    macro = pd.read_csv(config.macro_csv_path)
    feature_krx = normalize_ticker_columns(load_table(config.sql_dump_path, "feature_krx"))
    filter_flag = normalize_ticker_columns(load_table(config.sql_dump_path, "filter_flag"))
    foreign_holding = normalize_ticker_columns(load_table(config.sql_dump_path, "foreign_holding"))
    kospi_friday_daily = normalize_ticker_columns(load_table(config.sql_dump_path, "kospi_friday_daily"))
    labels = normalize_ticker_columns(load_table(config.sql_dump_path, "labels"))
    major_holder = normalize_ticker_columns(load_table(config.sql_dump_path, "major_holder"))
    stock_meta = normalize_ticker_columns(load_table(config.sql_dump_path, "stock_meta"))
    return DataBundle(
        feature_krx=feature_krx,
        filter_flag=filter_flag,
        foreign_holding=foreign_holding,
        kospi_friday_daily=kospi_friday_daily,
        labels=labels,
        major_holder=major_holder,
        period=load_table(config.sql_dump_path, "period"),
        sector_map=load_table(config.sql_dump_path, "sector_map"),
        stock_meta=stock_meta,
        predictions=normalize_ticker_columns(load_table(config.sql_dump_path, "predictions")),
        macro=macro,
    )


@st.cache_resource(show_spinner=False)
def load_model(config: AppConfig) -> dict[str, Any]:
    return load_model_package(config.model_pkl_path)


@dataclass
class PeriodContext:
    frame: pd.DataFrame
    period_start: pd.Timestamp | None
    period_end: pd.Timestamp | None
    ticker_to_name: dict[str, str]


def get_period_date_map(bundle: DataBundle) -> dict[str, tuple[pd.Timestamp, pd.Timestamp]]:
    mapping: dict[str, tuple[pd.Timestamp, pd.Timestamp]] = {}
    for _, row in bundle.period.dropna(subset=["period"]).iterrows():
        mapping[row["period"]] = (row["period_start"], row["period_end"])
    return mapping


def build_dynamic_feature_krx(bundle: DataBundle, period: str) -> pd.DataFrame:
    period_map = get_period_date_map(bundle)
    if period not in period_map:
        return pd.DataFrame(columns=bundle.feature_krx.columns)

    period_start, period_end = period_map[period]
    daily = bundle.kospi_friday_daily.copy()
    if daily.empty:
        return pd.DataFrame(columns=bundle.feature_krx.columns)

    daily["trade_date"] = pd.to_datetime(daily["date"].astype(str), format="%Y%m%d", errors="coerce")
    latest_trade_date = daily["trade_date"].dropna().max()
    if pd.isna(latest_trade_date):
        return pd.DataFrame(columns=bundle.feature_krx.columns)

    effective_end = min(period_end, latest_trade_date)
    daily = daily.loc[(daily["trade_date"] >= period_start) & (daily["trade_date"] <= effective_end)].copy()
    if daily.empty:
        return pd.DataFrame(columns=bundle.feature_krx.columns)

    sector_map = bundle.sector_map.rename(
        columns={"gics_sector_2023": "mapped_gics_sector", "krx_group": "mapped_krx_group"}
    )
    meta = bundle.stock_meta[["ticker", "ksic_sector"]].copy()
    meta = meta.merge(sector_map[["ksic_sector", "mapped_gics_sector", "mapped_krx_group"]], on="ksic_sector", how="left")
    daily = daily.merge(meta, on="ticker", how="left")

    grouped = (
        daily.groupby("ticker")
        .agg(
            avg_mktcap=("mktcap", "mean"),
            avg_close=("close", "mean"),
            avg_volume=("volume", "mean"),
            avg_trading_value=("trading_value", "mean"),
            float_ratio=("shares", lambda values: 1.0),
            gics_sector=("mapped_gics_sector", "last"),
            krx_group=("mapped_krx_group", "last"),
        )
        .reset_index()
    )
    grouped["period"] = period
    grouped["period_rank"] = grouped["avg_mktcap"].rank(ascending=False, method="first").astype(int)
    grouped["turnover_ratio"] = grouped["avg_trading_value"] / grouped["avg_mktcap"].replace(0, pd.NA)
    grouped["turnover_ratio"] = grouped["turnover_ratio"].fillna(0.0)
    grouped["float_ratio"] = grouped["float_ratio"].fillna(1.0)
    grouped["gics_sector"] = grouped["gics_sector"].fillna("湲고?")
    grouped["krx_group"] = grouped["krx_group"].fillna("湲고?")

    result = grouped[["period", "ticker", "avg_mktcap", "float_ratio", "gics_sector", "krx_group", "period_rank", "turnover_ratio"]].copy()
    result = result.sort_values("period_rank").head(300).reset_index(drop=True)
    result["period_rank"] = result["avg_mktcap"].rank(ascending=False, method="first").astype(int)
    return result


def get_available_periods(bundle: DataBundle) -> list[str]:
    periods = set(bundle.feature_krx["period"].dropna().unique().tolist())
    config = load_config()
    if config.auto_output_dir.exists():
        for path in config.auto_output_dir.glob("weekly_predictions_*.csv"):
            period = path.stem.replace("weekly_predictions_", "", 1)
            if period:
                periods.add(period)
    period_map = get_period_date_map(bundle)
    for period in period_map:
        if period not in periods:
            dynamic_frame = build_dynamic_feature_krx(bundle, period)
            if not dynamic_frame.empty:
                periods.add(period)
    return sorted(list(periods), key=period_sort_key, reverse=True)


def get_previous_period(bundle: DataBundle, current_period: str) -> str | None:
    all_periods = sorted(get_period_date_map(bundle).keys(), key=period_sort_key)
    if current_period not in all_periods:
        return None
    index = all_periods.index(current_period)
    return all_periods[index - 1] if index > 0 else None


def build_base_period_frame(bundle: DataBundle, period: str) -> pd.DataFrame:
    feature_krx = bundle.feature_krx.loc[bundle.feature_krx["period"] == period].copy()
    if feature_krx.empty:
        feature_krx = build_dynamic_feature_krx(bundle, period)
    if feature_krx.empty:
        return pd.DataFrame()

    labels = bundle.labels.loc[bundle.labels["period"] == period].copy()
    major = bundle.major_holder.loc[bundle.major_holder["period"] == period].copy()
    meta = bundle.stock_meta.copy()
    period_row = bundle.period.loc[bundle.period["period"] == period]

    period_start = period_row["period_start"].iloc[0] if not period_row.empty else None
    period_end = period_row["period_end"].iloc[0] if not period_row.empty else None

    def date_to_ym(value: pd.Timestamp) -> int:
        return value.year * 100 + value.month

    fh_agg = pd.DataFrame(columns=["ticker", "avg_foreign_ratio", "last_foreign_ratio", "avg_exhaustion_rate", "period"])
    macro_agg = pd.DataFrame([{"period": period}])
    if period_start is not None and period_end is not None:
        ym_start = date_to_ym(period_start)
        ym_end = date_to_ym(period_end)

        foreign_mask = (bundle.foreign_holding["ym"] >= ym_start) & (bundle.foreign_holding["ym"] <= ym_end)
        foreign_sub = bundle.foreign_holding.loc[foreign_mask].copy()
        if not foreign_sub.empty:
            fh_agg = (
                foreign_sub.groupby("ticker")
                .agg(
                    avg_foreign_ratio=("foreign_holding_ratio", "mean"),
                    last_foreign_ratio=("foreign_holding_ratio", "last"),
                    avg_exhaustion_rate=("foreign_limit_exhaustion_rate", "mean"),
                )
                .reset_index()
            )
            fh_agg["period"] = period

        macro_mask = (bundle.macro["ym"] >= ym_start) & (bundle.macro["ym"] <= ym_end)
        macro_sub = bundle.macro.loc[macro_mask].copy()
        macro_row = {"period": period}
        for column in [col for col in bundle.macro.columns if col != "ym"]:
            if not macro_sub.empty:
                macro_row[f"macro_{column}_mean"] = macro_sub[column].mean()
                macro_row[f"macro_{column}_last"] = macro_sub[column].iloc[-1]
            else:
                macro_row[f"macro_{column}_mean"] = None
                macro_row[f"macro_{column}_last"] = None
        macro_agg = pd.DataFrame([macro_row])

    sector_dict_gics = dict(zip(bundle.sector_map["ksic_sector"], bundle.sector_map["gics_sector_2023"]))
    sector_dict_krx = dict(zip(bundle.sector_map["ksic_sector"], bundle.sector_map["krx_group"]))

    frame = feature_krx.copy()
    if not labels.empty:
        frame = frame.merge(
            labels[["period", "ticker", "was_member", "label_in", "label_out", "actual_rank", "is_member"]],
            on=["period", "ticker"],
            how="left",
        )
    else:
        for column in ["was_member", "label_in", "label_out", "actual_rank", "is_member"]:
            frame[column] = None

    for column in ["was_member", "label_in", "label_out", "is_member"]:
        if column in frame.columns:
            frame[column] = frame[column].fillna(0).astype(int)

    frame = frame.merge(
        major[["period", "ticker", "major_holder_ratio", "treasury_ratio", "non_float_ratio", "float_rate"]],
        on=["period", "ticker"],
        how="left",
    )
    frame = frame.merge(fh_agg, on=["period", "ticker"], how="left")
    frame = frame.merge(macro_agg, on="period", how="left")
    frame = frame.merge(
        meta[["ticker", "is_not_common", "is_reits", "list_date", "ksic_sector"]],
        on="ticker",
        how="left",
    )

    if not bundle.filter_flag.empty:
        flag_frame = bundle.filter_flag[["ticker", "flag_date", "is_managed", "is_warning"]].copy()
        flag_frame = flag_frame.loc[flag_frame["flag_date"] == period]
        flag_frame = flag_frame.rename(columns={"flag_date": "period"})
        frame = frame.merge(flag_frame, on=["period", "ticker"], how="left")

    frame["gics_sector"] = frame.get("gics_sector")
    frame["krx_group"] = frame.get("krx_group")
    frame["gics_sector"] = frame["gics_sector"].fillna(frame["ksic_sector"].map(sector_dict_gics))
    frame["krx_group"] = frame["krx_group"].fillna(frame["ksic_sector"].map(sector_dict_krx))
    frame["period_start"] = period_start
    frame["period_end"] = period_end

    return frame


@st.cache_data(show_spinner=False)
def build_historical_model_frame(config: AppConfig, _bundle: DataBundle, _package: dict[str, Any]) -> pd.DataFrame:
    bundle = _bundle
    package = _package
    historical_periods = sorted(bundle.feature_krx["period"].dropna().unique().tolist(), key=period_sort_key)

    frames = [build_base_period_frame(bundle, period) for period in historical_periods]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame()

    history = pd.concat(frames, ignore_index=True)
    period_map = {period: index for index, period in enumerate(sorted(get_period_date_map(bundle).keys(), key=period_sort_key))}
    history["period_idx"] = history["period"].map(period_map)
    history = history.sort_values(["ticker", "period_idx"]).reset_index(drop=True)

    history["prev_was_member"] = pd.to_numeric(history["was_member"], errors="coerce")
    history["prev_rank"] = history.groupby("ticker")["period_rank"].shift(1)
    history["rank_change"] = history["period_rank"] - history["prev_rank"]
    history["prev_mktcap"] = history.groupby("ticker")["avg_mktcap"].shift(1)
    history["mktcap_change"] = (history["avg_mktcap"] - history["prev_mktcap"]) / history["prev_mktcap"].replace(0, pd.NA)
    history["prev_foreign"] = history.groupby("ticker")["avg_foreign_ratio"].shift(1)
    history["foreign_change"] = history["avg_foreign_ratio"] - history["prev_foreign"]
    history["prev_turnover"] = history.groupby("ticker")["turnover_ratio"].shift(1)
    history["turnover_change"] = history["turnover_ratio"] - history["prev_turnover"]

    history["list_date"] = pd.to_datetime(history["list_date"], errors="coerce")
    history["period_end"] = pd.to_datetime(history["period_end"], errors="coerce")
    history["months_listed"] = (
        (history["period_end"].dt.year - history["list_date"].dt.year) * 12
        + (history["period_end"].dt.month - history["list_date"].dt.month)
    )
    exclude = (
        (history["is_not_common"].fillna(0) == 1)
        | ((history["float_rate"] < 0.10) & history["float_rate"].notna())
        | (history["is_reits"].fillna(0) == 1)
        | ((history["months_listed"] < 6) & history["months_listed"].notna())
    )
    history = history.loc[~exclude].copy()
    history = history.loc[history["prev_was_member"].notna()].copy()

    le_gics = package["le_gics"]
    le_krx = package["le_krx"]
    gics_classes = set(le_gics.classes_)
    krx_classes = set(le_krx.classes_)
    default_sector = "기타"
    history["gics_sector_clean"] = history["gics_sector"].fillna(default_sector).astype(str).apply(
        lambda value: value if value in gics_classes else default_sector
    )
    history["krx_group_clean"] = history["krx_group"].fillna(default_sector).astype(str).apply(
        lambda value: value if value in krx_classes else default_sector
    )
    history["gics_sector_enc"] = le_gics.transform(history["gics_sector_clean"])
    history["krx_group_enc"] = le_krx.transform(history["krx_group_clean"])

    history["float_mktcap"] = history["avg_mktcap"] * history["float_rate"].fillna(0)
    history["float_mktcap_rank"] = history.groupby("period")["float_mktcap"].rank(ascending=False, method="first").astype(int)
    history["dist_from_200"] = history["period_rank"] - 200
    history["float_dist_from_200"] = history["float_mktcap_rank"] - 200

    def calc_consecutive(group: pd.DataFrame) -> list[int]:
        counts: list[int] = []
        count = 0
        for _, row in group.iterrows():
            if row["prev_was_member"] == 1:
                count += 1
            else:
                count = 0
            counts.append(count)
        return counts

    consecutive_values: list[int] = []
    for _, group in history.groupby("ticker"):
        consecutive_values.extend(calc_consecutive(group))
    history["consecutive_member"] = consecutive_values

    history["sector_rank"] = history.groupby(["period", "gics_sector_enc"])["period_rank"].rank(method="first").astype(int)
    history["sector_count"] = history.groupby(["period", "gics_sector_enc"])["ticker"].transform("count")
    history["sector_relative_rank"] = history["sector_rank"] / history["sector_count"]
    history["prev_rank_change"] = history.groupby("ticker")["rank_change"].shift(1)
    history["rank_acceleration"] = history["rank_change"] - history["prev_rank_change"].fillna(0)
    history["sector_member_score"] = history["gics_sector_enc"].map(package.get("sector_in_map", {})).fillna(0.5)
    history["prev_foreign_change"] = history.groupby("ticker")["foreign_change"].shift(1)
    history["foreign_acceleration"] = history["foreign_change"] - history["prev_foreign_change"].fillna(0)

    return history.reset_index(drop=True)


def build_period_context(bundle: DataBundle, period: str) -> PeriodContext:
    config = load_config()
    model_package = load_model(config)
    history_frame = build_historical_model_frame(config, bundle, model_package)
    if not history_frame.empty and period in history_frame["period"].unique():
        latest_snapshot = (
            bundle.kospi_friday_daily.sort_values("date")
            .dropna(subset=["ticker"])
            .drop_duplicates(subset=["ticker"], keep="last")
        )
        ticker_to_name = dict(zip(latest_snapshot["ticker"], latest_snapshot["company"]))
        period_row = bundle.period.loc[bundle.period["period"] == period]
        period_start = period_row["period_start"].iloc[0] if not period_row.empty else None
        period_end = period_row["period_end"].iloc[0] if not period_row.empty else None
        frame = history_frame.loc[history_frame["period"] == period].copy()
        return PeriodContext(frame=frame, period_start=period_start, period_end=period_end, ticker_to_name=ticker_to_name)

    feature_krx = bundle.feature_krx.loc[bundle.feature_krx["period"] == period].copy()
    if feature_krx.empty:
        feature_krx = build_dynamic_feature_krx(bundle, period)
    major = bundle.major_holder.loc[bundle.major_holder["period"] == period].copy()
    labels = bundle.labels.loc[bundle.labels["period"] == period].copy()
    meta = bundle.stock_meta.copy()

    period_row = bundle.period.loc[bundle.period["period"] == period]
    period_start = period_row["period_start"].iloc[0] if not period_row.empty else None
    period_end = period_row["period_end"].iloc[0] if not period_row.empty else None
    prev_period = get_previous_period(bundle, period)

    def date_to_ym(value: pd.Timestamp) -> int:
        return value.year * 100 + value.month

    fh_agg = pd.DataFrame(
        columns=["ticker", "avg_foreign_ratio", "last_foreign_ratio", "avg_exhaustion_rate", "period"]
    )
    macro_agg = pd.DataFrame([{"period": period}])
    if period_start is not None and period_end is not None:
        ym_start = date_to_ym(period_start)
        ym_end = date_to_ym(period_end)

        foreign_mask = (bundle.foreign_holding["ym"] >= ym_start) & (bundle.foreign_holding["ym"] <= ym_end)
        foreign_sub = bundle.foreign_holding.loc[foreign_mask].copy()
        if not foreign_sub.empty:
            fh_agg = (
                foreign_sub.groupby("ticker")
                .agg(
                    avg_foreign_ratio=("foreign_holding_ratio", "mean"),
                    last_foreign_ratio=("foreign_holding_ratio", "last"),
                    avg_exhaustion_rate=("foreign_limit_exhaustion_rate", "mean"),
                )
                .reset_index()
            )
            fh_agg["period"] = period

        macro_mask = (bundle.macro["ym"] >= ym_start) & (bundle.macro["ym"] <= ym_end)
        macro_sub = bundle.macro.loc[macro_mask].copy()
        macro_row = {"period": period}
        for column in [col for col in bundle.macro.columns if col != "ym"]:
            if not macro_sub.empty:
                macro_row[f"macro_{column}_mean"] = macro_sub[column].mean()
                macro_row[f"macro_{column}_last"] = macro_sub[column].iloc[-1]
            else:
                macro_row[f"macro_{column}_mean"] = None
                macro_row[f"macro_{column}_last"] = None
        macro_agg = pd.DataFrame([macro_row])

    prev_feature = pd.DataFrame()
    prev_fh_agg = pd.DataFrame()
    if prev_period is not None:
        prev_feature = bundle.feature_krx.loc[bundle.feature_krx["period"] == prev_period].copy()
        if prev_feature.empty:
            prev_feature = build_dynamic_feature_krx(bundle, prev_period)

        prev_period_row = bundle.period.loc[bundle.period["period"] == prev_period]
        if not prev_period_row.empty:
            prev_start = prev_period_row["period_start"].iloc[0]
            prev_end = prev_period_row["period_end"].iloc[0]
            ym_prev_start = date_to_ym(prev_start)
            ym_prev_end = date_to_ym(prev_end)
            prev_foreign_mask = (bundle.foreign_holding["ym"] >= ym_prev_start) & (bundle.foreign_holding["ym"] <= ym_prev_end)
            prev_foreign_sub = bundle.foreign_holding.loc[prev_foreign_mask].copy()
            if not prev_foreign_sub.empty:
                prev_fh_agg = (
                    prev_foreign_sub.groupby("ticker")
                    .agg(
                        prev_foreign_ratio=("foreign_holding_ratio", "mean"),
                        prev_last_foreign_ratio=("foreign_holding_ratio", "last"),
                        prev_exhaustion_rate=("foreign_limit_exhaustion_rate", "mean"),
                    )
                    .reset_index()
                )

    sector_dict_gics = dict(zip(bundle.sector_map["ksic_sector"], bundle.sector_map["gics_sector_2023"]))
    sector_dict_krx = dict(zip(bundle.sector_map["ksic_sector"], bundle.sector_map["krx_group"]))

    latest_snapshot = (
        bundle.kospi_friday_daily.sort_values("date")
        .dropna(subset=["ticker"])
        .drop_duplicates(subset=["ticker"], keep="last")
    )
    ticker_to_name = dict(zip(latest_snapshot["ticker"], latest_snapshot["company"]))

    frame = feature_krx.copy()
    if not prev_feature.empty:
        prev_feature = prev_feature[["ticker", "period_rank", "avg_mktcap", "turnover_ratio"]].rename(
            columns={
                "period_rank": "prev_rank",
                "avg_mktcap": "prev_avg_mktcap",
                "turnover_ratio": "prev_turnover_ratio",
            }
        )
        frame = frame.merge(prev_feature, on="ticker", how="left")
    if not labels.empty:
        frame = frame.merge(
            labels[["period", "ticker", "was_member", "label_in", "label_out", "actual_rank", "is_member"]],
            on=["period", "ticker"],
            how="left",
        )
    else:
        for column in ["was_member", "label_in", "label_out", "actual_rank", "is_member"]:
            frame[column] = None

    frame = frame.merge(
        major[
            [
                "period",
                "ticker",
                "major_holder_ratio",
                "treasury_ratio",
                "non_float_ratio",
                "float_rate",
            ]
        ],
        on=["period", "ticker"],
        how="left",
    )
    frame = frame.merge(fh_agg, on=["period", "ticker"], how="left")
    if not prev_fh_agg.empty:
        frame = frame.merge(prev_fh_agg, on="ticker", how="left")
    frame = frame.merge(macro_agg, on="period", how="left")
    frame = frame.merge(
        meta[["ticker", "is_not_common", "is_reits", "list_date", "ksic_sector"]],
        on="ticker",
        how="left",
    )

    if not bundle.filter_flag.empty:
        flag_frame = bundle.filter_flag[["ticker", "flag_date", "is_managed", "is_warning"]].copy()
        flag_frame = flag_frame.loc[flag_frame["flag_date"] == period]
        flag_frame = flag_frame.rename(columns={"flag_date": "period"})
        frame = frame.merge(flag_frame, on=["period", "ticker"], how="left")

    if "gics_sector" not in frame.columns:
        frame["gics_sector"] = None
    if "krx_group" not in frame.columns:
        frame["krx_group"] = None
    frame["gics_sector"] = frame["gics_sector"].fillna(frame["ksic_sector"].map(sector_dict_gics))
    frame["krx_group"] = frame["krx_group"].fillna(frame["ksic_sector"].map(sector_dict_krx))

    for column in ["prev_rank", "prev_avg_mktcap", "prev_foreign_ratio", "prev_turnover_ratio"]:
        if column not in frame.columns:
            frame[column] = pd.NA
    frame["rank_change"] = frame["period_rank"] - frame["prev_rank"]
    prev_avg_mktcap_nonzero = frame["prev_avg_mktcap"].replace(0, pd.NA)
    frame["mktcap_change"] = (frame["avg_mktcap"] - frame["prev_avg_mktcap"]) / prev_avg_mktcap_nonzero
    frame["foreign_change"] = frame["avg_foreign_ratio"] - frame["prev_foreign_ratio"]
    frame["turnover_change"] = frame["turnover_ratio"] - frame["prev_turnover_ratio"]

    return PeriodContext(frame=frame, period_start=period_start, period_end=period_end, ticker_to_name=ticker_to_name)

def get_prev_period(package: dict[str, Any], current_period: str) -> str | None:
    period_order = package.get("period_order", [])
    if current_period in period_order:
        index = period_order.index(current_period)
        return period_order[index - 1] if index > 0 else None
    return None


def get_prev_members(package: dict[str, Any], current_period: str) -> set[str]:
    prev_period = get_prev_period(package, current_period)
    if prev_period is None:
        return set()
    return set(package.get("actual_members", {}).get(prev_period, []))


def get_actual_members(package: dict[str, Any], current_period: str) -> set[str]:
    return set(package.get("actual_members", {}).get(current_period, []))


def build_actual_change_result(
    bundle: DataBundle,
    context: PeriodContext,
    period: str,
    prediction_result: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    labels = bundle.labels.loc[bundle.labels["period"] == period].copy()
    if labels.empty:
        return None

    labels = normalize_ticker_columns(labels)
    labels["ticker"] = labels["ticker"].astype(str).apply(normalize_ticker)
    labels["label_in"] = pd.to_numeric(labels.get("label_in"), errors="coerce").fillna(0).astype(int)
    labels["label_out"] = pd.to_numeric(labels.get("label_out"), errors="coerce").fillna(0).astype(int)
    labels["actual_rank"] = pd.to_numeric(labels.get("actual_rank"), errors="coerce")

    snapshot = context.frame.copy()
    lookup_columns = [column for column in ["ticker", "company", "gics_sector", "krx_group"] if column in snapshot.columns]
    if lookup_columns:
        snapshot = snapshot[lookup_columns].drop_duplicates(subset=["ticker"], keep="last")
        labels = labels.merge(snapshot, on="ticker", how="left")

    labels["company"] = labels.get("company", labels["ticker"])
    labels["company"] = labels["company"].fillna(labels["ticker"].astype(str).map(context.ticker_to_name))
    labels["company"] = labels["company"].fillna(labels["ticker"])
    blank_company = labels["company"].astype(str).str.strip().eq("")
    if blank_company.any():
        labels.loc[blank_company, "company"] = (
            labels.loc[blank_company, "ticker"].astype(str).map(context.ticker_to_name)
        )
    company_is_ticker = labels["company"].astype(str).str.strip().eq(labels["ticker"].astype(str).str.strip())
    if company_is_ticker.any():
        labels.loc[company_is_ticker, "company"] = (
            labels.loc[company_is_ticker, "ticker"].astype(str).map(context.ticker_to_name)
        )
    blank_company = labels["company"].astype(str).str.strip().eq("")
    if blank_company.any():
        labels.loc[blank_company, "company"] = labels.loc[blank_company, "ticker"]

    actual_in = (
        labels.loc[labels["label_in"] == 1]
        .sort_values(["actual_rank", "ticker"], ascending=[True, True], na_position="last")
        .copy()
    )
    actual_out = (
        labels.loc[labels["label_out"] == 1]
        .sort_values(["actual_rank", "ticker"], ascending=[True, True], na_position="last")
        .copy()
    )

    return {
        "actual_in": actual_in,
        "actual_out": actual_out,
        "actual_in_set": set(actual_in["ticker"].astype(str)),
        "actual_out_set": set(actual_out["ticker"].astype(str)),
    }


def get_stored_prediction_result(
    bundle: DataBundle,
    context: PeriodContext,
    package: dict[str, Any],
    period: str,
) -> dict[str, Any] | None:
    generated_result = get_generated_prediction_result(context, package, period)
    if generated_result is not None:
        return generated_result

    stored = bundle.predictions.loc[bundle.predictions["period"] == period].copy()
    if stored.empty:
        return None

    latest_run = stored["run_date"].max()
    stored = stored.loc[stored["run_date"] == latest_run].copy()
    stored["ticker"] = stored["ticker"].apply(normalize_ticker)

    frame = context.frame.copy()
    frame["ticker"] = frame["ticker"].apply(normalize_ticker)
    merged = frame.merge(
        stored[
            [
                "ticker",
                "company",
                "score",
                "pred_rank",
                "period_rank",
                "pred_top200",
                "strong_in",
                "strong_out",
                "prev_member",
                "model_version",
            ]
        ],
        on="ticker",
        how="right",
        suffixes=("", "_stored"),
    )

    for column in ["company", "score", "pred_rank", "period_rank", "pred_top200", "strong_in", "strong_out"]:
        stored_column = f"{column}_stored"
        if stored_column in merged.columns:
            merged[column] = merged[stored_column]
            merged = merged.drop(columns=[stored_column])

    if "company" not in merged.columns:
        merged["company"] = merged["ticker"].map(context.ticker_to_name)
    else:
        company_blank = merged["company"].fillna("").astype(str).str.strip().eq("")
        if company_blank.any():
            merged.loc[company_blank, "company"] = (
                merged.loc[company_blank, "ticker"].astype(str).map(context.ticker_to_name)
            )

    merged["score_pct"] = merged["score"] * 100
    merged["prev_was_member"] = merged["prev_member"].fillna(0).astype(int)
    merged = merged.sort_values(["pred_rank", "period_rank"], ascending=[True, True]).reset_index(drop=True)

    top200 = set(merged.loc[merged["pred_top200"] == 1, "ticker"].astype(str))
    strong_in = merged.loc[merged["strong_in"] == 1].sort_values(["score", "pred_rank"], ascending=[False, True]).copy()
    strong_out = merged.loc[merged["strong_out"] == 1].sort_values(["score", "pred_rank"], ascending=[True, True]).copy()

    return {
        "period": period,
        "scored": merged,
        "top200": top200,
        "pred_in": set(strong_in["ticker"].astype(str)),
        "pred_out": set(strong_out["ticker"].astype(str)),
        "missing_members": set(),
        "strong_in": strong_in,
        "strong_out": strong_out,
        "summary": {
            "run_date": pd.to_datetime(latest_run).date().isoformat(),
            "model_label": f"{package.get('method', 'model')} - {package.get('model_name', 'unknown')}",
            "model_version": stored["model_version"].iloc[0] if "model_version" in stored.columns else package.get("model_version", "unknown"),
            "feature_count": len(package.get("features", [])),
            "candidate_count": len(merged),
        },
    }


def get_generated_prediction_result(
    context: PeriodContext,
    package: dict[str, Any],
    period: str,
) -> dict[str, Any] | None:
    config = load_config()
    scored_path = config.auto_output_dir / f"weekly_predictions_{period}.csv"
    if not scored_path.exists():
        return None

    try:
        scored = pd.read_csv(scored_path)
    except Exception:
        return None

    if scored.empty:
        return None

    scored = normalize_ticker_columns(scored)
    if "score" in scored.columns:
        scored["score"] = pd.to_numeric(scored["score"], errors="coerce")
    if "score_pct" in scored.columns:
        scored["score_pct"] = pd.to_numeric(scored["score_pct"], errors="coerce")
    else:
        scored["score_pct"] = scored["score"] * 100

    for column in ["pred_rank", "period_rank", "pred_top200", "strong_in", "strong_out", "prev_was_member"]:
        if column in scored.columns:
            scored[column] = pd.to_numeric(scored[column], errors="coerce").fillna(0)
            if column != "score_pct":
                scored[column] = scored[column].astype(int)

    if "company" not in scored.columns:
        scored["company"] = scored["ticker"].map(context.ticker_to_name)

    scored = scored.sort_values(["pred_rank", "period_rank"], ascending=[True, True]).reset_index(drop=True)
    top200 = set(scored.loc[scored["pred_top200"] == 1, "ticker"].astype(str))
    strong_in = scored.loc[scored["strong_in"] == 1].sort_values(["score", "pred_rank"], ascending=[False, True]).copy()
    strong_out = scored.loc[scored["strong_out"] == 1].sort_values(["score", "pred_rank"], ascending=[True, True]).copy()

    file_mtime = pd.Timestamp(scored_path.stat().st_mtime, unit="s").tz_localize("UTC").tz_convert("Asia/Seoul")
    return {
        "period": period,
        "scored": scored,
        "top200": top200,
        "pred_in": set(strong_in["ticker"].astype(str)),
        "pred_out": set(strong_out["ticker"].astype(str)),
        "missing_members": set(),
        "strong_in": strong_in,
        "strong_out": strong_out,
        "summary": {
            "run_date": file_mtime.date().isoformat(),
            "model_label": f"{package.get('method', 'model')} - {package.get('model_name', 'unknown')}",
            "model_version": package.get("model_version", "unknown"),
            "feature_count": len(package.get("features", [])),
            "candidate_count": len(scored),
        },
    }


def make_csv(prediction_result: dict[str, Any], bundle: DataBundle | None = None, period: str | None = None) -> bytes:
    frame = prediction_result["scored"].copy()
    needs_actual_labels = "label_in" not in frame.columns or "label_out" not in frame.columns
    if needs_actual_labels and bundle is not None and period is not None and not bundle.labels.empty:
        labels = bundle.labels.loc[bundle.labels["period"].astype(str) == str(period)].copy()
        if not labels.empty:
            labels = normalize_ticker_columns(labels)
            labels["ticker"] = labels["ticker"].astype(str).apply(normalize_ticker)
            merge_columns = [column for column in ["ticker", "label_in", "label_out"] if column in labels.columns]
            if "ticker" in merge_columns:
                labels = labels[merge_columns].drop_duplicates(subset=["ticker"], keep="last")
                frame["ticker"] = frame["ticker"].astype(str).apply(normalize_ticker)
                frame = frame.merge(labels, on="ticker", how="left")
    export_columns = [
        "ticker",
        "company",
        "gics_sector",
        "score",
        "score_pct",
        "pred_rank",
        "period_rank",
        "pred_top200",
        "strong_in",
        "strong_out",
        "prev_was_member",
        "label_in",
        "label_out",
    ]
    frame = frame[[column for column in export_columns if column in frame.columns]]
    return frame.to_csv(index=False).encode("utf-8-sig")


def render_metric_card(title: str, value: str, caption: str = "") -> None:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-title">{title}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-caption">{caption}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def build_stock_card_html(row: pd.Series, card_type: str) -> str:
    color = "#43d36f" if card_type == "in" else "#ff6767"
    fill_class = "green" if card_type == "in" else "red"
    label = "\uD3B8\uC785 \uC720\uB825" if card_type == "in" else "\uD3B8\uCD9C \uC720\uB825"
    sector = row.get("gics_sector") or row.get("krx_group") or "\uBBF8\uBD84\uB958"
    score_value = float(row.get("score", 0))
    progress_width = min(max(score_value * 100, 0), 100)
    return f"""
        <div class="stock-item">
            <div class="stock-top">
                <div>
                    <div class="stock-name">{row.get('company', '')}</div>
                    <div class="stock-sub">{row['ticker']} \u00B7 {sector}</div>
                </div>
                <div class="stock-score-wrap">
                    <div class="stock-score">
                        <strong style="color:{color};">{score_value:.4f}\uC810</strong>
                        <span class="tag {'green' if card_type == 'in' else 'red'}">{label}</span>
                    </div>
                </div>
            </div>
            <div class="progress">
                <div class="fill {fill_class}" style="width:{progress_width:.1f}%;"></div>
            </div>
        </div>
    """


def render_stock_section(frame: pd.DataFrame, card_type: str) -> None:
    with st.container(height=620):
        for _, row in frame.iterrows():
            ticker = str(row.get("ticker", ""))
            st.markdown(build_stock_card_html(row, card_type), unsafe_allow_html=True)
            _, trigger_col = st.columns([0.76, 0.24])
            with trigger_col:
                st.markdown('<div class="detail-trigger">', unsafe_allow_html=True)
                if st.button("\uC885\uBAA9 \uC0C1\uC138", key=f"detail_{card_type}_{ticker}", use_container_width=True):
                    st.session_state["detail_ticker"] = ticker
                    st.rerun()
                st.markdown("</div>", unsafe_allow_html=True)


def build_actual_stock_card_html(row: pd.Series, card_type: str) -> str:
    color = "#43d36f" if card_type == "in" else "#ff6767"
    tag_label = "\uC2E4\uC81C \uD3B8\uC785" if card_type == "in" else "\uC2E4\uC81C \uD3B8\uCD9C"
    sector = row.get("gics_sector")
    if pd.isna(sector) or not str(sector).strip():
        sector = row.get("krx_group")
    if pd.isna(sector) or not str(sector).strip():
        sector = "\uBBF8\uBD84\uB958"
    actual_rank = row.get("actual_rank")
    rank_text = f"\uC2E4\uC81C \uC21C\uC704 {int(actual_rank)}" if pd.notna(actual_rank) else "\uC2E4\uC81C \uC21C\uC704 \uC815\uBCF4 \uC5C6\uC74C"
    company = row.get("company", "")
    if pd.isna(company) or not str(company).strip():
        company = row.get("ticker", "")
    return f"""
        <div class="stock-item">
            <div class="stock-top">
                <div>
                    <div class="stock-name">{company}</div>
                    <div class="stock-sub">{row['ticker']} \u00B7 {sector}</div>
                </div>
                <div class="stock-score-wrap">
                    <div class="stock-score">
                        <strong style="color:{color}; font-size:22px;">{rank_text}</strong>
                        <span class="tag {'green' if card_type == 'in' else 'red'}">{tag_label}</span>
                    </div>
                </div>
            </div>
        </div>
    """


def render_actual_stock_section(frame: pd.DataFrame, card_type: str) -> None:
    with st.container(height=620):
        for _, row in frame.iterrows():
            st.markdown(build_actual_stock_card_html(row, card_type), unsafe_allow_html=True)


def render_shap_section(selected_row: pd.Series, model_package: dict[str, Any]) -> None:
    shap_rows = compute_shap_rows(selected_row, model_package)
    if not shap_rows:
        st.info("\uD53C\uCC98 \uAE30\uC5EC\uB3C4\uB97C \uACC4\uC0B0\uD560 \uC218 \uC5C6\uC2B5\uB2C8\uB2E4.")
        return

    rows_html = []
    for row in shap_rows:
        color = "#43d36f" if row["tone"] == "positive" else "#ff6d78"
        gradient = (
            "linear-gradient(90deg,#35c868,#75ea97)"
            if row["tone"] == "positive"
            else "linear-gradient(90deg,#ff6d74,#ff8f8f)"
        )
        rows_html.append(
            f'<div class="shap-row">'
            f"<div>{row['label']}</div>"
            f'<div class="mini-bar"><span style="width:{row["width"]:.1f}%; background:{gradient};"></span></div>'
            f'<div style="font-weight:800; color:{color};">{row["value"]}</div>'
            f"</div>"
        )

    st.markdown(
        (
            '<div class="subcard">'
            "<h4>SHAP \uD53C\uCC98 \uAE30\uC5EC\uB3C4</h4>"
            '<div class="shap-list">'
            f'{"".join(rows_html)}'
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def render_news_section(selected_row: pd.Series, config: AppConfig) -> None:
    company = str(selected_row.get("company", "") or "")
    ticker = str(selected_row.get("ticker", "") or "")
    sector = str(selected_row.get("gics_sector") or selected_row.get("krx_group") or "")
    news_payload = fetch_naver_news(company, ticker, sector, config.naver_client_id, config.naver_client_secret)
    keywords = news_payload.get("keywords", [])
    items = news_payload.get("items", [])
    status = news_payload.get("status")

    keyword_html = "".join(f'<span class="keyword-chip">{keyword}</span>' for keyword in keywords)
    if items:
        news_html = "".join(
            f'<a class="news-item" href="{item["link"]}" target="_blank" rel="noopener noreferrer">'
            f'<span class="news-type">{item.get("type", "\uB274\uC2A4")}</span>'
            f'<span class="news-title">{item["title"]}</span>'
            f'<span class="news-meta">{item.get("pub_date_text", "")} \u00B7 \uB9C1\uD06C \uC5F4\uAE30</span>'
            f"</a>"
            for item in items
        )
    else:
        if status == "missing_key":
            message = "\uB124\uC774\uBC84 \uB274\uC2A4 API \uD0A4\uAC00 \uC5C6\uC5B4 \uB274\uC2A4\uB97C \uBCF4\uC5EC\uC904 \uC218 \uC5C6\uC2B5\uB2C8\uB2E4."
        elif status == "error":
            message = "\uB274\uC2A4 \uC815\uBCF4\uB97C \uAC00\uC838\uC624\uB294 \uC911 \uC624\uB958\uAC00 \uBC1C\uC0DD\uD588\uC2B5\uB2C8\uB2E4."
        else:
            message = "\uAD00\uB828 \uB274\uC2A4\uAC00 \uC544\uC9C1 \uC5C6\uC2B5\uB2C8\uB2E4."
        news_html = f'<div class="news-item"><span class="news-title">{message}</span></div>'

    keyword_section = (
        f'<div class="keyword-wrap">{keyword_html}</div>'
        if keyword_html
        else '<div class="keyword-wrap"><span class="keyword-chip">\uD0A4\uC6CC\uB4DC \uC5C6\uC74C</span></div>'
    )

    st.markdown(
        (
            '<div class="subcard">'
            "<h4>\uAD00\uB828 \uC774\uC288</h4>"
            f"{keyword_section}"
            '<div class="news-list">'
            f"{news_html}"
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def render_price_section(bundle: DataBundle, selected_row: pd.Series) -> None:
    ticker = str(selected_row.get("ticker", "") or "")
    history = fetch_yahoo_stock_history(ticker)
    if history.empty:
        price_frame = bundle.kospi_friday_daily.loc[bundle.kospi_friday_daily["ticker"] == ticker].copy()
        price_frame = price_frame.dropna(subset=["date", "close"]).sort_values("date")
        if not price_frame.empty:
            price_frame["date"] = pd.to_datetime(price_frame["date"].astype(str), format="%Y%m%d", errors="coerce")
            history = price_frame[["date", "close"]].dropna()

    st.markdown('<div class="subcard"><h4>\uC8FC\uAC00 \uCD94\uC774</h4>', unsafe_allow_html=True)
    if history.empty:
        st.info("\uC8FC\uAC00 \uB370\uC774\uD130\uB97C \uCC3E\uC9C0 \uBABB\uD588\uC2B5\uB2C8\uB2E4.")
    else:
        fig = px.line(history, x="date", y="close", markers=False)
        fig.update_traces(line_color="#56a8ff")
        fig.update_layout(
            margin=dict(l=10, r=10, t=10, b=10),
            height=360,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#dbe7f8",
            xaxis_title=None,
            yaxis_title=None,
            title=None,
            showlegend=False,
            hoverlabel=dict(bgcolor="#0f1b2d", font_color="#e8eef9"),
        )
        fig.update_xaxes(showgrid=True, gridcolor="rgba(156,179,207,0.16)")
        fig.update_yaxes(showgrid=True, gridcolor="rgba(156,179,207,0.16)")
        fig.update_traces(
            hovertemplate="\uB0A0\uC9DC %{x|%Y-%m-%d}<br>\uC885\uAC00 %{y:,.0f}\uC6D0<extra></extra>"
        )
        st.plotly_chart(fig, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)


@st.fragment(run_every="180s")
def render_live_sidebar(config: AppConfig, latest_date: Any) -> None:
    live_quote_df, live_quote_basis = fetch_naver_market_sum_snapshot()
    if not live_quote_df.empty:
        realtime_market = live_quote_df.copy().reset_index(drop=True)
        realtime_market["rank"] = range(1, len(realtime_market) + 1)
        realtime_market_date = live_quote_basis
        market_source = "네이버"
    else:
        realtime_market, realtime_market_date = fetch_latest_kospi_market_snapshot(config.krxdata_api_key)
        market_source = "저장 스냅샷"
    latest_market_top = realtime_market.head(200).copy() if not realtime_market.empty else pd.DataFrame()
    realtime_index, market_trend = fetch_yahoo_kospi_index()

    index_value = realtime_index["value"] if realtime_index else None
    index_change = realtime_index["change"] if realtime_index else None
    index_change_rate = realtime_index["change_rate"] if realtime_index else None
    index_basis = realtime_index["date"] if realtime_index else (
        realtime_market_date or (str(latest_date) if latest_date is not None else "N/A")
    )
    market_update_text = (
        f"업데이트 {realtime_market_date} · {market_source}"
        if realtime_market_date
        else f"업데이트 시각 없음 · {market_source}"
    )
    value_text = f"{index_value:,.2f}" if index_value is not None and pd.notna(index_value) else "N/A"
    change_text = (
        f"{index_change:+,.2f} ({index_change_rate:+.2f}%)"
        if index_change is not None and pd.notna(index_change) and index_change_rate is not None and pd.notna(index_change_rate)
        else f"기준일 {index_basis}"
    )

    st.markdown(
        f"""
        <div class="aside-card index-card">
            <div class="side-stat-title">실시간 코스피 지수</div>
            <div class="side-stat-value">{value_text}</div>
            <div class="side-stat-change">{change_text}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if not market_trend.empty:
        market_trend["date"] = pd.to_datetime(market_trend["date"], errors="coerce")
        trend_fig = px.area(market_trend, x="date", y="value")
        trend_fig.update_traces(line_color="#43d36f", fillcolor="rgba(67,211,111,0.14)")
        trend_fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            height=220,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#dbe7f8",
            xaxis_title=None,
            yaxis_title=None,
        )
        trend_fig.update_xaxes(showgrid=False)
        trend_fig.update_yaxes(showgrid=False, showticklabels=False)
        st.plotly_chart(trend_fig, use_container_width=True)

    st.markdown(
        f"""
        <div class="aside-card watch-card">
            <div class="panel-head">
                <div>
                    <div class="section-title">시총 순위 200</div>
                    <div class="section-sub">코스피 포함 종목 순위 · 시가총액 기준 상위 종목</div>
                    <div class="section-sub">{market_update_text}</div>
                </div>
                <span class="tag blue">실시간</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if latest_market_top.empty:
        st.info("실시간 시장 데이터가 없습니다.")
    else:
        with st.container(height=920):
            for index, (_, row) in enumerate(latest_market_top.iterrows(), start=1):
                change_rate = row.get("change_rate")
                rate_text = f"{float(change_rate):+.2f}%" if pd.notna(change_rate) else "-"
                delta_color = "#48da7b" if pd.notna(change_rate) and float(change_rate) >= 0 else "#ff6d78"
                price_text = f"{float(row.get('close')):,.0f}원" if pd.notna(row.get("close")) else "가격 없음"
                company = str(row.get("company", "") or "")
                ticker = str(row.get("ticker", "") or "")
                st.markdown(
                    f"""
                    <div class="rank-row">
                        <div class="rank-num">{index}</div>
                        <div>
                            <div>{company}</div>
                            <div class="rank-value">{ticker} · {price_text}</div>
                        </div>
                        <div class="rank-delta" style="color:{delta_color};">{rate_text}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )


@st.dialog("\uC885\uBAA9 \uC0C1\uC138", width="large")
def render_stock_modal(bundle: DataBundle, selected_row: pd.Series, model_package: dict[str, Any], config: AppConfig) -> None:
    company = str(selected_row.get("company", "") or "")
    ticker = str(selected_row.get("ticker", "") or "")
    sector = selected_row.get("gics_sector") or selected_row.get("krx_group") or "\uBBF8\uBD84\uB958"
    score_value = float(selected_row.get("score", 0))
    modal_logo_uri = encode_image_to_data_uri(str(config.project_root / "assets" / "next200_logo_badge.png"))

    st.markdown(
        f"""
        <div class="modal-head">
            <div>
                <div class="modal-title">{company} ({ticker}) \uC885\uBAA9 \uC0C1\uC138</div>
                <div class="modal-subtitle">{sector} \u00B7 \uC608\uCE21 \uC810\uC218 {score_value:.4f}\uC810</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    col1, col2 = st.columns([1.05, 0.95], gap="large")
    with col1:
        render_shap_section(selected_row, model_package)
        render_news_section(selected_row, config)
    with col2:
        render_price_section(bundle, selected_row)
    if modal_logo_uri:
        st.markdown(
            f'<div class="modal-brand-corner"><img src="{modal_logo_uri}" alt="NEXT200 logo"></div>',
            unsafe_allow_html=True,
        )


def apply_global_css() -> None:
    st.markdown(
        """
        <style>
        [data-testid="stHeader"] {
            background: rgba(5, 11, 20, 0.72);
            backdrop-filter: blur(10px);
        }
        [data-testid="stToolbar"] {
            right: 18px;
            top: 10px;
        }
        [data-testid="stAppViewContainer"] > .main {
            padding-top: 0.4rem;
        }
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(46,82,160,0.18), transparent 26%),
                radial-gradient(circle at top right, rgba(0,166,255,0.12), transparent 22%),
                linear-gradient(180deg, #050b14 0%, #07101d 100%);
        }
        .block-container {
            padding-top: 4.8rem;
            padding-bottom: 2.4rem;
            max-width: none;
        }
        [data-testid="stVerticalBlock"] > [data-testid="stVerticalBlockBorderWrapper"] > div:has(> div > [data-testid="stSelectbox"]),
        [data-testid="stVerticalBlock"] > [data-testid="stVerticalBlockBorderWrapper"] > div:has(> div > [data-testid="stTextInput"]) {
            margin-bottom: 10px;
        }
        div[data-baseweb="select"] > div,
        .stTextInput input,
        .stDownloadButton button,
        .stButton button {
            border-radius: 14px !important;
            border: 1px solid rgba(115,134,162,0.18) !important;
            background: rgba(255,255,255,0.04) !important;
            color: #e8eef9 !important;
            min-height: 48px !important;
        }
        .stTextInput input::placeholder {
            color: #6c819d !important;
        }
        .stDownloadButton button,
        .stButton button[kind="primary"] {
            background: linear-gradient(180deg, #3b8eff, #2666ff) !important;
            border: 0 !important;
            box-shadow: 0 14px 24px rgba(38, 102, 255, 0.28) !important;
            font-weight: 800 !important;
        }
        label[data-testid="stWidgetLabel"] p {
            font-size: 12px !important;
            font-weight: 800 !important;
            letter-spacing: 0.04em !important;
            color: #89a0be !important;
            text-transform: uppercase !important;
        }
        .hero-card, .metric-card, .stock-card, .panel-card, .aside-card, .subcard {
            border: 1px solid rgba(115,134,162,0.18);
            background: linear-gradient(180deg, rgba(17, 28, 46, 0.98), rgba(11, 20, 34, 0.98));
            border-radius: 22px;
            box-shadow: 0 18px 44px rgba(0, 0, 0, 0.30);
        }
        .hero-card {
            padding: 24px 28px;
            margin-bottom: 16px;
        }
        .topbar {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 14px;
            padding: 22px 24px;
        }
        .title-wrap h2 {
            margin: 0;
            font-size: 30px;
            font-weight: 900;
            letter-spacing: -0.03em;
            color: #e8eef9;
        }
        .title-wrap p {
            margin: 8px 0 0;
            color: #89a0be;
            font-size: 14px;
            line-height: 1.6;
        }
        .toolbar {
            display: flex;
            align-items: center;
            gap: 10px;
            flex-wrap: wrap;
            justify-content: flex-end;
        }
        .pill {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 10px 14px;
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.04);
            border: 1px solid rgba(115,134,162,0.18);
            color: #b8c8dd;
            font-size: 13px;
            font-weight: 700;
            white-space: nowrap;
        }
        .csv-btn .stDownloadButton > button {
            width: 100%;
            padding: 12px 16px;
            border-radius: 14px;
            border: 1px solid rgba(86, 168, 255, 0.24);
            background: rgba(86, 168, 255, 0.08);
            color: #d7e7ff;
            font-weight: 800;
        }
        .hero-title {
            font-size: 38px;
            font-weight: 900;
            letter-spacing: -0.04em;
            color: #e8eef9;
        }
        .hero-sub {
            margin-top: 8px;
            color: #91a7c4;
            font-size: 15px;
        }
        .metric-card {
            padding: 18px 20px;
            min-height: 132px;
        }
        .metric-title {
            color: #89a0be;
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-weight: 800;
        }
        .metric-value {
            color: #eef4ff;
            font-size: 34px;
            font-weight: 900;
            margin-top: 10px;
        }
        .metric-caption {
            color: #8ea6c5;
            font-size: 13px;
            margin-top: 8px;
            line-height: 1.5;
        }
        .stock-card {
            padding: 16px 16px 14px 16px;
            margin-bottom: 12px;
        }
        .stock-item {
            border: 1px solid rgba(115,134,162,0.18);
            border-radius: 18px;
            padding: 14px 14px 12px;
            background: rgba(255, 255, 255, 0.025);
            margin-bottom: 12px;
            transition: transform 0.18s ease, border-color 0.18s ease, background 0.18s ease;
        }
        .stock-item:hover {
            transform: translateY(-2px);
            border-color: rgba(86, 168, 255, 0.32);
            background: rgba(86, 168, 255, 0.05);
        }
        .stock-top {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 12px;
            margin-bottom: 8px;
        }
        .stock-head {
            display: flex;
            justify-content: space-between;
            gap: 12px;
            align-items: center;
        }
        .stock-name {
            color: #edf3ff;
            font-size: 22px;
            font-weight: 800;
            letter-spacing: -0.03em;
        }
        .stock-sub {
            color: #91a7c4;
            font-size: 12px;
            margin-top: 4px;
        }
        .stock-score {
            text-align: right;
            white-space: nowrap;
        }
        .stock-score strong {
            display: block;
            font-size: 28px;
            letter-spacing: -0.03em;
        }
        .stock-pill {
            display: inline-block;
            margin-top: 12px;
            margin-bottom: 10px;
            padding: 7px 12px;
            border-radius: 999px;
            font-size: 12px;
            font-weight: 800;
        }
        .pill-in {
            background: rgba(72,218,123,0.13);
            color: #48da7b;
        }
        .pill-out {
            background: rgba(255,109,120,0.13);
            color: #ff6d78;
        }
        .stock-progress {
            height: 8px;
            border-radius: 999px;
            overflow: hidden;
            background: rgba(255,255,255,0.08);
        }
        .progress {
            height: 8px;
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.07);
            overflow: hidden;
            margin-top: 10px;
        }
        .fill {
            height: 100%;
            border-radius: inherit;
        }
        .fill.green { background: linear-gradient(90deg, #2cc767, #75ea97); }
        .fill.red { background: linear-gradient(90deg, #ff5e67, #ff8d8d); }
        .fill.amber { background: linear-gradient(90deg, #da9d34, #f3c160); }
        .stock-progress-fill {
            height: 100%;
            border-radius: inherit;
        }
          .panel-card, .aside-card {
              padding: 18px 20px;
          }
          .sidebar-brand {
              display: flex;
              align-items: center;
              justify-content: center;
              margin-bottom: 16px;
              padding: 18px 20px;
              border-radius: 22px;
              border: 1px solid rgba(115,134,162,0.18);
              background: linear-gradient(180deg, rgba(17, 28, 46, 0.98), rgba(11, 20, 34, 0.98));
              box-shadow: 0 18px 44px rgba(0, 0, 0, 0.30);
          }
          .sidebar-brand img {
              width: 100%;
              max-width: 210px;
              height: auto;
              display: block;
          }
          .panel {
              padding: 20px;
          }
        .section-title {
            margin: 0;
            font-size: 15px;
            font-weight: 800;
            color: #e6eefb;
        }
        .section-sub {
            margin-top: 6px;
            color: #89a0be;
            font-size: 12px;
            line-height: 1.6;
        }
        .analysis-item {
            margin-top: 12px;
            padding: 14px 16px;
            border-radius: 16px;
            border: 1px solid rgba(115,134,162,0.14);
            background: rgba(255,255,255,0.03);
        }
        .analysis-item strong {
            display: block;
            color: #e7eef9;
            font-size: 14px;
            margin-bottom: 8px;
        }
          .analysis-item p {
              margin: 0;
              color: #91a7c4;
              font-size: 12px;
              line-height: 1.7;
          }
          .modal-brand-corner {
              display: flex;
              justify-content: flex-end;
              margin-top: 14px;
          }
          .modal-brand-corner img {
              width: 136px;
              max-width: 34%;
              height: auto;
              border-radius: 18px;
              box-shadow: 0 14px 28px rgba(0, 0, 0, 0.28);
              opacity: 0.96;
          }
          .aside-card {
              margin-bottom: 16px;
          }
        .side-stat-title {
            color: #dbe7f8;
            font-size: 14px;
            font-weight: 800;
        }
        .side-stat-value {
            margin-top: 10px;
            color: #eef4ff;
            font-size: 46px;
            font-weight: 900;
            letter-spacing: -0.04em;
        }
        .side-stat-change {
            margin-top: 8px;
            color: #48da7b;
            font-size: 18px;
            font-weight: 800;
        }
        .panel-head {
            display: flex;
            justify-content: space-between;
            gap: 12px;
            align-items: flex-start;
        }
        .tag {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-width: 52px;
            padding: 8px 12px;
            border-radius: 999px;
            font-size: 12px;
            font-weight: 800;
        }
        .tag.blue {
            background: rgba(86, 168, 255, 0.14);
            color: #8fc3ff;
            border: 1px solid rgba(86, 168, 255, 0.24);
        }
        .detail-trigger {
            margin: -4px 0 14px;
        }
        .detail-trigger .stButton > button {
            border-radius: 14px;
            border: 1px solid rgba(86, 168, 255, 0.26);
            background: rgba(86, 168, 255, 0.08);
            color: #d7e7ff;
            font-weight: 800;
            padding: 12px 14px;
        }
        .section-card {
            padding: 20px;
            border: 1px solid rgba(115,134,162,0.18);
            background: linear-gradient(180deg, rgba(17, 28, 46, 0.98), rgba(11, 20, 34, 0.98));
            border-radius: 22px;
            box-shadow: 0 18px 44px rgba(0, 0, 0, 0.30);
            margin-bottom: 16px;
        }
        .rank-row {
            display: grid;
            grid-template-columns: 28px minmax(0, 1fr) auto;
            gap: 12px;
            align-items: center;
            padding: 14px 6px;
            border-bottom: 1px solid rgba(115,134,162,0.14);
        }
        .rank-num {
            color: #56a8ff;
            font-size: 22px;
            font-weight: 900;
            text-align: center;
        }
        .rank-value {
            margin-top: 4px;
            color: #91a7c4;
            font-size: 12px;
        }
        .rank-delta {
            font-size: 15px;
            font-weight: 800;
            color: #48da7b;
            white-space: nowrap;
        }
        .modal-head {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 12px;
            margin-bottom: 18px;
            padding-bottom: 18px;
            border-bottom: 1px solid rgba(115,134,162,0.16);
        }
        .modal-title {
            font-size: 22px;
            font-weight: 900;
            color: #eaf1fb;
            letter-spacing: -0.03em;
        }
        .modal-subtitle {
            margin-top: 8px;
            color: #89a0be;
            font-size: 13px;
        }
        .subcard {
            padding: 18px;
            background: rgba(255,255,255,0.03);
        }
        .subcard h4 {
            margin: 0 0 16px;
            font-size: 15px;
            color: #dbe7f8;
        }
        .shap-list {
            display: grid;
            gap: 12px;
        }
        .shap-row {
            display: grid;
            grid-template-columns: 110px 1fr 62px;
            gap: 10px;
            align-items: center;
            font-size: 13px;
            color: #c8d6ea;
        }
        .mini-bar {
            height: 8px;
            border-radius: 999px;
            overflow: hidden;
            background: rgba(255,255,255,0.07);
        }
        .mini-bar > span {
            display: block;
            height: 100%;
            border-radius: inherit;
        }
        .keyword-wrap {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin-bottom: 14px;
        }
        .keyword-chip {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 6px 10px;
            border-radius: 999px;
            background: rgba(86, 168, 255, 0.14);
            color: #9ecbff;
            font-size: 12px;
            font-weight: 700;
        }
        .news-list {
            display: grid;
            gap: 10px;
        }
        .news-item {
            display: block;
            padding: 14px;
            border-radius: 16px;
            background: rgba(12, 22, 37, 0.88);
            border: 1px solid rgba(86, 168, 255, 0.12);
            text-decoration: none;
        }
        .news-type {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-width: 46px;
            padding: 6px 10px;
            margin-right: 10px;
            border-radius: 999px;
            background: rgba(86, 168, 255, 0.14);
            color: #8fc3ff;
            font-size: 11px;
            font-weight: 800;
            vertical-align: middle;
        }
        .news-title {
            font-size: 14px;
            font-weight: 700;
            color: #dce8f9;
            vertical-align: middle;
            line-height: 1.55;
        }
        .news-meta {
            display: block;
            margin-top: 8px;
            color: #89a0be;
            font-size: 12px;
        }
        [data-testid="stDialog"] [data-testid="stVerticalBlock"] {
            gap: 18px;
        }
        [data-testid="stDialog"] > div[role="dialog"] {
            border-radius: 28px;
            border: 1px solid rgba(115,134,162,0.20);
            background: linear-gradient(180deg, rgba(15, 25, 41, 0.98), rgba(10, 18, 30, 0.98));
            box-shadow: 0 32px 90px rgba(0, 0, 0, 0.45);
        }
        [data-testid="stDialog"] button[aria-label="Close"] {
            color: #89a0be !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def main() -> None:
    st.set_page_config(page_title="KOSPI200 \uD3B8\uC785\u00B7\uD3B8\uCD9C \uC608\uCE21", layout="wide")
    apply_global_css()

    config = load_config()
    bundle = load_bundle(config)
    model_package = load_model(config)
    available_periods = get_available_periods(bundle)

    if not available_periods:
        st.error("\uC608\uCE21 \uAC00\uB2A5\uD55C \uBC18\uAE30 \uB370\uC774\uD130\uAC00 \uC5C6\uC2B5\uB2C8\uB2E4. `feature_krx` \uC6D0\uBCF8\uC744 \uD655\uC778\uD574 \uC8FC\uC138\uC694.")
        return

    latest_available_period = available_periods[0]
    latest_date = bundle.kospi_friday_daily["date"].max() if not bundle.kospi_friday_daily.empty else None
    sidebar_logo_uri = encode_image_to_data_uri(str(config.project_root / "assets" / "next200_logo_light.png"))

    left_col, center_col, right_col = st.columns([1.05, 2.75, 1.25], gap="large")

    with left_col:
        if sidebar_logo_uri:
            st.markdown(
                f'<div class="sidebar-brand"><img src="{sidebar_logo_uri}" alt="NEXT200 logo"></div>',
                unsafe_allow_html=True,
            )
        st.markdown(
            """
            <div class="panel-card">
                <div class="section-title">\uC608\uCE21 \uB300\uC2DC\uBCF4\uB4DC</div>
                <div class="section-sub">\uC2E4\uC81C \uC6B4\uC601 \uD654\uBA74 \uAE30\uC900</div>
                <p style="margin-top:12px; color:#9fb2ce; line-height:1.7;">
                    \uC120\uD0DD\uD55C \uBC18\uAE30\uC758 \uD3B8\uC785\u00B7\uD3B8\uCD9C \uD6C4\uBCF4\uB97C \uD655\uC778\uD558\uACE0, \uC885\uBAA9 \uCE74\uB4DC\uB97C \uB20C\uB7EC \uC0C1\uC138 \uBD84\uC11D \uD654\uBA74\uC5D0\uC11C
                    SHAP \uAE30\uC5EC\uB3C4, \uAD00\uB828 \uB274\uC2A4, \uC8FC\uAC00 \uD750\uB984\uC744 \uD568\uAED8 \uBCFC \uC218 \uC788\uC2B5\uB2C8\uB2E4.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown("<div style='height:14px;'></div>", unsafe_allow_html=True)
        selected_period = st.selectbox("\uC608\uCE21 \uAE30\uC900\uC77C", available_periods, index=0)
        email = st.text_input("\uC774\uBA54\uC77C", placeholder="\uC774\uBA54\uC77C \uC8FC\uC18C \uC785\uB825")
        if st.button("\uAD6C\uB3C5 \uC2E0\uCCAD", use_container_width=True):
            ok, message = save_subscriber_email(email)
            if ok:
                st.success(message)
            else:
                st.error(message)

        st.markdown(
            f"""
            <div class="panel-card" style="margin-top:16px;">
                <div class="section-title">\uC6B4\uC601 \uBA54\uBAA8</div>
                <div class="section-sub">\uCD5C\uC2E0 \uB370\uC774\uD130 \uAE30\uC900 \uC548\uB0B4</div>
                <p style="margin-top:12px; color:#9fb2ce; line-height:1.7;">
                    \uD604\uC7AC \uC571\uC740 \uC800\uC7A5\uB41C \uCD5C\uC2E0 \uB370\uC774\uD130 \uC911 \uAC00\uC7A5 \uCD5C\uADFC \uBC18\uAE30\uB97C \uAE30\uC900\uC73C\uB85C \uC608\uCE21\uD569\uB2C8\uB2E4.
                    \uC9C0\uAE08 \uC0AC\uC6A9 \uAC00\uB2A5\uD55C \uCD5C\uC2E0 \uBC18\uAE30\uB294 <strong>{latest_available_period}</strong> \uC785\uB2C8\uB2E4.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    context = build_period_context(bundle, selected_period)
    if context.frame.empty:
        st.error(f"{selected_period} \uBC18\uAE30\uC5D0 \uD574\uB2F9\uD558\uB294 \uD53C\uCC98 \uB370\uC774\uD130\uAC00 \uC5C6\uC2B5\uB2C8\uB2E4.")
        return

    model_package["ticker_to_name"].update(context.ticker_to_name)
    prediction_result = get_stored_prediction_result(bundle, context, model_package, selected_period)
    if prediction_result is None:
        prediction_result = run_prediction(
            context.frame,
            model_package,
            selected_period,
            period_end_date=context.period_end,
        )

    labels_current_members = set()
    labels_prev_members = set()
    if "is_member" in context.frame.columns:
        labels_current_members = set(
            context.frame.loc[pd.to_numeric(context.frame["is_member"], errors="coerce").fillna(0).astype(int) == 1, "ticker"]
            .astype(str)
        )
    if "was_member" in context.frame.columns:
        labels_prev_members = set(
            context.frame.loc[pd.to_numeric(context.frame["was_member"], errors="coerce").fillna(0).astype(int) == 1, "ticker"]
            .astype(str)
        )

    current_members = labels_current_members or get_actual_members(model_package, selected_period)
    prev_members = labels_prev_members or get_prev_members(model_package, selected_period)
    comparison = compare_with_actual(prediction_result, current_members, prev_members) if current_members else None

    scored = prediction_result["scored"]
    strong_in = prediction_result["strong_in"].copy()
    strong_out = prediction_result["strong_out"].copy()
    predicted_top200 = scored.loc[scored["pred_top200"] == 1].copy()
    actual_top200 = pd.DataFrame()
    actual_top200_label = "\u0032\u0030\u0032\u0036 \uD604\uC7AC actual"
    actual_top200_path = config.project_root / "data" / "incoming" / "manual" / "actual_kospi200_2026.csv"
    if actual_top200_path.exists():
        try:
            try:
                actual_top200 = pd.read_csv(actual_top200_path, encoding="utf-8-sig")
            except UnicodeDecodeError:
                actual_top200 = pd.read_csv(actual_top200_path, encoding="cp949")
            if "\uC885\uBAA9\uCF54\uB4DC" in actual_top200.columns and "ticker" not in actual_top200.columns:
                actual_top200 = actual_top200.rename(columns={"\uC885\uBAA9\uCF54\uB4DC": "ticker", "\uC885\uBAA9\uBA85": "company"})
            actual_top200 = normalize_ticker_columns(actual_top200)
            latest_sector_snapshot = (
                bundle.feature_krx.sort_values("period", key=lambda s: s.map(period_sort_key))
                .dropna(subset=["ticker"])
                .drop_duplicates(subset=["ticker"], keep="last")
            )
            actual_top200 = actual_top200.merge(
                latest_sector_snapshot[["ticker", "gics_sector"]],
                on="ticker",
                how="left",
            )
        except Exception:
            actual_top200 = pd.DataFrame()
    basis_date = context.period_end.strftime("%Y-%m-%d") if context.period_end is not None else "N/A"
    _, sidebar_live_basis = fetch_naver_market_sum_snapshot()
    realtime_basis_text = sidebar_live_basis or "확인 불가"
    future_basis_text = get_future_prediction_basis_text(config)
    csv_bytes = make_csv(prediction_result, bundle=bundle, period=selected_period)
    comparison_text = f"{comparison['top200_accuracy']:.1%}" if comparison else "\uBE44\uAD50 \uBD88\uAC00"
    actual_change_result = build_actual_change_result(bundle, context, selected_period, prediction_result)

    with left_col:
        st.markdown(
            f"""
            <div class="panel-card" style="margin-top:16px;">
                <div class="section-title">\uBA74\uCC45 \uBB38\uAD6C</div>
                <div class="section-sub">\uC11C\uBE44\uC2A4 \uC774\uC6A9 \uC804 \uBC18\uB4DC\uC2DC \uD655\uC778</div>
                <div class="analysis-item">
                    <strong>\uBCF8 \uD654\uBA74\uC740 \uD22C\uC790 \uAD8C\uC720\uAC00 \uC544\uB2CC \uC815\uBCF4 \uC81C\uACF5\uC6A9 \uC608\uC2DC \uD654\uBA74\uC785\uB2C8\uB2E4.</strong>
                    <p>\uC608\uCE21 \uACB0\uACFC, SHAP \uC124\uBA85, \uB274\uC2A4 \uC694\uC57D, \uC9C0\uC218 \uBC0F \uC885\uBAA9 \uC218\uCE58\uB294 \uBAA8\uB378\uACFC \uC678\uBD80 \uB370\uC774\uD130 \uC18C\uC2A4\uC5D0 \uB530\uB77C \uB2EC\uB77C\uC9C8 \uC218 \uC788\uC73C\uBA70 \uC2E4\uC81C \uD22C\uC790 \uD310\uB2E8\uC758 \uC720\uC77C\uD55C \uADFC\uAC70\uB85C \uC0AC\uC6A9\uD574\uC11C\uB294 \uC548 \uB429\uB2C8\uB2E4.</p>
                </div>
                <div class="analysis-item">
                    <strong>\uC2E4\uD589 \uAE30\uC900 \uC694\uC57D</strong>
                    <p>\uD6C4\uBCF4 \uC885\uBAA9 \uC218 {prediction_result['summary']['candidate_count']:,}\uAC1C, \uD3B8\uC785 \uD6C4\uBCF4 {len(strong_in)}\uAC1C, \uD3B8\uCD9C \uD6C4\uBCF4 {len(strong_out)}\uAC1C, \uC2E4\uC81C \uBE44\uAD50 \uAC00\uB2A5 \uBC18\uAE30 \uAE30\uC900 \uC77C\uCE58\uB3C4\uB294 {comparison_text}\uC785\uB2C8\uB2E4.</p>
                </div>
                <div class="analysis-item" style="margin-bottom:0;">
                    <strong>\uC6B4\uC601 \uC720\uC758 \uC0AC\uD56D</strong>
                    <p>\uBBF8\uB798 \uBC18\uAE30 \uC608\uCE21\uC740 \uCD5C\uC2E0 \uC218\uB3D9 \uB370\uC774\uD130\uC640 \uC790\uB3D9 \uC218\uC9D1 \uC2DC\uC138\uB97C \uAE30\uBC18\uC73C\uB85C \uACC4\uC0B0\uB418\uBBC0\uB85C, \uC2E4\uC81C \uBC1C\uD45C \uC804\uC5D0\uB294 \uBC18\uB4DC\uC2DC \uBCC4\uB3C4 \uAC80\uC99D\uC774 \uD544\uC694\uD569\uB2C8\uB2E4.</p>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown(
            f"""
            <div class="panel-card" style="margin-top:16px;">
                <div class="section-title">데이터 정보</div>
                <div class="section-sub">출처 및 기준일 안내</div>
                <div class="analysis-item">
                    <strong>반기 기준 종료일</strong>
                    <p>선택 반기: {selected_period}<br>{basis_date}</p>
                </div>
                <div class="analysis-item">
                    <strong>실시간 시세 업데이트 시각</strong>
                    <p>{realtime_basis_text}</p>
                </div>
                <div class="analysis-item">
                    <strong>미래 예측 데이터 기준일</strong>
                    <p>{future_basis_text}</p>
                </div>
                <div class="analysis-item">
                    <strong>데이터 출처</strong>
                    <p>과거 반기: SQL historical labels/predictions<br>미래 반기: Yahoo Finance, 네이버 증권, OpenDART<br>우측 실시간 패널: 네이버 증권, Yahoo Finance</p>
                </div>
                <div class="analysis-item" style="margin-bottom:0;">
                    <strong>비고</strong>
                    <p>미래 반기 예측은 최신 수집 데이터 기준으로 계산되며, 우측 패널 시세는 준실시간으로 갱신됩니다.</p>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with center_col:
        st.markdown(
            f"""
            <div class="hero-card topbar">
                  <div class="title-wrap">
                      <h2>KOSPI 200 \uD3B8\uC785\u00B7\uD3B8\uCD9C \uC608\uCE21</h2>
                      <p>\uD655\uC815\uB41C \uD654\uBA74 \uC694\uC18C\uB97C \uAE30\uC900\uC73C\uB85C \uAC00\uC7A5 \uAC00\uAE4C\uC6B4 \uBBF8\uB798 \uBC18\uAE30\uB97C \uC608\uCE21\uD569\uB2C8\uB2E4. \uC885\uBAA9 \uC0C1\uC138\uB294 \uC544\uB798 \uBC84\uD2BC\uC73C\uB85C \uD655\uC778\uD560 \uC218 \uC788\uC2B5\uB2C8\uB2E4.</p>
                  </div>
                  <div class="toolbar">
                      <span class="pill">\uBAA8\uB378 \uBC84\uC804 {prediction_result['summary']['model_version']}</span>
                  </div>
              </div>
            """,
            unsafe_allow_html=True,
        )

        current_view_tab, actual_compare_tab = st.tabs(["현재 화면 유지", "실제 결과 비교"])

        with current_view_tab:
            st.caption("아래에는 기존 예측 결과와 CSV 다운로드가 그대로 표시됩니다.")

        with actual_compare_tab:
            if actual_change_result is None:
                st.info("\uD574\uB2F9 \uBC18\uAE30\uC5D0\uB294 \uC2E4\uC81C \uD3B8\uC785/\uD3B8\uCD9C \uB370\uC774\uD130\uAC00 \uC544\uC9C1 \uC5C6\uC2B5\uB2C8\uB2E4.")
            else:
                actual_in = actual_change_result["actual_in"]
                actual_out = actual_change_result["actual_out"]
                actual_in_set = actual_change_result["actual_in_set"]
                actual_out_set = actual_change_result["actual_out_set"]
                predicted_in_set = set(strong_in["ticker"].astype(str))
                predicted_out_set = set(strong_out["ticker"].astype(str))

                metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4, gap="small")
                with metric_col1:
                    render_metric_card("\uC2E4\uC81C \uD3B8\uC785", f"{len(actual_in):,}\uAC1C", "\uD574\uB2F9 \uBC18\uAE30 \uAE30\uC900")
                with metric_col2:
                    render_metric_card("\uC2E4\uC81C \uD3B8\uCD9C", f"{len(actual_out):,}\uAC1C", "\uD574\uB2F9 \uBC18\uAE30 \uAE30\uC900")
                with metric_col3:
                    render_metric_card("\uD3B8\uC785 \uC77C\uCE58", f"{len(predicted_in_set & actual_in_set)}\uAC1C", "\uC608\uCE21 vs \uC2E4\uC81C")
                with metric_col4:
                    render_metric_card("\uD3B8\uCD9C \uC77C\uCE58", f"{len(predicted_out_set & actual_out_set)}\uAC1C", "\uC608\uCE21 vs \uC2E4\uC81C")

                actual_in_col, actual_out_col = st.columns(2, gap="large")
                with actual_in_col:
                    st.markdown(
                        """
                        <div class="section-card">
                            <div class="panel-head">
                                <div>
                                    <h3 style="margin:0; font-size:18px; letter-spacing:-0.02em;">실제 편입 종목</h3>
                                    <p style="margin:5px 0 0; font-size:12px; color:#89a0be;">해당 반기에 실제로 새롭게 편입된 종목</p>
                                </div>
                                <span class="tag green">실제 결과</span>
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                    if actual_in.empty:
                        st.info("\uD574\uB2F9 \uBC18\uAE30 \uAE30\uC900 \uC2E4\uC81C \uD3B8\uC785 \uC885\uBAA9\uC774 \uC5C6\uC2B5\uB2C8\uB2E4.")
                    else:
                        render_actual_stock_section(actual_in, "in")

                with actual_out_col:
                    st.markdown(
                        """
                        <div class="section-card">
                            <div class="panel-head">
                                <div>
                                    <h3 style="margin:0; font-size:18px; letter-spacing:-0.02em;">실제 편출 종목</h3>
                                    <p style="margin:5px 0 0; font-size:12px; color:#89a0be;">해당 반기에 실제로 편출된 종목</p>
                                </div>
                                <span class="tag red">실제 결과</span>
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                    if actual_out.empty:
                        st.info("\uD574\uB2F9 \uBC18\uAE30 \uAE30\uC900 \uC2E4\uC81C \uD3B8\uCD9C \uC885\uBAA9\uC774 \uC5C6\uC2B5\uB2C8\uB2E4.")
                    else:
                        render_actual_stock_section(actual_out, "out")

        st.markdown('<div class="csv-btn">', unsafe_allow_html=True)
        st.download_button(
            "CSV \uB2E4\uC6B4\uB85C\uB4DC",
            data=csv_bytes,
            file_name=f"kospi200_prediction_{selected_period}.csv",
            mime="text/csv",
            use_container_width=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)

        in_col, out_col = st.columns(2, gap="large")
        with in_col:
            st.markdown(
                """
                <div class="section-card">
                    <div class="panel-head">
                        <div>
                            <h3 style="margin:0; font-size:18px; letter-spacing:-0.02em;">편입 예측</h3>
                            <p style="margin:5px 0 0; font-size:12px; color:#89a0be;">강한 편입 후보 순서대로 정렬</p>
                        </div>
                        <span class="tag green">상위 후보</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if strong_in.empty:
                st.warning("\uD604\uC7AC \uB370\uC774\uD130 \uAE30\uC900\uC73C\uB85C \uAC15\uD55C \uD3B8\uC785 \uD6C4\uBCF4\uAC00 \uC5C6\uC2B5\uB2C8\uB2E4.")
            else:
                render_stock_section(strong_in, "in")

        with out_col:
            st.markdown(
                """
                <div class="section-card">
                    <div class="panel-head">
                        <div>
                            <h3 style="margin:0; font-size:18px; letter-spacing:-0.02em;">편출 예측</h3>
                            <p style="margin:5px 0 0; font-size:12px; color:#89a0be;">강한 편출 후보 순서대로 정렬</p>
                        </div>
                        <span class="tag red">주의 필요</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if strong_out.empty:
                st.warning("\uD604\uC7AC \uB370\uC774\uD130 \uAE30\uC900\uC73C\uB85C \uAC15\uD55C \uD3B8\uCD9C \uD6C4\uBCF4\uAC00 \uC5C6\uC2B5\uB2C8\uB2E4.")
            else:
                render_stock_section(strong_out, "out")

        lower_left, lower_right = st.columns(2, gap="large")
        with lower_left:
            st.markdown(
                """
                <div class="section-card">
                    <div class="panel-head">
                        <div>
                            <h3 style="margin:0; font-size:18px; letter-spacing:-0.02em;">예측 KOSPI200 섹터 비율</h3>
                            <p style="margin:5px 0 0; font-size:12px; color:#89a0be;">예측 TOP200 기준 분포</p>
                        </div>
                        <span class="tag blue">구성 현황</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if predicted_top200.empty:
                st.info("TOP200 \uC608\uCE21 \uACB0\uACFC\uAC00 \uBE44\uC5B4 \uC788\uC2B5\uB2C8\uB2E4.")
            else:
                sector_series = predicted_top200["gics_sector"].fillna("\uAE30\uD0C0").value_counts().reset_index()
                sector_series.columns = ["sector", "count"]
                fig = px.pie(sector_series, names="sector", values="count", hole=0.58)
                fig.update_layout(
                    margin=dict(l=10, r=10, t=10, b=10),
                    height=360,
                    paper_bgcolor="rgba(0,0,0,0)",
                    font_color="#dbe7f8",
                    legend_title_text="\uC139\uD130",
                )
                st.plotly_chart(fig, use_container_width=True)

        with lower_right:
            st.markdown(
                f"""
                <div class="section-card">
                    <div class="panel-head">
                        <div>
                            <h3 style="margin:0; font-size:18px; letter-spacing:-0.02em;">실제 KOSPI200 섹터 비율</h3>
                            <p style="margin:5px 0 0; font-size:12px; color:#89a0be;">{actual_top200_label} 구성종목 기준</p>
                        </div>
                        <span class="tag blue">실제 구성</span>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if actual_top200.empty:
                st.info("현재 actual KOSPI200 구성종목 CSV를 찾을 수 없습니다.")
            else:
                actual_sector_series = actual_top200["gics_sector"].fillna("기타").value_counts().reset_index()
                actual_sector_series.columns = ["sector", "count"]
                actual_fig = px.pie(actual_sector_series, names="sector", values="count", hole=0.58)
                actual_fig.update_layout(
                    margin=dict(l=10, r=10, t=10, b=10),
                    height=360,
                    paper_bgcolor="rgba(0,0,0,0)",
                    font_color="#dbe7f8",
                    legend_title_text="섹터",
                )
                st.plotly_chart(actual_fig, use_container_width=True)


    with right_col:
        render_live_sidebar(config, latest_date)

    detail_ticker = st.session_state.get("detail_ticker")
    if detail_ticker:
        selected = scored.loc[scored["ticker"].astype(str) == str(detail_ticker)]
        if not selected.empty:
            render_stock_modal(bundle, selected.iloc[0], model_package, config)
        st.session_state.pop("detail_ticker", None)


if __name__ == "__main__":
    main()
