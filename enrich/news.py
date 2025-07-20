"""News enrichment using NewsAPI.

Set environment variable:
    NEWSAPI_KEY=<your key>

Adds columns:
- headlines: list of {title, source, url, publishedAt}
- headline_count
- headline_first_source
- news_status (ok / missing_key / error)
"""
from __future__ import annotations
import os, time, hashlib
from datetime import timedelta
from typing import Dict, Any, List, Tuple
import requests
import pandas as pd

NEWS_ENDPOINT = "https://newsapi.org/v2/everything"
API_KEY_ENV = "NEWSAPI_KEY"
CACHE_TTL = 3600  # seconds
_cache: Dict[str, Dict[str, Any]] = {}

def _ck(query: str, from_date: str, to_date: str) -> str:
    return hashlib.md5(f"{query}|{from_date}|{to_date}".encode()).hexdigest()

def _news_request(query: str, from_date: str, to_date: str, page_size: int = 20) -> Dict[str, Any]:
    api_key = os.getenv(API_KEY_ENV)
    if not api_key:
        return {"status": "missing_key", "articles": []}
    key = _ck(query, from_date, to_date)
    now = time.time()
    cached = _cache.get(key)
    if cached and now - cached["ts"] < CACHE_TTL:
        return cached["data"]
    params = {
        "q": query,
        "from": from_date,
        "to": to_date,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": page_size,
        "apiKey": api_key,
    }
    try:
        resp = requests.get(NEWS_ENDPOINT, params=params, timeout=15)
        data = resp.json()
    except Exception as e:
        data = {"status": "error", "error": str(e), "articles": []}
    _cache[key] = {"ts": now, "data": data}
    return data

def enrich_news(df: pd.DataFrame, window_days: int = 2, max_articles_per_trade: int = 5) -> pd.DataFrame:
    if df.empty or "trade_date" not in df.columns:
        return df
    df = df.copy()
    headlines_list: List[List[Dict[str, Any]]] = []
    counts: List[int] = []
    first_sources: List[str] = []
    statuses: List[str] = []

    # Deduplicate queries (ticker or stock_clean + date window)
    query_cache: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    for _, row in df.iterrows():
        td = row.get("trade_date")
        if pd.isna(td):
            headlines_list.append([])
            counts.append(0)
            first_sources.append("")
            statuses.append("no_date")
            continue
        from_d = (td - timedelta(days=window_days)).date().isoformat()
        to_d = (td + timedelta(days=window_days)).date().isoformat()
        token = row.get("ticker") or row.get("stock_clean")
        if not token:
            headlines_list.append([])
            counts.append(0)
            first_sources.append("")
            statuses.append("no_query")
            continue
        query = f'"{token}"'
        key = (query, from_d, to_d)
        if key not in query_cache:
            query_cache[key] = _news_request(query, from_d, to_d)
        result = query_cache[key]
        arts = result.get("articles", [])[:max_articles_per_trade]
        simplified = [
            {
                "title": a.get("title"),
                "source": a.get("source", {}).get("name"),
                "url": a.get("url"),
                "publishedAt": a.get("publishedAt"),
            } for a in arts
        ]
        headlines_list.append(simplified)
        counts.append(len(simplified))
        first_sources.append(simplified[0]["source"] if simplified else "")
        statuses.append(result.get("status", "unknown"))

    df["headlines"] = headlines_list
    df["headline_count"] = counts
    df["headline_first_source"] = first_sources
    df["news_status"] = statuses
    return df
