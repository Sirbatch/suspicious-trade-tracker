
"""QuiverQuant congressional trade ingestion (fetch -> parse -> normalize).
No scoring or external enrichment here.
"""
from __future__ import annotations
import re
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Dict, Any

import requests
import pandas as pd
from bs4 import BeautifulSoup

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " \
             "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
SOURCE_URL = "https://www.quiverquant.com/congresstrading"

EXPECTED_HEADERS = {"Politician", "Stock", "Trade Type", "Trade Date", "Amount", "Sector"}
AMOUNT_PATTERN = re.compile(r"\$?([\d,.]+)\s*([KkMm]?)(?:\s*-\s*\$?([\d,.]+)\s*([KkMm]?))?")

@dataclass
class ParseResult:
    records: List[Dict[str, Any]]
    header_hash: str
    warnings: List[str]


class QuiverIngestionError(Exception):
    pass


def fetch_html(url: str = SOURCE_URL, timeout: int = 20) -> str:
    """Fetch HTML with basic error handling."""
    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}
    resp = requests.get(url, headers=headers, timeout=timeout)
    if resp.status_code != 200:
        raise QuiverIngestionError(f"Non-200 status {resp.status_code} for {url}")
    if not resp.text:
        raise QuiverIngestionError("Empty response body")
    return resp.text


def _standardize_header(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip())


def parse_table(html: str) -> ParseResult:
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        raise QuiverIngestionError("No tables found in HTML.")

    candidate = None
    header_hash = None
    warnings: List[str] = []

    for tbl in tables:
        header_row = tbl.find("tr")
        if not header_row:
            continue
        headers = [_standardize_header(th.get_text()) for th in header_row.find_all(["th", "td"])]
        header_set = set(h for h in headers if h)
        if EXPECTED_HEADERS.issubset(header_set):
            header_hash = hashlib.md5("||".join(headers).encode()).hexdigest()
            candidate = (tbl, headers)
            break

    if candidate is None:
        raise QuiverIngestionError("No table contained expected headers.")

    tbl, headers = candidate
    rows = tbl.find_all("tr")[1:]  # skip header
    records: List[Dict[str, Any]] = []

    for r in rows:
        cols = r.find_all("td")
        if len(cols) < 6:
            continue
        values = [c.get_text(strip=True) for c in cols[:6]]
        mapping = dict(zip(headers[:6], values))
        if not EXPECTED_HEADERS.issubset(mapping.keys()):
            warnings.append(f"Row missing expected columns: {mapping}")
            continue
        records.append({
            "Politician": mapping.get("Politician"),
            "Stock": mapping.get("Stock"),
            "Trade Type": mapping.get("Trade Type"),
            "Trade Date": mapping.get("Trade Date"),
            "Amount": mapping.get("Amount"),
            "Sector": mapping.get("Sector"),
        })

    return ParseResult(records=records, header_hash=header_hash or "", warnings=warnings)


def _parse_amount_bracket(bracket: str) -> Dict[str, Any]:
    """Parse an amount bracket like '$15K - $50K' -> low/high/mid in dollars."""
    if not bracket:
        return {"amount_low": None, "amount_high": None, "amount_mid": None}
    m = AMOUNT_PATTERN.search(bracket)
    if not m:
        return {"amount_low": None, "amount_high": None, "amount_mid": None}

    def to_number(num_str: str, suffix: str) -> float:
        if num_str is None:
            return None
        base = float(num_str.replace(",", ""))
        if suffix.lower() == 'k':
            base *= 1_000
        elif suffix.lower() == 'm':
            base *= 1_000_000
        return base

    low = to_number(m.group(1), m.group(2) or "")
    high_raw = m.group(3)
    high = to_number(high_raw, m.group(4) or "") if high_raw else low
    if low is None:
        return {"amount_low": None, "amount_high": None, "amount_mid": None}
    mid = (low + high) / 2 if (low is not None and high is not None) else low
    return {"amount_low": low, "amount_high": high, "amount_mid": mid}


CORP_SUFFIXES = re.compile(r"\b(Inc|Incorporated|Corp|Corporation|Co|Company|Class [A-Z]|Common Stock|Ord Shs|PLC|Ltd)\.?$", re.IGNORECASE)

def clean_stock_name(raw: str) -> str:
    if not raw:
        return raw
    cleaned = raw.replace("–", "-")
    parts = [p.strip() for p in cleaned.split("-")]
    primary = parts[0]
    prev = None
    while prev != primary:
        prev = primary
        primary = CORP_SUFFIXES.sub("", primary).strip()
    return re.sub(r"\s+", " ", primary)


def normalize_records(pr: ParseResult, fetched_at: datetime) -> pd.DataFrame:
    df = pd.DataFrame(pr.records)
    if df.empty:
        return df.assign(scrape_ts=fetched_at, source_url=SOURCE_URL, header_hash=pr.header_hash, parse_warnings=["No rows parsed"])

    df["trade_date"] = pd.to_datetime(df["Trade Date"], errors="coerce")
    amt_parsed = df["Amount"].apply(_parse_amount_bracket).apply(pd.Series)
    df = pd.concat([df, amt_parsed], axis=1)
    df["raw_stock"] = df["Stock"]
    df["stock_clean"] = df["raw_stock"].apply(clean_stock_name)
    df["ticker"] = None  # Enrichment placeholder
    df["scrape_ts"] = fetched_at
    df["source_url"] = SOURCE_URL
    df["header_hash"] = pr.header_hash
    df["parse_warnings"] = "|".join(pr.warnings) if pr.warnings else ""
    preferred = [
        "Politician", "ticker", "stock_clean", "raw_stock", "Sector", "Trade Type", "Trade Date", "trade_date",
        "Amount", "amount_low", "amount_high", "amount_mid",
        "scrape_ts", "source_url", "header_hash", "parse_warnings"
    ]
    existing = [c for c in preferred if c in df.columns]
    return df[existing]


def fetch_quiver_trades() -> pd.DataFrame:
    fetched_at = datetime.now(timezone.utc)
    html = fetch_html()
    pr = parse_table(html)
    df = normalize_records(pr, fetched_at)
    return df

if __name__ == "__main__":
    trades = fetch_quiver_trades()
    print(trades.head())
    print(f"Rows: {len(trades)}  Warnings: {trades['parse_warnings'].unique()}")
