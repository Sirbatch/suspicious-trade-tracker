"""Ticker normalization stub.

Goal (later):
- Take the cleaned company/stock name (stock_clean) and map it to an exchange ticker.
- Use a cached lookup (yfinance, OpenFIGI, or your own symbol master) so we avoid repeated API hits.

For now this just creates the columns so downstream code doesn't break.
"""

from __future__ import annotations
import pandas as pd
from functools import lru_cache

@lru_cache(maxsize=4096)
def _lookup_ticker(clean_name: str):
    """
    Placeholder lookup.
    TODO: implement:
      1. Exact match against a preloaded symbol table
      2. Fuzzy fallback (rapidfuzz) for close matches
      3. Cache result
    Return None if not found.
    """
    return None

def normalize_tickers(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if 'stock_clean' not in df.columns:
        return df
    df = df.copy()
    df['ticker'] = df['stock_clean'].apply(lambda n: _lookup_ticker(n) or None)
    df['ticker_missing'] = df['ticker'].isna()
    return df
