"""Suspicious scoring model (deterministic, modular)."""
from __future__ import annotations
import math
import pandas as pd

# Weight configuration (adjust as components mature)
WEIGHTS = {
    "amount": 0.45,
    "sector_volatility": 0.15,
    "news_intensity": 0.15,
    "event_proximity": 0.15,
    "pattern": 0.10,
}

# Placeholder sector volatility (0..1). Replace with real metrics later.
SECTOR_VOL = {
    "Technology": 0.85,
    "Energy": 0.90,
    "Healthcare": 0.70,
    "Financial": 0.65,
    "Industrials": 0.60,
    "Consumer Discretionary": 0.75,
    "Utilities": 0.30,
}

def _norm(x, min_x, max_x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return 0.0
    rng = max_x - min_x
    if rng <= 0:
        return 0.0
    return (x - min_x) / rng

def score_amount(df: pd.DataFrame) -> pd.Series:
    amt = df["amount_mid"].fillna(df["amount_low"]).fillna(df["amount_high"])
    min_a, max_a = amt.min(), amt.max()
    return amt.apply(lambda v: _norm(v, min_a, max_a))

def score_sector_volatility(df: pd.DataFrame) -> pd.Series:
    return df["Sector"].map(SECTOR_VOL).fillna(0.5)

def score_news_intensity(df: pd.DataFrame) -> pd.Series:
    if "headline_count" in df.columns:
        hc = df["headline_count"].fillna(0)
        min_h, max_h = hc.min(), hc.max()
        return hc.apply(lambda v: _norm(v, min_h, max_h))
    return pd.Series([0.0]*len(df), index=df.index)

def score_event_proximity(df: pd.DataFrame) -> pd.Series:
    if "days_to_event" in df.columns:
        d = df["days_to_event"].abs()
        max_d = d.max() if len(d) else 1
        return d.apply(lambda v: 1 - min(v, max_d)/max_d)
    if "event_flag" in df.columns:
        return df["event_flag"].fillna(0).astype(float)
    return pd.Series([0.0]*len(df), index=df.index)

def score_pattern(df: pd.DataFrame) -> pd.Series:
    if "stock_clean" not in df.columns:
        return pd.Series([0.0]*len(df), index=df.index)
    combo_counts = df.groupby(["Politician","stock_clean"])["stock_clean"].transform("count")
    max_c = combo_counts.max() or 1
    return combo_counts.apply(lambda c: (c-1)/(max_c-1) if max_c > 1 else 0.0)

def compute_scores(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.assign(score=[])
    components = {
        "score_amount": score_amount(df),
        "score_sector_volatility": score_sector_volatility(df),
        "score_news_intensity": score_news_intensity(df),
        "score_event_proximity": score_event_proximity(df),
        "score_pattern": score_pattern(df),
    }
    comp_df = pd.DataFrame(components)
    weighted = (
        comp_df["score_amount"] * WEIGHTS["amount"] +
        comp_df["score_sector_volatility"] * WEIGHTS["sector_volatility"] +
        comp_df["score_news_intensity"] * WEIGHTS["news_intensity"] +
        comp_df["score_event_proximity"] * WEIGHTS["event_proximity"] +
        comp_df["score_pattern"] * WEIGHTS["pattern"]
    )
    raw = weighted.fillna(0)
    score_norm = (raw - raw.min()) / (raw.max() - raw.min() or 1)
    df = df.copy()
    for c in comp_df.columns:
        df[c] = comp_df[c]
    df["score_raw"] = raw
    df["score"] = (score_norm * 100).round(2)
    return df
