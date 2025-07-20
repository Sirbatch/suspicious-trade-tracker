import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt  # <-- NEW

from ingest.quiver import fetch_quiver_trades, QuiverIngestionError
from scoring.model import compute_scores
from enrich.tickers import normalize_tickers
from enrich.news import enrich_news  # NewsAPI enrichment

st.set_page_config(page_title="Congressional Suspicious Trade Tracker", layout="wide")
st.title("🕵️ Congressional Suspicious Trade Tracker")
st.caption("Heuristic scoring of public congressional trade disclosures. Not an allegation of wrongdoing.")

@st.cache_data(ttl=600)
def load_raw_trades() -> pd.DataFrame:
    try:
        return fetch_quiver_trades()
    except QuiverIngestionError as e:
        st.error(f"Ingestion error: {e}")
        return pd.DataFrame()

with st.spinner("Fetching latest trades..."):
    df_raw = load_raw_trades()

if df_raw.empty:
    st.warning("No trade data parsed. Site layout may have changed or no recent trades.")
    st.stop()

# --------------------------------------------------------------------- enrichment
with st.spinner("Normalizing tickers..."):
    df_enriched = normalize_tickers(df_raw)

with st.spinner("Fetching related news (if NEWSAPI_KEY set)..."):
    df_enriched = enrich_news(df_enriched)

df_scored = compute_scores(df_enriched)

# --------------------------------------------------------------------- sidebar filters
st.sidebar.header("Filters")
sectors = ["All"] + sorted([s for s in df_scored['Sector'].dropna().unique()])
sector_choice = st.sidebar.selectbox("Sector", sectors, index=0)

score_min = float(df_scored['score'].min())
score_max = float(df_scored['score'].max())
score_range = st.sidebar.slider("Score Range", min_value=0.0, max_value=100.0,
                                value=(score_min, score_max), step=1.0)

if 'trade_date' in df_scored.columns and not df_scored['trade_date'].isna().all():
    min_date = pd.to_datetime(df_scored['trade_date'].min()).date()
    max_date = pd.to_datetime(df_scored['trade_date'].max()).date()
    date_range = st.sidebar.date_input("Trade Date Range", value=(min_date, max_date))
else:
    date_range = None

# --------------------------------------------------------------------- apply filters
filtered = df_scored.copy()
if sector_choice != "All":
    filtered = filtered[filtered['Sector'] == sector_choice]

filtered = filtered[(filtered['score'] >= score_range[0]) & (filtered['score'] <= score_range[1])]

if date_range and len(date_range) == 2:
    start_d, end_d = date_range
    mask = (filtered['trade_date'].dt.date >= start_d) & (filtered['trade_date'].dt.date <= end_d)
    filtered = filtered[mask]

# --------------------------------------------------------------------- KPI metrics
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Trades", f"{len(df_scored):,}")
col2.metric("Filtered Trades", f"{len(filtered):,}")
col3.metric("Unique Politicians", filtered['Politician'].nunique())
col4.metric("Avg Score (Filtered)", f"{filtered['score'].mean():.1f}" if len(filtered) else '-')

# --------------------------------------------------------------------- charts
if not filtered.empty:
    counts = filtered['Politician'].value_counts().head(20).sort_values(ascending=True)
    st.subheader("Top Politicians by Trade Count (Filtered)")
    st.bar_chart(counts)

    st.subheader("Score Distribution (Filtered)")
    st.caption("Histogram of suspicious scores (filtered set).")

    # ---------- NEW histogram with matplotlib
    fig, ax = plt.subplots()
    ax.hist(filtered["score"], bins=20, edgecolor="black")
    ax.set_xlabel("Suspicious Score")
    ax.set_ylabel("Trades")
    st.pyplot(fig)
else:
    st.info("No trades match current filters.")

# --------------------------------------------------------------------- data table
st.subheader("Trades")
show_cols = [c for c in [
    'scrape_ts', 'Politician', 'ticker', 'stock_clean', 'Sector', 'Trade Type', 'Trade Date',
    'Amount', 'amount_low', 'amount_high', 'amount_mid',
    'headline_count', 'headline_first_source', 'news_status',
    'score', 'score_amount', 'score_sector_volatility', 'score_pattern',
    'score_news_intensity', 'score_event_proximity'
] if c in filtered.columns]

st.dataframe(filtered[show_cols].sort_values('score', ascending=False), use_container_width=True)

# --------------------------------------------------------------------- detail panel
if not filtered.empty:
    st.markdown("### Trade Detail")
    selected_idx = st.selectbox("Select a trade for detail", filtered.index.tolist())
    row = filtered.loc[selected_idx]
    with st.expander("Score Breakdown", expanded=True):
        st.write({
            'score': row['score'],
            'amount_component': row.get('score_amount'),
            'sector_vol_component': row.get('score_sector_volatility'),
            'pattern_component': row.get('score_pattern'),
            'news_intensity_component': row.get('score_news_intensity'),
            'event_proximity_component': row.get('score_event_proximity'),
        })
    with st.expander("Headlines"):
        heads = row.get('headlines', [])
        if heads:
            for h in heads:
                st.markdown(f"- [{h.get('title')}]({h.get('url')}) ({h.get('source')})")
        else:
            st.caption("No headlines (missing key, none found, or no matching articles).")

# --------------------------------------------------------------------- export
if not filtered.empty:
    st.download_button(
        "Download Filtered Trades (CSV)",
        filtered.to_csv(index=False).encode('utf-8'),
        file_name="filtered_trades.csv",
        mime="text/csv"
    )

# --------------------------------------------------------------------- debug
with st.expander("Debug / Metadata"):
    st.write({
        'header_hash_unique': df_raw['header_hash'].unique().tolist(),
        'parse_warnings_unique': df_raw['parse_warnings'].unique().tolist(),
        'data_age_seconds': (pd.Timestamp.utcnow() - pd.to_datetime(df_raw['scrape_ts']).max()).total_seconds(),
        'rows_raw': len(df_raw),
        'rows_filtered': len(filtered),
        'news_status_counts': filtered.get('news_status', pd.Series(dtype=str)).value_counts().to_dict()
    })

st.caption("Set NEWSAPI_KEY environment variable before running to activate news enrichment.")
