
import streamlit as st
import pandas as pd
import plotly.express as px
import urllib.parse
from quiver_scraper import fetch_quiver_trades
df = fetch_quiver_trades()
df["Trade Date"] = pd.to_datetime(df["Trade Date"], errors="coerce")

st.title("Suspicious Congressional Trade Tracker")

# --- Filters ---
sectors = df["Sector"].dropna().unique()
selected_sector = st.selectbox("Filter by Sector", ["All"] + sorted(sectors.tolist()))
if selected_sector != "All":
    df = df[df["Sector"] == selected_sector]

min_score = st.slider("Minimum Suspicious Score", 0.0, 1.0, 0.5)
df = df[df["Suspicious Score"] >= min_score]

# --- Charts ---
st.subheader("Trade Volume by Politician")
trade_counts = df["Politician"].value_counts().reset_index()
trade_counts.columns = ["Politician", "Trade Count"]
st.plotly_chart(px.bar(trade_counts, x="Politician", y="Trade Count", title="Number of Trades per Politician"))

st.subheader("Suspicious Score Distribution")
st.plotly_chart(px.histogram(df, x="Suspicious Score", nbins=10, title="Distribution of Suspicious Scores"))

# --- Data Table ---
st.subheader("Trade Data")

# Add clickable news links for each row
def create_news_link(row):
    base = "https://www.google.com/search?q="
    query = f"{row['Politician']} {row['Stock']} news {row['Trade Date']}"
    return f"[🔎 News]({base + urllib.parse.quote_plus(query)})"

df["News"] = df.apply(create_news_link, axis=1)
display_df = df[["Politician", "Stock", "Trade Type", "Trade Date", "Amount", "Sector", "Suspicious Score", "News"]]

st.dataframe(display_df, use_container_width=True)

# --- Download Button ---
st.download_button(
    label="Download Filtered Data",
    data=display_df.to_csv(index=False).encode("utf-8"),
    file_name="filtered_trades.csv",
    mime="text/csv"
)
