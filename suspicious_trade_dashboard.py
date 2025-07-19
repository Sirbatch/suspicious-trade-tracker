
import streamlit as st
import pandas as pd

# Load Excel data
df = pd.read_excel("Suspicious_Trade_Tracker_Report.xlsx")

st.title("Suspicious Congressional Trade Tracker")

# Filters
sectors = df["Sector"].dropna().unique()
selected_sector = st.selectbox("Filter by Sector", ["All"] + sorted(sectors.tolist()))
if selected_sector != "All":
    df = df[df["Sector"] == selected_sector]

min_score = st.slider("Minimum Suspicious Score", 0.0, 1.0, 0.5)
df = df[df["Suspicious Score"] >= min_score]

# Show table
st.dataframe(df.sort_values("Suspicious Score", ascending=False), use_container_width=True)

# Download link
st.download_button(
    label="Download Filtered Data",
    data=df.to_csv(index=False).encode("utf-8"),
    file_name="filtered_trades.csv",
    mime="text/csv"
)
