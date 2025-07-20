import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import yfinance as yf

def fetch_quiver_trades():
    url = "https://www.quiverquant.com/congresstrading"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    trades = []
    table = soup.find("table")
    if not table:
        return pd.DataFrame()

    rows = table.find_all("tr")[1:]
    for row in rows:
        cols = row.find_all("td")
        if len(cols) >= 6:
            trade = {
                "Politician": cols[0].get_text(strip=True),
                "Stock": cols[1].get_text(strip=True),
                "Trade Type": cols[2].get_text(strip=True),
                "Trade Date": cols[3].get_text(strip=True),
                "Amount": cols[4].get_text(strip=True),
                "Sector": cols[5].get_text(strip=True),
            }
            trades.append(trade)

    df = pd.DataFrame(trades)

    def resolve_ticker(name):
        try:
            ticker = yf.Ticker(name.split()[0]).info.get("symbol")
            return ticker if ticker else name
        except:
            return name

    df["Resolved Ticker"] = df["Stock"].apply(resolve_ticker)

    def compute_suspicious_score(row):
        score = 0.0
        if "$15,000" in row["Amount"]:
            score += 0.3
        elif "$50,000" in row["Amount"]:
            score += 0.5
        elif "$100,000" in row["Amount"]:
            score += 0.7

        volatile_sectors = ["Technology", "Healthcare", "Energy"]
        if row["Sector"] in volatile_sectors:
            score += 0.2

        return min(score, 1.0)

    df["Suspicious Score"] = df.apply(compute_suspicious_score, axis=1)
    return df
