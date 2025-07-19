import requests
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np

def fetch_quiver_trades():
    url = "https://www.quiverquant.com/congresstrading"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    trades = []
    table = soup.find("table")
    if not table:
        return pd.DataFrame()

    rows = table.find_all("tr")[1:]  # skip header
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
    df["Suspicious Score"] = np.random.uniform(0.0, 1.0, size=len(df))
    return df

