import pandas as pd
df = pd.read_csv("data/financial_news.csv", parse_dates=["Date"])
print("Columns:", df.columns.tolist())
print("Sample rows:\n", df.head(5))
print("\nHeadlines per ticker:")
for t in ["TCS","HDFCBANK","BAJFINANCE","ASIANPAINT","LEMONTREE","VBL"]:
    c = (df["Ticker"]==t).sum()
    print(f"{t}: {c} rows")

