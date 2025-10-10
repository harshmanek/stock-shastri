# scripts/prepare_features.py

import os
import sys
import mysql.connector
import pandas as pd

# Ensure project root is in sys.path for config import
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config import DATABASE_CONFIG  # NEWS_CSV_PATH and STOCK_SYMBOLS not needed here

# 1. Read all data from database
conn = mysql.connector.connect(**DATABASE_CONFIG)

stocks = pd.read_sql(
    "SELECT date, ticker, close_price FROM stocks", 
    conn, parse_dates=['date']
)
sentiment = pd.read_sql(
    "SELECT Date AS date, Ticker AS ticker, SentimentScore AS sentiment_score FROM sentiment_data", 
    conn, parse_dates=['date']
)
macro = pd.read_sql(
    "SELECT date, usd_inr_rate, interest_rate, unemployment_rate FROM macro_indicators", 
    conn, parse_dates=['date']
)
events = pd.read_sql(
    "SELECT event_type, impact_window_start, impact_window_end FROM market_events", 
    conn, parse_dates=['impact_window_start','impact_window_end']
)
conn.close()

# 2. Merge on ticker and date
df = stocks.merge(sentiment[['date', 'sentiment_score']], on='date', how='left')
df = df.merge(macro, on='date', how='left')

# 3. Encode event flags
for evt in events['event_type'].unique():
    flag_col = f"{evt.lower()}_flag"
    df[flag_col] = 0
    evt_dates = events[events['event_type']==evt]
    for _, row in evt_dates.iterrows():
        mask = (df['date']>=row['impact_window_start']) & (df['date']<=row['impact_window_end'])
        df.loc[mask, flag_col] = 1

# 4. Add lagged returns and target
df['return_1'] = df.groupby('ticker')['close_price'].pct_change(1)
df['return_direction'] = (df['return_1'].shift(-1) > 0).astype(int)

# 5. Fill missing values
# Neutral sentiment for missing
df['sentiment_score'] = df['sentiment_score'].fillna(0.0)
# Forward/backward fill macro indicators
df[['usd_inr_rate','interest_rate','unemployment_rate']] = df[
    ['usd_inr_rate','interest_rate','unemployment_rate']
].ffill().bfill()

# Drop only rows missing the target
df = df.dropna(subset=['return_direction']).reset_index(drop=True)

# Diagnostic output
print("Merged shape after cleaning:", df.shape)
print(df.isna().sum())

# 6. Save for modeling
os.makedirs('data', exist_ok=True)
output_path = os.path.join('data','features.csv')
df.to_csv(output_path, index=False)
print(f"✅ Merged feature file saved at {output_path}")
