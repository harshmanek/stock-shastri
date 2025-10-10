# scripts/update_recent_sentiment.py

import os
import sys
import re
import pandas as pd
import mysql.connector
from datetime import datetime
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk

nltk.download('vader_lexicon', quiet=True)

# Ensure project root for config import
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config import DATABASE_CONFIG

# Parameters
INPUT_FILE = 'data/finsen_processed.csv'
FILTERED_FILE = 'data/finsen_recent.csv'
DATE_FORMAT = '%Y-%m-%d'
START_DATE = '2019-01-01'
END_DATE   = '2023-01-13'

def extract_date(text):
    m = re.search(r'(\d{4}-\d{2}-\d{2})', str(text))
    return m.group(1) if m else None

def filter_finsen():
    df = pd.read_csv(INPUT_FILE)
    # Extract date from the end of the Content or from the last token
    df['pub_date'] = df['Content'].apply(extract_date)
    df['pub_date'] = pd.to_datetime(df['pub_date'], format=DATE_FORMAT, errors='coerce')
    mask = (df['pub_date'] >= START_DATE) & (df['pub_date'] <= END_DATE)
    recent = df.loc[mask].copy()
    recent.to_csv(FILTERED_FILE, index=False)
    print(f"Filtered {len(recent)} rows from {INPUT_FILE} into {FILTERED_FILE}")
    return recent

def update_sentiment_table(df):
    # Connect to DB
    conn = mysql.connector.connect(**DATABASE_CONFIG)
    cur = conn.cursor()
    # Clear old entries in date range
    cur.execute(f"""
        DELETE FROM sentiment_data
        WHERE Date BETWEEN %s AND %s;
    """, (START_DATE, END_DATE))
    conn.commit()
    analyzer = SentimentIntensityAnalyzer()
    # Insert new sentiment records
    for _, row in df.iterrows():
        date = row['pub_date'].date()
        title = row['Title']
        score = analyzer.polarity_scores(str(title))['compound']
        # Assign ticker GENERAL_FINANCIAL as placeholder
        cur.execute("""
            INSERT INTO sentiment_data (Date, Ticker, SentimentScore, Headline)
            VALUES (%s,%s,%s,%s);
        """, (date, 'GENERAL_FINANCIAL', score, title))
    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {len(df)} new sentiment records into database")

if __name__ == '__main__':
    recent_df = filter_finsen()
    update_sentiment_table(recent_df)
