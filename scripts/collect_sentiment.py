# scripts/collect_sentiment.py

import os
import sys
import pandas as pd
import mysql.connector
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk

# Download VADER lexicon
nltk.download('vader_lexicon', quiet=True)

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config import STOCK_SYMBOLS, DATABASE_CONFIG, NEWS_CSV_PATH

class SentimentCollectorFree:
    def __init__(self):
        self.db = mysql.connector.connect(**DATABASE_CONFIG)
        self.cur = self.db.cursor()
        self.table_name = 'sentiment_data'
        self.analyzer = SentimentIntensityAnalyzer()

    def setup_table(self):
        # Drop old table then create fresh with correct schema
        self.cur.execute(f"DROP TABLE IF EXISTS {self.table_name};")
        create_sql = f"""
        CREATE TABLE {self.table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            Date DATE,
            Ticker VARCHAR(50),
            SentimentScore FLOAT,
            Headline TEXT
        ) ENGINE=InnoDB;
        """
        self.cur.execute(create_sql)
        self.db.commit()

    def truncate_table(self):
        # Clear any records (after drop+create this is optional)
        self.cur.execute(f"TRUNCATE TABLE {self.table_name};")
        self.db.commit()

    def collect(self):
        # Load and parse dates
        df = pd.read_csv(NEWS_CSV_PATH)
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df.dropna(subset=['Date'])
        
        for ticker in STOCK_SYMBOLS:
            subset = df[df['Ticker'] == ticker]
            for _, row in subset.iterrows():
                date = row['Date'].date()
                title = str(row['Title'])
                score = self.analyzer.polarity_scores(title)['compound']
                
                insert_sql = f"""
                INSERT INTO {self.table_name} 
                  (Date, Ticker, SentimentScore, Headline)
                VALUES (%s, %s, %s, %s);
                """
                self.cur.execute(insert_sql, (date, ticker, score, title))
        self.db.commit()

    def close(self):
        self.cur.close()
        self.db.close()

if __name__ == '__main__':
    sc = SentimentCollectorFree()
    sc.setup_table()
    print("🗑️  sentiment_data table dropped and recreated")
    sc.truncate_table()
    print("🗑️  sentiment_data table truncated")
    sc.collect()
    print("✅ Sentiment collection complete")
    sc.close()
