# scripts/collect_sentiment.py

import os, sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_dir)

import mysql.connector
from config import STOCK_SYMBOLS, DATABASE_CONFIG, NEWS_CSV_PATH
import pandas as pd
from datetime import timedelta
from nltk.sentiment.vader import SentimentIntensityAnalyzer

class SentimentCollectorFree:
    def __init__(self):
        # Database and sentiment analyzer
        self.db = mysql.connector.connect(**DATABASE_CONFIG)
        self.sia = SentimentIntensityAnalyzer()
        # Load news CSV
        self.news_df = pd.read_csv(os.path.join(root_dir, NEWS_CSV_PATH), parse_dates=['Date'])
        # Detect the headline column (case-insensitive)
        hdrs = [c for c in self.news_df.columns if 'headline' in c.lower() or 'title' in c.lower()]
        if not hdrs:
            raise KeyError(f"No column containing 'headline' or 'title' found in {NEWS_CSV_PATH}")
        self.headline_col = hdrs[0]
        print(f"✅ Using '{self.headline_col}' column for news headlines")

    def get_stock_dates(self, ticker):
        cursor = self.db.cursor()
        cursor.execute("SELECT date FROM stocks WHERE ticker=%s ORDER BY date", (ticker,))
        dates = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return dates

    def collect_sentiment_for_date(self, ticker, date):
        # Filter by date and ticker keyword in headline
        mask = (
            (self.news_df['Date'].dt.date == date) &
            self.news_df[self.headline_col].str.contains(ticker, case=False, na=False)
        )
        headlines = self.news_df.loc[mask, self.headline_col].tolist()
        if not headlines:
            return None, 0
        scores = [self.sia.polarity_scores(text)['compound'] for text in headlines]
        return sum(scores) / len(scores), len(headlines)

    def insert_sentiment(self, ticker, date, score, count):
        cursor = self.db.cursor()
        cursor.execute(
            "INSERT IGNORE INTO sentiment_data (ticker,date,sentiment_score,tweet_count,news_count) "
            "VALUES (%s,%s,%s,0,%s)",
            (ticker, date, score, count)
        )
        self.db.commit()
        cursor.close()

    def run(self):
        for full_ticker in STOCK_SYMBOLS.keys():
            ticker = full_ticker.replace('.NS','')
            print(f"\n🔄 Processing sentiment for {ticker}")
            for date in self.get_stock_dates(ticker):
                score, count = self.collect_sentiment_for_date(ticker, date)
                if score is not None:
                    self.insert_sentiment(ticker, date, score, count)
            print(f"✅ Completed {ticker}")

if __name__ == "__main__":
    sc = SentimentCollectorFree()
    sc.run()
