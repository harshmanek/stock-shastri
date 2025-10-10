import os, sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)  # Force local imports first

import mysql.connector
import pandas as pd
from datetime import datetime
import numpy as np

# Import NLTK and download VADER if needed
try:
    from nltk.sentiment.vader import SentimentIntensityAnalyzer
except ImportError:
    import nltk
    nltk.download('vader_lexicon')
    from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Define constants directly (avoid config import issues)
DATABASE_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'hmrl1',  # Replace with your MySQL password
    'database': 'stock_prediction'
}

STOCK_SYMBOLS = {
    'TCS.NS': 'Tata Consultancy Services',
    'HDFCBANK.NS': 'HDFC Bank',
    'BAJFINANCE.NS': 'Bajaj Finance',
    'ASIANPAINT.NS': 'Asian Paints',
    'LEMONTREE.NS': 'Lemon Tree Hotels',
    'VBL.NS': 'Varun Beverages'
}

class FlexibleSentimentCollector:
    def __init__(self):
        self.db = mysql.connector.connect(**DATABASE_CONFIG)
        self.sia = SentimentIntensityAnalyzer()
        
        # Load news CSV
        csv_path = os.path.join(root_dir, 'data/financial_news.csv')
        self.news_df = pd.read_csv(csv_path, parse_dates=['Date'])
        self.news_df['Date'] = pd.to_datetime(self.news_df['Date'], errors='coerce')
        self.news_df.dropna(subset=['Date'], inplace=True)
        print(f"📰 Loaded {len(self.news_df)} headlines from CSV")
        
        # Find headline column
        headline_cols = [c for c in self.news_df.columns if 'title' in c.lower() or 'headline' in c.lower()]
        self.headline_col = headline_cols[0] if headline_cols else 'Title'
        print(f"✅ Using '{self.headline_col}' column for headlines")

    def get_stock_dates(self, ticker):
        """Get all dates for a ticker from stocks table"""
        cursor = self.db.cursor()
        query = "SELECT DISTINCT date FROM stocks WHERE ticker = %s ORDER BY date"
        cursor.execute(query, (ticker,))
        dates = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return dates

    def get_flexible_search_terms(self, ticker):
        """Create multiple search variations for each ticker"""
        search_terms = {
            'TCS': ['TCS', 'Tata Consultancy', 'Tata Consultancy Services', 'IT services', 'software services'],
            'HDFCBANK': ['HDFC', 'HDFC Bank', 'housing development finance', 'private bank', 'banking'],
            'BAJFINANCE': ['Bajaj Finance', 'Bajaj', 'NBFC', 'consumer finance', 'financial services'],
            'ASIANPAINT': ['Asian Paints', 'Asian Paint', 'paint', 'decorative', 'coatings'],
            'LEMONTREE': ['Lemon Tree', 'hotel', 'hospitality', 'accommodation', 'tourism'],
            'VBL': ['Varun Beverages', 'Varun', 'beverages', 'soft drinks', 'pepsi', 'cola']
        }
        
        return search_terms.get(ticker, [ticker])

    def collect_sentiment_for_date(self, ticker, date):
        """Collect sentiment for a specific ticker and date using flexible matching"""
        search_terms = self.get_flexible_search_terms(ticker)
        
        # Filter news by date (±3 days window for more coverage)
        date_start = date - pd.Timedelta(days=3)
        date_end = date + pd.Timedelta(days=3)
        
        date_mask = (self.news_df['Date'].dt.date >= date_start) & (self.news_df['Date'].dt.date <= date_end)
        period_news = self.news_df.loc[date_mask]
        
        if period_news.empty:
            return None, 0
        
        # Find relevant headlines using multiple search terms
        relevant_headlines = []
        for term in search_terms:
            mask = period_news[self.headline_col].str.contains(term, case=False, na=False)
            relevant_headlines.extend(period_news.loc[mask, self.headline_col].tolist())
        
        # Remove duplicates
        relevant_headlines = list(set(relevant_headlines))
        
        if not relevant_headlines:
            # If no specific matches, use general financial sentiment for the period
            general_headlines = period_news[self.headline_col].tolist()[:10]  # Take up to 10 general headlines
            if general_headlines:
                scores = [self.sia.polarity_scores(headline)['compound'] for headline in general_headlines]
                avg_score = np.mean(scores) * 0.5  # Reduce weight for general sentiment
                return avg_score, len(general_headlines)
            return None, 0
        
        # Calculate sentiment scores
        scores = [self.sia.polarity_scores(headline)['compound'] for headline in relevant_headlines]
        avg_score = np.mean(scores)
        
        return avg_score, len(relevant_headlines)

    def insert_sentiment(self, ticker, date, score, count):
        """Insert sentiment data into database"""
        cursor = self.db.cursor()
        cursor.execute(
            "INSERT IGNORE INTO sentiment_data (ticker, date, sentiment_score, tweet_count, news_count) "
            "VALUES (%s, %s, %s, 0, %s)",
            (ticker, date, score, count)
        )
        self.db.commit()
        cursor.close()

    def run(self):
        """Process sentiment for all tickers and dates"""
        total_inserted = 0
        
        for full_ticker in STOCK_SYMBOLS.keys():
            ticker = full_ticker.replace('.NS', '')
            print(f"\n🔄 Processing sentiment for {ticker}")
            
            dates = self.get_stock_dates(ticker)
            print(f"   Found {len(dates)} trading dates")
            
            inserted_count = 0
            for i, date in enumerate(dates):
                score, count = self.collect_sentiment_for_date(ticker, date)
                if score is not None:
                    self.insert_sentiment(ticker, date, score, count)
                    inserted_count += 1
                    total_inserted += 1
                
                # Progress indicator
                if (i + 1) % 200 == 0:
                    print(f"   Processed {i + 1}/{len(dates)} dates...")
            
            print(f"✅ {ticker}: {inserted_count} sentiment records inserted")
        
        print(f"\n🎉 Total sentiment records inserted: {total_inserted}")
        self.db.close()

if __name__ == "__main__":
    collector = FlexibleSentimentCollector()
    collector.run()
