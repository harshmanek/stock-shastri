import pandas as pd
import numpy as np
from datetime import datetime

# Load and check the CSV
csv_path = 'data/financial_news.csv'
news_df = pd.read_csv(csv_path)
news_df['Date'] = pd.to_datetime(news_df['Date'], errors='coerce')
news_df.dropna(subset=['Date'], inplace=True)

print("🔍 DEBUGGING SENTIMENT DATA MISMATCH...\n")

# 1. Check date ranges
print("📅 DATE ANALYSIS:")
print(f"News CSV date range: {news_df['Date'].min()} to {news_df['Date'].max()}")
print(f"Total headlines: {len(news_df)}")

# 2. Check sample headlines
print(f"\n📰 SAMPLE HEADLINES:")
sample_headlines = news_df['Title'].head(10).tolist()
for i, headline in enumerate(sample_headlines, 1):
    print(f"{i:2d}. {headline}")

# 3. Test search term matching
print(f"\n🔍 SEARCH TERM ANALYSIS:")
search_terms = {
    'TCS': ['TCS', 'Tata Consultancy', 'IT services'],
    'HDFCBANK': ['HDFC', 'banking', 'private bank'],
    'BAJFINANCE': ['Bajaj', 'finance', 'NBFC'],
    'ASIANPAINT': ['Asian Paints', 'paint', 'decorative'],
    'LEMONTREE': ['Lemon Tree', 'hotel', 'hospitality'],
    'VBL': ['Varun', 'beverages', 'soft drinks']
}

for ticker, terms in search_terms.items():
    print(f"\n{ticker}:")
    total_matches = 0
    for term in terms:
        matches = news_df['Title'].str.contains(term, case=False, na=False).sum()
        total_matches += matches
        print(f"  '{term}': {matches} matches")
    print(f"  Total unique matches: {total_matches}")

# 4. Check recent dates (2024-2025)
print(f"\n📅 RECENT NEWS (2024-2025):")
recent_news = news_df[news_df['Date'].dt.year >= 2024]
print(f"Headlines from 2024-2025: {len(recent_news)}")

if len(recent_news) > 0:
    print("Sample recent headlines:")
    for headline in recent_news['Title'].head(5):
        print(f"  - {headline}")

# 5. Test a specific date match
print(f"\n🎯 TESTING DATE MATCHING:")
test_date = datetime(2024, 1, 15).date()
date_window = 3

date_start = test_date - pd.Timedelta(days=date_window)
date_end = test_date + pd.Timedelta(days=date_window)

date_mask = (news_df['Date'].dt.date >= date_start) & (news_df['Date'].dt.date <= date_end)
period_news = news_df.loc[date_mask]

print(f"Test date: {test_date}")
print(f"Date window: {date_start} to {date_end}")
print(f"Headlines in window: {len(period_news)}")

if len(period_news) > 0:
    print("Sample headlines in window:")
    for headline in period_news['Title'].head(3):
        print(f"  - {headline}")
