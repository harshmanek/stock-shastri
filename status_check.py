import mysql.connector
import pandas as pd
import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from config import DATABASE_CONFIG

def check_project_status():
    print("🔍 CHECKING PROJECT STATUS...\n")
    
    # 1. Check database tables
    conn = mysql.connector.connect(**DATABASE_CONFIG)
    cursor = conn.cursor()
    
    print("📊 DATABASE STATUS:")
    tables = ['stocks', 'sentiment_data', 'macro_indicators', 'market_events']
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table}: {count} records")
    
    # 2. Check stock data by ticker
    print("\n📈 STOCK DATA BY TICKER:")
    cursor.execute("SELECT ticker, COUNT(*), MIN(date), MAX(date) FROM stocks GROUP BY ticker")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]} records ({row[2]} to {row[3]})")
    
    # 3. Check sentiment data
    print("\n🎭 SENTIMENT DATA:")
    cursor.execute("SELECT ticker, COUNT(*) FROM sentiment_data GROUP BY ticker")
    sentiment_results = cursor.fetchall()
    if sentiment_results:
        for row in sentiment_results:
            print(f"  {row[0]}: {row[1]} sentiment records")
    else:
        print("  ❌ No sentiment data found")
    
    # 4. Check CSV file
    print("\n📰 NEWS CSV STATUS:")
    try:
        df = pd.read_csv('data/financial_news.csv', parse_dates=['Date'])
        print(f"  Total headlines: {len(df)}")
        print(f"  Date range: {df['Date'].min()} to {df['Date'].max()}")
        
        if 'Ticker' in df.columns:
            print("  Headlines by ticker:")
            for ticker in ['TCS', 'HDFCBANK', 'BAJFINANCE', 'ASIANPAINT', 'LEMONTREE', 'VBL']:
                count = (df['Ticker'] == ticker).sum()
                print(f"    {ticker}: {count}")
    except Exception as e:
        print(f"  ❌ Error reading CSV: {e}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_project_status()
