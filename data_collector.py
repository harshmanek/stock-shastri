import yfinance as yf
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
import time

class StockDataCollector:
    def __init__(self):
        # Our 6 selected stocks
        self.stocks = {
            'TCS.NS': 'Tata Consultancy Services',
            'HDFCBANK.NS': 'HDFC Bank',
            'BAJFINANCE.NS': 'Bajaj Finance',
            'ASIANPAINT.NS': 'Asian Paints',
            'LEMONTREE.NS': 'Lemon Tree Hotels',
            'VBL.NS': 'Varun Beverages'
        }
        
        # Database connection
        self.connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='hmrl1', 
            database='stock_prediction'
        )
    
    def collect_historical_data(self, start_date='2019-01-01', end_date=None):
        """Collect 5+ years of historical data for all stocks"""
        
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        print(f"📈 Collecting stock data from {start_date} to {end_date}")
        
        for ticker, name in self.stocks.items():
            print(f"\n🔄 Downloading {name} ({ticker})...")
            
            try:
                # Download data using yfinance
                stock = yf.Ticker(ticker)
                hist = stock.history(start=start_date, end=end_date)
                
                if hist.empty:
                    print(f"❌ No data found for {ticker}")
                    continue
                
                
                hist.reset_index(inplace=True)
                hist['ticker'] = ticker.replace('.NS', '')
                
                # Clean column names
                hist.columns = ['date', 'open_price', 'high_price', 'low_price', 
                              'close_price', 'volume', 'dividends', 'stock_splits', 'ticker']
                
                # Select only needed columns
                hist = hist[['ticker', 'date', 'open_price', 'high_price', 
                           'low_price', 'close_price', 'volume']]
                
                
                self.insert_stock_data(hist)
                
                print(f"✅ {name}: {len(hist)} records inserted")
                time.sleep(1)  
                
            except Exception as e:
                print(f"❌ Error downloading {ticker}: {e}")
    
    def insert_stock_data(self, df):
        """Insert stock data into database"""
        cursor = self.connection.cursor()
        
        insert_query = """
        INSERT IGNORE INTO stocks (ticker, date, open_price, high_price, low_price, close_price, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        # Convert DataFrame to list of tuples
        data = []
        for _, row in df.iterrows():
            data.append((
                row['ticker'],
                row['date'].strftime('%Y-%m-%d'),
                float(row['open_price']),
                float(row['high_price']),
                float(row['low_price']),
                float(row['close_price']),
                int(row['volume'])
            ))
        
        try:
            cursor.executemany(insert_query, data)
            self.connection.commit()
        except Exception as e:
            print(f"❌ Database error: {e}")
        finally:
            cursor.close()
    
    def get_latest_data(self, ticker):
        """Get the most recent data for a stock"""
        cursor = self.connection.cursor()
        
        query = """
        SELECT * FROM stocks 
        WHERE ticker = %s 
        ORDER BY date DESC 
        LIMIT 5
        """
        
        cursor.execute(query, (ticker.replace('.NS', ''),))
        result = cursor.fetchall()
        cursor.close()
        
        return result
    
    def verify_data(self):
        """Check what data we have collected"""
        cursor = self.connection.cursor()
        
        query = """
        SELECT ticker, COUNT(*) as record_count, MIN(date) as start_date, MAX(date) as end_date
        FROM stocks
        GROUP BY ticker
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        print("\n📊 DATA COLLECTION SUMMARY:")
        print("-" * 70)
        for row in results:
            ticker, count, start, end = row
            print(f"{ticker:<12} | {count:>4} records | {start} to {end}")
        
        cursor.close()

# Test the data collector
if __name__ == "__main__":
    collector = StockDataCollector()
    
    # Collect historical data (this will take a few minutes)
    collector.collect_historical_data()
    
    # Verify what we collected
    collector.verify_data()
    
    # Test getting latest data
    print("\n📈 Latest TCS data:")
    latest = collector.get_latest_data('TCS.NS')
    for row in latest:
        print(row)
