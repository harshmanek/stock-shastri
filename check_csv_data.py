import pandas as pd
import mysql.connector
from config import DATABASE_CONFIG

# Check CSV dates and content
print("🔍 CHECKING CSV FILE...")
df = pd.read_csv('data/financial_news.csv', parse_dates=['Date'])
print(f'CSV Shape: {df.shape}')
print(f'Date range: {df["Date"].min()} to {df["Date"].max()}')
print('\nSample titles:')
print(df['Title'].head(5).tolist())

print('\nTicker mentions in CSV:')
for ticker in ['TCS', 'HDFC', 'Bajaj', 'Asian', 'Lemon', 'VBL']:
    count = df['Title'].str.contains(ticker, case=False, na=False).sum()
    print(f'{ticker}: {count} mentions')

# Check stock dates
print("\n🔍 CHECKING STOCK DATES...")
conn = mysql.connector.connect(**DATABASE_CONFIG)
cursor = conn.cursor()
cursor.execute('SELECT MIN(date), MAX(date) FROM stocks')
result = cursor.fetchone()
print(f'Stock date range: {result[0]} to {result[1]}')
cursor.close()
conn.close()
