import mysql.connector
from config import DATABASE_CONFIG

conn = mysql.connector.connect(**DATABASE_CONFIG)
cursor = conn.cursor()

# Check stock data
cursor.execute('SELECT ticker, COUNT(*) as records, MIN(date), MAX(date) FROM stocks GROUP BY ticker')
results = cursor.fetchall()
print('📊 STOCK DATA VERIFICATION:')
for row in results:
    print(f'{row[0]:<12} | {row[1]:>4} records | {row[2]} to {row[3]}')

# Check sentiment data
cursor.execute('SELECT ticker, COUNT(*) as sentiment_records FROM sentiment_data GROUP BY ticker')
results = cursor.fetchall()
print('\n🎭 SENTIMENT DATA VERIFICATION:')
for row in results:
    print(f'{row[0]:<12} | {row[1]:>4} sentiment records')

cursor.close()
conn.close()
