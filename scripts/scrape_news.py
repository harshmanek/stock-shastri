# scripts/scrape_news.py

import os, sys
root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root)

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from config import NEWS_CSV_PATH

def scrape_moneycontrol(ticker, start_year=2021, end_year=2025):
    all_news = []
    for year in range(start_year, end_year+1):
        for month in range(1,13):
            url = f"https://www.moneycontrol.com/stocks/company_info/stock_news.php?sc{ticker.replace('.NS','').lower()}=MC2&month={month}&year={year}"
            resp = requests.get(url)
            soup = BeautifulSoup(resp.text, 'lxml')
            for item in soup.select('.clearfix .PT10 a'):
                date_str = item.find_previous('span').text.strip()
                # parse date like 'Sep 19, 2025'
                try:
                    date = datetime.strptime(date_str, '%b %d, %Y').date()
                except:
                    continue
                headline = item.text.strip()
                all_news.append({'Date': date, 'Title': headline})
    return pd.DataFrame(all_news)

if __name__ == "__main__":
    tickers = ['TCS.NS','HDFCBANK.NS','BAJFINANCE.NS','ASIANPAINT.NS','LEMONTREE.NS','VBL.NS']
    df_list = []
    for t in tickers:
        code = t.replace('.NS','')
        print(f"Scraping {code}...")
        df = scrape_moneycontrol(t)
        df['Ticker'] = code
        df_list.append(df)
    new_df = pd.concat(df_list, ignore_index=True)
    # Append to existing CSV
    csv_path = os.path.join(root, NEWS_CSV_PATH)
    if os.path.exists(csv_path):
        old = pd.read_csv(csv_path, parse_dates=['Date'])
        combined = pd.concat([old, new_df]).drop_duplicates(['Date','Title','Ticker'])
    else:
        combined = new_df
    combined.to_csv(csv_path, index=False)
    print("✅ Updated news CSV through 2025")
