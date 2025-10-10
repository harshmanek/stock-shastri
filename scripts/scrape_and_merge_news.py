import os, sys
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_dir)

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import time
import random
from config import STOCK_SYMBOLS

class NewsScraperMerger:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.existing_csv_path = os.path.join(root_dir, 'data/financial_news.csv')
        self.new_headlines = []
        
    def scrape_moneycontrol_stock_news(self, ticker_symbol, company_name, max_pages=5):
        """Scrape MoneyControl for stock-specific news"""
        print(f"🔍 Scraping MoneyControl for {company_name} ({ticker_symbol})...")
        
        # MoneyControl search URL format
        search_term = company_name.replace(' ', '+')
        base_url = f"https://www.moneycontrol.com/news/tags/{search_term.lower()}.html"
        
        try:
            for page in range(1, max_pages + 1):
                url = f"{base_url}?page={page}" if page > 1 else base_url
                response = requests.get(url, headers=self.headers)
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find news articles
                articles = soup.find_all('li', class_='clearfix')
                
                for article in articles:
                    try:
                        # Extract title
                        title_element = article.find('a')
                        if title_element:
                            title = title_element.get_text().strip()
                            
                            # Extract date
                            date_element = article.find('span')
                            if date_element:
                                date_text = date_element.get_text().strip()
                                # Parse date (format: "October 02, 2025")
                                try:
                                    date_obj = datetime.strptime(date_text, "%B %d, %Y")
                                except:
                                    # Try alternative format
                                    try:
                                        date_obj = datetime.strptime(date_text, "%b %d, %Y")
                                    except:
                                        date_obj = datetime.now()  # Default to today
                                
                                self.new_headlines.append({
                                    'Date': date_obj,
                                    'Title': title,
                                    'Description': title,  # Use title as description
                                    'Source': 'MoneyControl',
                                    'Ticker': ticker_symbol.replace('.NS', '')
                                })
                    except Exception as e:
                        continue
                        
                # Random delay to be respectful
                time.sleep(random.uniform(1, 3))
                
        except Exception as e:
            print(f"❌ Error scraping MoneyControl for {ticker_symbol}: {e}")
    
    def scrape_economic_times_news(self, ticker_symbol, company_name, max_articles=50):
        """Scrape Economic Times for stock news"""
        print(f"🔍 Scraping Economic Times for {company_name}...")
        
        try:
            # Economic Times search URL
            search_term = company_name.replace(' ', '%20')
            url = f"https://economictimes.indiatimes.com/topic/{search_term}"
            
            response = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find news articles
            articles = soup.find_all('div', class_='eachStory')[:max_articles]
            
            for article in articles:
                try:
                    # Extract title
                    title_element = article.find('h3') or article.find('h2')
                    if title_element:
                        title_link = title_element.find('a')
                        if title_link:
                            title = title_link.get_text().strip()
                            
                            # Extract date
                            date_element = article.find('time') or article.find('span', class_='date-format')
                            if date_element:
                                date_text = date_element.get_text().strip()
                                try:
                                    # Try to parse various date formats
                                    if 'ago' in date_text.lower():
                                        date_obj = datetime.now() - timedelta(days=1)
                                    else:
                                        date_obj = datetime.strptime(date_text, "%b %d, %Y")
                                except:
                                    date_obj = datetime.now()
                            else:
                                date_obj = datetime.now()
                            
                            self.new_headlines.append({
                                'Date': date_obj,
                                'Title': title,
                                'Description': title,
                                'Source': 'Economic Times',
                                'Ticker': ticker_symbol.replace('.NS', '')
                            })
                except Exception as e:
                    continue
                    
            time.sleep(random.uniform(2, 4))
            
        except Exception as e:
            print(f"❌ Error scraping Economic Times for {ticker_symbol}: {e}")
    
    def scrape_business_standard_news(self, ticker_symbol, company_name):
        """Scrape Business Standard for additional coverage"""
        print(f"🔍 Scraping Business Standard for {company_name}...")
        
        try:
            search_term = company_name.replace(' ', '-').lower()
            url = f"https://www.business-standard.com/topic/{search_term}"
            
            response = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find articles
            articles = soup.find_all('div', class_='listingstory')[:20]
            
            for article in articles:
                try:
                    title_element = article.find('h2') or article.find('a')
                    if title_element:
                        title = title_element.get_text().strip()
                        
                        # Try to find date
                        date_element = article.find('span', class_='publish-date')
                        if date_element:
                            date_text = date_element.get_text().strip()
                            try:
                                date_obj = datetime.strptime(date_text, "%B %d, %Y")
                            except:
                                date_obj = datetime.now()
                        else:
                            date_obj = datetime.now()
                        
                        self.new_headlines.append({
                            'Date': date_obj,
                            'Title': title,
                            'Description': title,
                            'Source': 'Business Standard',
                            'Ticker': ticker_symbol.replace('.NS', '')
                        })
                except:
                    continue
                    
            time.sleep(random.uniform(1, 2))
            
        except Exception as e:
            print(f"❌ Error scraping Business Standard for {ticker_symbol}: {e}")
    
    def scrape_all_stocks(self):
        """Scrape news for all stocks"""
        print("🚀 Starting news scraping for all stocks...")
        
        for ticker, company_name in STOCK_SYMBOLS.items():
            print(f"\n📰 Scraping news for {company_name}...")
            
            # Scrape from all sources
            self.scrape_moneycontrol_stock_news(ticker, company_name, max_pages=3)
            self.scrape_economic_times_news(ticker, company_name, max_articles=30)
            self.scrape_business_standard_news(ticker, company_name)
            
            print(f"✅ Collected {len([h for h in self.new_headlines if h['Ticker'] == ticker.replace('.NS', '')])} headlines for {company_name}")
            
            # Longer delay between stocks
            time.sleep(random.uniform(3, 7))
    
    def merge_with_existing_csv(self):
        """Merge scraped data with existing CSV"""
        print("\n🔗 Merging with existing CSV...")
        
        try:
            # Load existing CSV
            existing_df = pd.read_csv(self.existing_csv_path, parse_dates=['Date'])
            print(f"📊 Existing CSV has {len(existing_df)} records")
            
            # Convert new headlines to DataFrame
            new_df = pd.DataFrame(self.new_headlines)
            print(f"📊 Scraped {len(new_df)} new headlines")
            
            if len(new_df) == 0:
                print("❌ No new headlines scraped. Check your internet connection or website changes.")
                return
            
            # Ensure same columns
            if 'Description' not in existing_df.columns:
                existing_df['Description'] = existing_df['Title']
            if 'Source' not in existing_df.columns:
                existing_df['Source'] = 'Original CSV'
            if 'Ticker' not in existing_df.columns:
                existing_df['Ticker'] = 'Unknown'
            
            # Combine DataFrames
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            
            # Remove duplicates based on Date and Title
            combined_df = combined_df.drop_duplicates(subset=['Date', 'Title'], keep='first')
            
            # Sort by date
            combined_df = combined_df.sort_values('Date').reset_index(drop=True)
            
            # Save back to CSV
            combined_df.to_csv(self.existing_csv_path, index=False)
            
            print(f"✅ Merged CSV saved with {len(combined_df)} total records")
            print(f"📈 Added {len(combined_df) - len(existing_df)} new unique headlines")
            
            # Show summary by ticker
            print("\n📊 Headlines by ticker after merge:")
            for ticker in STOCK_SYMBOLS.keys():
                clean_ticker = ticker.replace('.NS', '')
                count = combined_df['Title'].str.contains(clean_ticker, case=False, na=False).sum()
                print(f"{clean_ticker}: {count} mentions")
                
        except Exception as e:
            print(f"❌ Error merging CSV: {e}")
    
    def run(self):
        """Execute complete scraping and merging process"""
        print("🎯 Starting news scraping and merging process...")
        self.scrape_all_stocks()
        self.merge_with_existing_csv()
        print("\n🎉 News scraping and merging completed!")

if __name__ == "__main__":
    scraper = NewsScraperMerger()
    scraper.run()
