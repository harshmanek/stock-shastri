# config.py

# MySQL Database Configuration
DATABASE_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'hmrl1',  # <— Replace with your MySQL root password
    'database': 'stock_prediction'
}

# Your selected stocks (tickers must match those in stocks table)
STOCK_SYMBOLS = {
    'TCS.NS': 'Tata Consultancy Services',
    'HDFCBANK.NS': 'HDFC Bank',
    'BAJFINANCE.NS': 'Bajaj Finance',
    'ASIANPAINT.NS': 'Asian Paints',
    'LEMONTREE.NS': 'Lemon Tree Hotels',
    'VBL.NS': 'Varun Beverages'
}

# File paths
NEWS_CSV_PATH = 'data/financial_news.csv'  # Kaggle news dataset

# NLTK Settings
NLTK_VADER_LEXICON = 'vader_lexicon'

# Other global settings
DATE_FORMAT = '%Y-%m-%d'
