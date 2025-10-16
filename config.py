# config.py
import os

# Get the project root directory
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# Stock symbols
STOCK_SYMBOLS = ['TCS', 'HDFCBANK', 'BAJFINANCE', 'ASIANPAINT', 'LEMONTREE', 'VBL']

# Database configuration
DATABASE_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'hmrl1',
    'database': 'stock_prediction'
}

# Paths (using absolute paths)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
MODEL_PATH = os.path.join(PROJECT_ROOT, 'models', 'rf_model.pkl')
NEWS_CSV_PATH = os.path.join(DATA_DIR, 'financial_news.csv')

# Debug: Print paths to verify
if __name__ == "__main__":
    print("Project root:", PROJECT_ROOT)
    print("Data directory:", DATA_DIR)
    print("Model path:", MODEL_PATH)
    print("News CSV path:", NEWS_CSV_PATH)
