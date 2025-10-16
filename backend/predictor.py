import pandas as pd
import joblib
import os
import sys

# Add project root to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config import DATA_DIR, MODEL_PATH

class StockPredictor:
    def __init__(self):
        self.model = joblib.load(MODEL_PATH)
        self.features = [
            'close_price','sentiment_score','usd_inr_rate','interest_rate',
            'unemployment_rate','days_to_next_event','days_since_last_event',
            'is_event_window','event_impact_score'
        ]
        data_path = os.path.join(DATA_DIR, 'features_with_events.csv')
        self.data = pd.read_csv(data_path, parse_dates=['date'])

    def get_latest_features(self, ticker):
        df_t = self.data[self.data['ticker']==ticker].sort_values('date').iloc[-1]
        return df_t[self.features].values.reshape(1,-1)

    def predict(self, ticker):
        try:
            # Remove .NS suffix if present
            clean_ticker = ticker.replace('.NS', '')
            
            # Check if ticker exists in the data
            available_tickers = self.data['ticker'].unique()
            if clean_ticker not in available_tickers:
                available_str = ", ".join(available_tickers)
                raise ValueError(f"Ticker {ticker} not found. Available tickers: {available_str}")
                
            X = self.get_latest_features(clean_ticker)
            pred = int(self.model.predict(X)[0])
            conf = float(self.model.predict_proba(X)[0][pred])
            return pred, conf
        except Exception as e:
            raise ValueError(f"Error predicting for {ticker}: {str(e)}")
            
    def get_latest_features(self, ticker):
        # Remove .NS suffix if present
        clean_ticker = ticker.replace('.NS', '')
        df_t = self.data[self.data['ticker']==clean_ticker].sort_values('date').iloc[-1]
        return df_t[self.features].values.reshape(1,-1)
