import pandas as pd
import joblib
from backend.config import DATA_DIR, MODEL_PATH
import os

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
        X = self.get_latest_features(ticker)
        pred = int(self.model.predict(X)[0])
        conf = float(self.model.predict_proba(X)[0][pred])
        return pred, conf
