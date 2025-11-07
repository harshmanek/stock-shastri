import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from config import DATA_DIR
import json

class PredictionTracker:
    def __init__(self):
        self.history_file = os.path.join(DATA_DIR, 'prediction_history.json')
        self._load_history()

    def _load_history(self):
        """Load prediction history from file"""
        try:
            if os.path.exists(self.history_file):
                with open(self.history_file, 'r') as f:
                    self.history = json.load(f)
            else:
                self.history = {}
        except Exception as e:
            print(f"Error loading prediction history: {str(e)}")
            self.history = {}

    def _save_history(self):
        """Save prediction history to file"""
        try:
            with open(self.history_file, 'w') as f:
                json.dump(self.history, f)
        except Exception as e:
            print(f"Error saving prediction history: {str(e)}")

    def add_prediction(self, ticker, timeframe, prediction, confidence):
        """Add a new prediction to history"""
        if ticker not in self.history:
            self.history[ticker] = {}
        
        timestamp = datetime.now().isoformat()
        if timeframe not in self.history[ticker]:
            self.history[ticker][timeframe] = []
            
        self.history[ticker][timeframe].append({
            'timestamp': timestamp,
            'prediction': prediction,
            'confidence': confidence,
            'verified': False
        })
        self._save_history()

    def get_accuracy_stats(self, ticker, timeframe='1d'):
        """Get accuracy statistics for a ticker and timeframe"""
        if ticker not in self.history or timeframe not in self.history[ticker]:
            return {
                'total_predictions': 0,
                'verified_predictions': 0,
                'accuracy': 0,
                'avg_confidence': 0
            }

        predictions = self.history[ticker][timeframe]
        verified = [p for p in predictions if p['verified']]
        correct = [p for p in verified if p['actual_result'] == p['prediction']]

        total = len(predictions)
        verified_count = len(verified)
        accuracy = len(correct) / verified_count if verified_count > 0 else 0
        avg_confidence = np.mean([p['confidence'] for p in predictions]) if total > 0 else 0

        return {
            'total_predictions': total,
            'verified_predictions': verified_count,
            'accuracy': round(accuracy * 100, 2),
            'avg_confidence': round(avg_confidence * 100, 2)
        }

    def verify_predictions(self, ticker, actual_price, cutoff_date=None):
        """Verify past predictions based on actual price"""
        if ticker not in self.history:
            return

        if cutoff_date is None:
            cutoff_date = datetime.now() - timedelta(days=1)

        for timeframe in self.history[ticker]:
            for pred in self.history[ticker][timeframe]:
                pred_date = datetime.fromisoformat(pred['timestamp'])
                if not pred['verified'] and pred_date < cutoff_date:
                    # Verify prediction
                    pred['verified'] = True
                    pred['actual_result'] = 1 if actual_price > pred['reference_price'] else 0
                    pred['actual_price'] = actual_price

        self._save_history()

class TimeframePredictor:
    def __init__(self, base_predictor):
        self.base_predictor = base_predictor
        self.prediction_tracker = PredictionTracker()

    def predict_multiple_timeframes(self, ticker):
        """Make predictions for multiple timeframes"""
        timeframes = {
            '1d': {'days': 1, 'description': 'Next Day'},
            '1w': {'days': 7, 'description': 'Next Week'},
            '1m': {'days': 30, 'description': 'Next Month'}
        }

        results = {}
        try:
            current_price = self._get_current_price(ticker)
            
            for tf, info in timeframes.items():
                prediction = self.base_predictor.predict(ticker, horizon_days=info['days'])
                if isinstance(prediction, dict) and 'prediction' in prediction:
                    results[tf] = {
                        'timeframe': info['description'],
                        'prediction': prediction['prediction'],
                        'confidence': prediction.get('confidence', 0),
                        'reference_price': current_price
                    }
                    # Track prediction
                    self.prediction_tracker.add_prediction(
                        ticker, tf, prediction['prediction'], 
                        prediction.get('confidence', 0)
                    )

        except Exception as e:
            print(f"Error making multiple timeframe predictions: {str(e)}")

        return results

    def _get_current_price(self, ticker):
        """Get current price for a ticker"""
        try:
            price_data = pd.read_csv(os.path.join(DATA_DIR, 'features.csv'))
            ticker_data = price_data[price_data['ticker'] == ticker].copy()
            if not ticker_data.empty:
                return ticker_data['close'].iloc[-1]
        except Exception as e:
            print(f"Error getting current price: {str(e)}")
        return None

    def get_historical_accuracy(self, ticker):
        """Get historical accuracy for all timeframes"""
        return {
            '1d': self.prediction_tracker.get_accuracy_stats(ticker, '1d'),
            '1w': self.prediction_tracker.get_accuracy_stats(ticker, '1w'),
            '1m': self.prediction_tracker.get_accuracy_stats(ticker, '1m')
        }