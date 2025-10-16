from flask import Flask, jsonify, request
from flask_cors import CORS
import os
import sys
import pandas as pd

# Add project root to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from trainer import train_model
from processor import merge_macro_and_events
from predictor import StockPredictor
from config import DATA_DIR, MODEL_PATH

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

predictor = StockPredictor()

@app.route('/')
def home():
    return jsonify({
        'message': 'Stock Prediction API',
        'endpoints': {
            'predict': '/predict/<ticker>',
            'train': '/train',
            'update_macro': '/update_macro'
        }
    })

@app.route('/train', methods=['POST'])
def retrain():
    train_model()
    # Reload predictor with new model
    global predictor
    predictor = StockPredictor()
    return jsonify({'status': 'model retrained'}), 200

@app.route('/update_macro', methods=['POST'])
def update_macro():
    events = pd.read_csv(os.path.join(DATA_DIR, 'event_features.csv'), parse_dates=['date'])
    merge_macro_and_events(events)
    return jsonify({'status': 'macro updated'}), 200

@app.route('/predict/<ticker>', methods=['GET'])
def predict(ticker):
    try:
        pred, conf = predictor.predict(ticker)
        return jsonify({
            'ticker': ticker,
            'prediction': pred,
            'confidence': round(conf, 4),
            'direction': 'UP' if pred == 1 else 'DOWN'
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/feature_importances', methods=['GET'])
@app.route('/feature_importances/<ticker>', methods=['GET'])
def get_feature_importances(ticker=None):
    try:
        importances = predictor.model.feature_importances_.tolist()
        feature_names = predictor.features
        
        if ticker:
            # Get feature values for the specific ticker
            X = predictor.get_latest_features(ticker)
            feature_values = X[0].tolist()
            
            # Scale importances based on feature values
            scaled_importances = [imp * abs(val) for imp, val in zip(importances, feature_values)]
            total = sum(scaled_importances)
            if total > 0:  # Normalize
                scaled_importances = [imp/total for imp in scaled_importances]
            importances = scaled_importances
            
        return jsonify({
            'features': feature_names,
            'importances': importances
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/price_history/<ticker>', methods=['GET'])
def get_price_history(ticker):
    try:
        # Get last 30 days of price data
        df = predictor.data[predictor.data['ticker'] == ticker].sort_values('date')
        df = df.tail(30)
        
        return jsonify({
            'dates': df['date'].dt.strftime('%Y-%m-%d').tolist(),
            'prices': df['close_price'].tolist()
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=8000)
