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
from technical_analysis import get_technical_indicators, get_price_statistics
from timeframe_prediction import TimeframePredictor

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Initialize predictors and watchlist
base_predictor = StockPredictor()
timeframe_predictor = TimeframePredictor(base_predictor)
watchlist = {}

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

@app.route('/technical/<ticker>', methods=['GET'])
def get_technical(ticker):
    """Get technical indicators for a stock"""
    indicators = get_technical_indicators(ticker)
    if indicators is None:
        return jsonify({'error': f'Could not calculate indicators for {ticker}'}), 404
    return jsonify(indicators)

@app.route('/statistics/<ticker>', methods=['GET'])
def get_statistics(ticker):
    """Get enhanced price statistics for a stock"""
    stats = get_price_statistics(ticker)
    if stats is None:
        return jsonify({'error': f'Could not calculate statistics for {ticker}'}), 404
    return jsonify(stats)

@app.route('/watchlist', methods=['GET', 'POST', 'DELETE'])
def manage_watchlist():
    """Manage user watchlist"""
    if request.method == 'GET':
        return jsonify(list(watchlist.values()))
    
    elif request.method == 'POST':
        data = request.get_json()
        ticker = data.get('ticker')
        if not ticker:
            return jsonify({'error': 'Ticker is required'}), 400
        
        # Add to watchlist with timestamp
        watchlist[ticker] = {
            'ticker': ticker,
            'added_at': pd.Timestamp.now().isoformat()
        }
        return jsonify({'message': f'Added {ticker} to watchlist'})
    
    elif request.method == 'DELETE':
        data = request.get_json()
        ticker = data.get('ticker')
        if not ticker:
            return jsonify({'error': 'Ticker is required'}), 400
        
        if ticker in watchlist:
            del watchlist[ticker]
            return jsonify({'message': f'Removed {ticker} from watchlist'})
        return jsonify({'error': 'Ticker not found in watchlist'}), 404

@app.route('/predict/timeframes/<ticker>', methods=['GET'])
def predict_timeframes(ticker):
    """Get predictions for multiple timeframes"""
    try:
        results = timeframe_predictor.predict_multiple_timeframes(ticker)
        accuracy = timeframe_predictor.get_historical_accuracy(ticker)
        return jsonify({
            'predictions': results,
            'accuracy': accuracy
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 400

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
        # Remove .NS suffix if present
        clean_ticker = ticker.replace('.NS', '')
        
        # Get price history from predictor's data
        df = predictor.data[predictor.data['ticker'] == clean_ticker].sort_values('date')
        
        if len(df) == 0:
            return jsonify({'error': f'Ticker {ticker} not found'}), 404
            
        # Get last 90 days of data for better visualization
        df = df.tail(90)
        
        # Extract dates and prices, convert dates to ISO format
        dates = df['date'].dt.strftime('%Y-%m-%d').tolist()
        prices = df['close_price'].tolist()
        
        return jsonify({
            'dates': dates,
            'prices': prices
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=8000)
