from flask import Flask, jsonify, request
from flask_cors import CORS
from backend.trainer import train_model
from backend.processor import merge_macro_and_events
from backend.predictor import StockPredictor
import pandas as pd
import os
from backend.config import DATA_DIR

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

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5000)
