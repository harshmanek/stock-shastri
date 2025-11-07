import pandas as pd
import numpy as np
from config import DATA_DIR
import os

def calculate_rsi(data, period=14):
    """Calculate Relative Strength Index"""
    delta = data.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_macd(data, short_period=12, long_period=26, signal_period=9):
    """Calculate MACD (Moving Average Convergence Divergence)"""
    exp1 = data.ewm(span=short_period, adjust=False).mean()
    exp2 = data.ewm(span=long_period, adjust=False).mean()
    macd = exp1 - exp2
    signal = macd.ewm(span=signal_period, adjust=False).mean()
    return pd.DataFrame({
        'macd': macd,
        'signal': signal,
        'histogram': macd - signal
    })

def calculate_moving_averages(data):
    """Calculate common moving averages"""
    return pd.DataFrame({
        'MA5': data.rolling(window=5).mean(),
        'MA20': data.rolling(window=20).mean(),
        'MA50': data.rolling(window=50).mean(),
        'MA200': data.rolling(window=200).mean(),
    })

def get_price_statistics(ticker):
    """Get enhanced price statistics for a given ticker"""
    try:
        # Load price data
        price_data = pd.read_csv(os.path.join(DATA_DIR, 'features.csv'))
        ticker_data = price_data[price_data['ticker'] == ticker].copy()
        if ticker_data.empty:
            return None
        
        # Calculate current price and changes
        current_price = ticker_data['close'].iloc[-1]
        prev_close = ticker_data['close'].iloc[-2]
        daily_change = ((current_price - prev_close) / prev_close) * 100
        
        # Calculate 52-week high/low
        year_data = ticker_data.tail(252)  # ~252 trading days in a year
        week_52_high = year_data['high'].max()
        week_52_low = year_data['low'].min()
        
        # Calculate average volume
        avg_volume = ticker_data['volume'].tail(30).mean()  # 30-day average
        
        return {
            'current_price': round(current_price, 2),
            'daily_change_percent': round(daily_change, 2),
            'week_52_high': round(week_52_high, 2),
            'week_52_low': round(week_52_low, 2),
            'avg_volume': int(avg_volume),
            'from_52_week_high': round(((current_price - week_52_high) / week_52_high) * 100, 2),
            'from_52_week_low': round(((current_price - week_52_low) / week_52_low) * 100, 2)
        }
    except Exception as e:
        print(f"Error calculating price statistics: {str(e)}")
        return None

def get_technical_indicators(ticker):
    """Get all technical indicators for a given ticker"""
    try:
        # Load price data
        price_data = pd.read_csv(os.path.join(DATA_DIR, 'features.csv'))
        ticker_data = price_data[price_data['ticker'] == ticker].copy()
        if ticker_data.empty:
            return None
        
        # Calculate indicators
        close_prices = ticker_data['close']
        rsi = calculate_rsi(close_prices)
        macd_data = calculate_macd(close_prices)
        moving_averages = calculate_moving_averages(close_prices)
        
        # Get the most recent values
        latest_data = {
            'rsi': round(rsi.iloc[-1], 2),
            'macd': round(macd_data['macd'].iloc[-1], 2),
            'macd_signal': round(macd_data['signal'].iloc[-1], 2),
            'macd_histogram': round(macd_data['histogram'].iloc[-1], 2),
            'ma5': round(moving_averages['MA5'].iloc[-1], 2),
            'ma20': round(moving_averages['MA20'].iloc[-1], 2),
            'ma50': round(moving_averages['MA50'].iloc[-1], 2),
            'ma200': round(moving_averages['MA200'].iloc[-1], 2)
        }
        
        # Add interpretation
        latest_data['rsi_interpretation'] = 'Overbought' if latest_data['rsi'] > 70 else 'Oversold' if latest_data['rsi'] < 30 else 'Neutral'
        latest_data['macd_interpretation'] = 'Bullish' if latest_data['macd'] > latest_data['macd_signal'] else 'Bearish'
        latest_data['trend'] = 'Bullish' if latest_data['ma50'] > latest_data['ma200'] else 'Bearish'
        
        return latest_data
    except Exception as e:
        print(f"Error calculating technical indicators: {str(e)}")
        return None