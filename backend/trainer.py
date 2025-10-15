import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import joblib
from backend.config import DATA_DIR, MODEL_PATH  # Changed from .config to backend.config
import os


def train_model():
    # Build full path
    data_file = os.path.join(DATA_DIR, 'features_with_events.csv')
    
    print(f"Loading data from: {data_file}")
    df = pd.read_csv(data_file, parse_dates=['date'])
    print(f"Data loaded. Shape: {df.shape}")
    
    features = [
        'close_price','sentiment_score','usd_inr_rate','interest_rate',
        'unemployment_rate','days_to_next_event','days_since_last_event',
        'is_event_window','event_impact_score'
    ]
    
    X = df[features]
    y = df['return_direction']
    
    print(f"Features shape: {X.shape}")
    print(f"Target shape: {y.shape}")
    
    split = int(len(df)*0.7)
    print(f"Training on {split} samples, testing on {len(df)-split} samples")
    
    rf = RandomForestClassifier(n_estimators=200, max_depth=10, random_state=42)
    print("Training model...")
    rf.fit(X[:split], y[:split])
    
    # Create models directory if it doesn't exist
    model_dir = os.path.dirname(MODEL_PATH)
    os.makedirs(model_dir, exist_ok=True)
    
    joblib.dump(rf, MODEL_PATH)
    print(f"✅ Model trained and saved to: {MODEL_PATH}")


if __name__ == "__main__":
    train_model()
