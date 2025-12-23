import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
import joblib
import os
import sys

# Add project root to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config import DATA_DIR, MODEL_PATH


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
    
    # Evaluate the model on the testing data
    y_test = y[split:]
    X_test = X[split:]
    y_pred = rf.predict(X_test)

    # Calculate accuracy
    accuracy = accuracy_score(y_test, y_pred)
    print(f"Model Accuracy: {accuracy * 100:.2f}%")

    # Detailed classification report
    print("Classification Report:")
    print(classification_report(y_test, y_pred))
    
    # Create models directory if it doesn't exist
    model_dir = os.path.dirname(MODEL_PATH)
    os.makedirs(model_dir, exist_ok=True)
    
    joblib.dump(rf, MODEL_PATH)
    print(f"✅ Model trained and saved to: {MODEL_PATH}")


if __name__ == "__main__":
    train_model()