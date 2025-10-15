import os

# Get the backend directory (where this config.py file is)
BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))

# Get the project root (one level up from backend/)
PROJECT_ROOT = os.path.dirname(BACKEND_DIR)

# Database configuration
DATABASE_CONFIG = {
    'host': 'localhost',
    'user': 'root',           # Change to your MySQL username
    'password': 'your_pass',  # Change to your MySQL password
    'database': 'stock_shastri_db'
}

# Data and model paths (using absolute paths)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
MODEL_PATH = os.path.join(PROJECT_ROOT, 'models', 'rf_model.pkl')

# Debug: Print paths to verify
if __name__ == "__main__":
    print("Project root:", PROJECT_ROOT)
    print("Data directory:", DATA_DIR)
    print("Model path:", MODEL_PATH)
