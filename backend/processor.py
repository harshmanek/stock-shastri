import pandas as pd
import os
import sys

# Add project root to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from collector import load_raw_macro
from config import DATA_DIR

def merge_macro_and_events(events_df):
    fx, repo, unemp = load_raw_macro()
    macro = (fx
        .merge(repo, on='date', how='outer')
        .merge(unemp, on='date', how='outer')
        .merge(events_df, on='date', how='left')
    )
    for col in ['usd_inr_rate','interest_rate','unemployment_rate']:
        macro[col] = macro[col].ffill().bfill()
    for col in ['is_event_window','event_impact_score']:
        macro[col] = macro[col].fillna(0)
    for col in ['days_to_next_event','days_since_last_event']:
        macro[col] = macro[col].fillna(365)
    
    output_path = os.path.join(DATA_DIR, 'macro_complete.csv')
    macro.to_csv(output_path, index=False)
    return macro
