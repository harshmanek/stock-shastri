import os
import sys
import pandas as pd
import mysql.connector

# Ensure config import
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from config import DATABASE_CONFIG

def update_macro_table():
    """Update macro_indicators table with unemployment data"""
    
    # Load merged macro data
    macro = pd.read_csv('data/macro_all_clean.csv', parse_dates=['date'])
    
    conn = mysql.connector.connect(**DATABASE_CONFIG)
    cur = conn.cursor()
    
    # Update existing records with unemployment rate
    sql = """
    INSERT INTO macro_indicators (date, usd_inr_rate, interest_rate, unemployment_rate)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      usd_inr_rate=VALUES(usd_inr_rate),
      interest_rate=VALUES(interest_rate),
      unemployment_rate=VALUES(unemployment_rate)
    """
    
    print(f"Updating {len(macro)} records in macro_indicators table...")
    
    for _, r in macro.iterrows():
        cur.execute(
            sql,
            (r['date'].date(),
             float(r['usd_inr_rate']) if pd.notna(r['usd_inr_rate']) else None,
             float(r['interest_rate']) if pd.notna(r['interest_rate']) else None,
             float(r['unemployment_rate']) if pd.notna(r['unemployment_rate']) else None)
        )
    
    conn.commit()
    cur.close()
    conn.close()
    
    print("✅ Database updated successfully!")

if __name__ == "__main__":
    update_macro_table()
