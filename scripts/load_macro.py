import os
import sys
import pandas as pd
import mysql.connector

# Set PROJECT_ROOT and ensure local config import
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from config import DATABASE_CONFIG

# Load the cleaned macro file with date, usd_inr_rate, interest_rate
macro = pd.read_csv('data/macro_all_clean.csv', parse_dates=['date'])

# Connect to the database
conn = mysql.connector.connect(**DATABASE_CONFIG)
cur = conn.cursor()

# Delete old macro data for the date range
cur.execute(
    "DELETE FROM macro_indicators WHERE date BETWEEN %s AND %s;",
    (macro['date'].min().date(), macro['date'].max().date())
)

# Prepare and execute insert statement
sql = """
INSERT INTO macro_indicators (date, usd_inr_rate, interest_rate)
VALUES (%s, %s, %s)
ON DUPLICATE KEY UPDATE
  usd_inr_rate=VALUES(usd_inr_rate),
  interest_rate=VALUES(interest_rate)
"""

for _, r in macro.iterrows():
    # Use .date() for datetime values, None for NaN
    usd_inr = None if pd.isna(r['usd_inr_rate']) else float(r['usd_inr_rate'])
    interest = None if pd.isna(r['interest_rate']) else float(r['interest_rate'])
    cur.execute(sql, (r['date'].date(), usd_inr, interest))

conn.commit()
cur.close()
conn.close()
print(f"Inserted {len(macro)} macro rows into macro_indicators table.")
import os
import sys
import pandas as pd
import mysql.connector

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from config import DATABASE_CONFIG

macro = pd.read_csv('data/macro_all_clean.csv', parse_dates=['date'])

conn = mysql.connector.connect(**DATABASE_CONFIG)
cur = conn.cursor()

cur.execute(
    "DELETE FROM macro_indicators WHERE date BETWEEN %s AND %s;",
    (macro['date'].min().date(), macro['date'].max().date())
)

sql = """
INSERT INTO macro_indicators (date, usd_inr_rate, interest_rate)
VALUES (%s, %s, %s)
ON DUPLICATE KEY UPDATE
  usd_inr_rate=VALUES(usd_inr_rate),
  interest_rate=VALUES(interest_rate)
"""

for _, r in macro.iterrows():
    usd_inr = None if pd.isna(r['usd_inr_rate']) else float(r['usd_inr_rate'])
    interest = None if pd.isna(r['interest_rate']) else float(r['interest_rate'])
    cur.execute(sql, (r['date'].date(), usd_inr, interest))

conn.commit()
cur.close()
conn.close()
print(f"Inserted {len(macro)} macro rows into macro_indicators table.")
