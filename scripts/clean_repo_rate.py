import pandas as pd

# --- Step 1: Load and clean raw repo rate CSV ---
df = pd.read_csv('data/repo_rate_raw.csv')

# Clean interest rate: remove %, strip, convert to float
df['interest_rate'] = (
    df['interest_rate']
      .astype(str)
      .str.replace('%', '', regex=False)
      .str.strip()
      .astype(float)
)

# Convert date column to datetime (auto-detecting formats)
df['date'] = pd.to_datetime(df['date'], errors='coerce')
df = df[['date', 'interest_rate']].dropna().sort_values('date')

# --- Step 2: Forward-fill to full daily calendar in your project window ---
project_days = pd.DataFrame({'date': pd.date_range('2019-01-01', '2023-01-13', freq='D')})
repo_daily = project_days.merge(df, on='date', how='left')

# Fill missing values: forward-fill, then backward-fill so start-of-series is not NaN
repo_daily['interest_rate'] = repo_daily['interest_rate'].ffill().bfill()

# --- Step 3: Save cleaned daily repo rate file ---
repo_daily.to_csv('data/repo_daily_clean.csv', index=False)
print("Cleaned daily repo rates saved to data/repo_daily_clean.csv")
print(repo_daily.head(12))
print(repo_daily.tail(10))
