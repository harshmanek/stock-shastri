import pandas as pd

# 1. Load raw CSV
raw = pd.read_csv('data/usdinr_raw.csv')  
# Adjust filename as needed

# 2. Normalize Date column
# Try parsing multiple formats
raw['date'] = pd.to_datetime(
    raw['Date'], 
    format=None,      # infer formats
    dayfirst=False,   # US style month/day/year
    errors='coerce'
)

# 3. Keep only needed columns
df = raw[['date', 'Price']].rename(columns={'Price':'usd_inr_rate'})

# 4. Drop rows with invalid dates or rates
df = df.dropna(subset=['date','usd_inr_rate'])

# 5. Ensure project date window
start, end = '2019-01-01','2023-01-13'
mask = (df['date'] >= start) & (df['date'] <= end)
df = df.loc[mask].sort_values('date')

# 6. Save cleaned CSV
df.to_csv('data/usdinr_clean.csv', index=False)
print(f"Cleaned USD/INR rows: {len(df)} ({df['date'].min().date()}→{df['date'].max().date()})")
