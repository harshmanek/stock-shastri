import pandas as pd
from pandas_datareader import wb
from backend.config import DATA_DIR
import os

def fetch_unemployment():
    df = wb.download(indicator='SL.UEM.TOTL.ZS', country='IN', start=2010, end=2023)
    df = df.reset_index().rename(columns={'year':'Year','SL.UEM.TOTL.ZS':'unemployment_rate'})
    df['date'] = pd.to_datetime(df['Year'].astype(str)+'-01-01')
    return df[['date','unemployment_rate']]

def load_raw_macro():
    fx   = pd.read_csv(os.path.join(DATA_DIR,'usdinr_clean.csv'), parse_dates=['date'])
    repo = pd.read_csv(os.path.join(DATA_DIR,'repo_daily_clean.csv'), parse_dates=['date'])
    unemp = fetch_unemployment()
    daily = pd.DataFrame({'date':pd.date_range('2019-01-01','2023-01-13')})
    unemp_daily = daily.merge(unemp, on='date', how='left').ffill().bfill()
    return fx, repo, unemp_daily
