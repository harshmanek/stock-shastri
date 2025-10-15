import os
import sys
import pandas as pd
from pandas_datareader import wb
import matplotlib.pyplot as plt

# Set paths
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')

def download_unemployment_data():
    """Download unemployment data from World Bank"""
    print("Downloading unemployment data from World Bank...")
    
    indicator = 'SL.UEM.TOTL.ZS'  # Unemployment, total (% of total labor force)
    country = 'IN'  # India
    start, end = 2010, 2023
    
    try:
        wb_df = wb.download(indicator=indicator, country=country, start=start, end=end)
        wb_df = wb_df.reset_index().rename(columns={'year':'Year', indicator:'unemployment_rate'})
        wb_df = wb_df.sort_values('Year')
        wb_df['date'] = pd.to_datetime(wb_df['Year'].astype(str) + '-01-01')
        wb_df = wb_df[['date','unemployment_rate']].dropna()
        print("✓ Unemployment data downloaded successfully!")
        return wb_df
    
    except Exception as e:
        print(f"⚠ Error downloading from World Bank: {e}")
        print("Using fallback manual data...")
        
        # Fallback manual data (approximate values for India 2019-2023)
        manual_data = {
            '2019-01-01': 2.55,
            '2020-01-01': 4.84,
            '2021-01-01': 4.66,
            '2022-01-01': 4.10,
            '2023-01-01': 3.25
        }
        wb_df = pd.DataFrame(list(manual_data.items()), columns=['date', 'unemployment_rate'])
        wb_df['date'] = pd.to_datetime(wb_df['date'])
        print("✓ Using manual unemployment data")
        return wb_df

def create_daily_unemployment():
    """Convert annual unemployment to daily forward-filled series"""
    print("Creating daily unemployment series...")
    
    wb_df = download_unemployment_data()
    
    # Create daily series
    daily = pd.DataFrame({'date': pd.date_range('2019-01-01','2023-01-13', freq='D')})
    unemp_daily = daily.merge(wb_df, on='date', how='left').sort_values('date')
    unemp_daily['unemployment_rate'] = unemp_daily['unemployment_rate'].ffill().bfill()
    
    # Save
    unemp_path = os.path.join(DATA_DIR, 'unemployment_daily_clean.csv')
    unemp_daily.to_csv(unemp_path, index=False)
    print(f"✓ Saved unemployment data to {unemp_path}")
    
    return unemp_daily

def merge_macro_data():
    """Merge unemployment with existing macro data"""
    print("Merging with existing macro data...")
    
    # Load existing data
    fx_path = os.path.join(DATA_DIR, 'usdinr_clean.csv')
    repo_path = os.path.join(DATA_DIR, 'repo_daily_clean.csv')
    
    if not os.path.exists(fx_path) or not os.path.exists(repo_path):
        print("❌ Error: USD/INR or repo rate files not found!")
        print("Please make sure these files exist:")
        print(f"  - {fx_path}")
        print(f"  - {repo_path}")
        return None
    
    fx = pd.read_csv(fx_path, parse_dates=['date'])
    repo = pd.read_csv(repo_path, parse_dates=['date'])
    unemp = create_daily_unemployment()
    
    # Merge all
    macro = fx.merge(repo, on='date', how='outer').merge(unemp, on='date', how='outer').sort_values('date')
    
    # Fill missing values
    macro['usd_inr_rate'] = macro['usd_inr_rate'].ffill().bfill()
    macro['interest_rate'] = macro['interest_rate'].ffill().bfill()
    macro['unemployment_rate'] = macro['unemployment_rate'].ffill().bfill()
    
    # Save
    macro_path = os.path.join(DATA_DIR, 'macro_all_clean.csv')
    macro.to_csv(macro_path, index=False)
    
    print(f"✓ Merged macro data saved to {macro_path}")
    print(f"  Shape: {macro.shape}")
    print(f"  Date range: {macro['date'].min()} to {macro['date'].max()}")
    
    # Check for missing values
    missing = macro[['usd_inr_rate', 'interest_rate', 'unemployment_rate']].isna().sum()
    print("\nMissing values check:")
    for col, count in missing.items():
        print(f"  {col}: {count} missing")
    
    return macro

def visualize_unemployment(macro):
    """Create visualization of unemployment rate"""
    plt.figure(figsize=(14,5))
    plt.plot(macro['date'], macro['unemployment_rate'], label='Unemployment Rate (%)', color='red', linewidth=2)
    plt.title('India Unemployment Rate Over Time (2019-2023)', fontsize=14)
    plt.xlabel('Date')
    plt.ylabel('Unemployment Rate (%)')
    plt.legend()
    plt.grid(alpha=0.3)
    plt.tight_layout()
    
    # Save plot
    plot_path = os.path.join(DATA_DIR, 'unemployment_plot.png')
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"✓ Unemployment plot saved to {plot_path}")
    plt.show()

if __name__ == "__main__":
    print("=" * 60)
    print("Adding Unemployment Rate to Macro Features")
    print("=" * 60)
    
    # Create data directory if it doesn't exist
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Process data
    macro = merge_macro_data()
    
    if macro is not None:
        # Show preview
        print("\n" + "="*40)
        print("PREVIEW - First 10 rows:")
        print("="*40)
        print(macro.head(10).to_string(index=False))
        
        print("\n" + "="*40)
        print("PREVIEW - Last 10 rows:")
        print("="*40)
        print(macro.tail(10).to_string(index=False))
        
        # Create visualization
        visualize_unemployment(macro)
        
        print("\n✅ SUCCESS! Unemployment rate added to macro features.")
        print("\nNext steps:")
        print("1. Update your macro_indicators database table")
        print("2. Run: python scripts/prepare_features.py")
        print("3. Update your model to include unemployment_rate feature")
    else:
        print("\n❌ FAILED to merge macro data. Please check file paths.")
