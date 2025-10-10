# scripts/merge_news_data.py

import pandas as pd
import os

def merge_all_news_data():
    """
    Merge FinSen processed data with existing news CSV,
    reassign tickers, and save the combined dataset.
    """
    print("🔄 MERGING NEWS DATASETS...")
    
    # Load existing financial news (if available)
    existing_path = os.path.join('data', 'financial_news.csv')
    if os.path.exists(existing_path):
        existing_df = pd.read_csv(existing_path, parse_dates=['Date'], dayfirst=True)
        print(f"   Existing CSV: {len(existing_df)} headlines")
    else:
        print("   No existing financial_news.csv found, starting fresh")
        existing_df = pd.DataFrame(columns=['Date', 'Title', 'Description', 'Source', 'Ticker'])
    
    # Load processed FinSen data
    finsen_path = os.path.join('data', 'finsen_processed.csv')
    if not os.path.exists(finsen_path):
        print("   ❌ finsen_processed.csv not found!")
        return
    
    finsen_df = pd.read_csv(finsen_path)
    print(f"   FinSen processed: {len(finsen_df)} headlines")
    
    # Ensure Date column exists and is datetime
    if 'Date' in finsen_df.columns:
        finsen_df['Date'] = pd.to_datetime(finsen_df['Date'], errors='coerce')
    else:
        finsen_df['Date'] = pd.date_range(start='2023-01-01', periods=len(finsen_df), freq='h')
    
    # Standardize columns to match existing format
    finsen_processed = pd.DataFrame({
        'Date': finsen_df['Date'],
        'Title': finsen_df['Title'],
        'Description': finsen_df.get('Content', finsen_df['Title']),
        'Source': 'FinSen_Dataset',
        'Ticker': 'GENERAL'
    })
    
    # Combine datasets
    combined_df = pd.concat([existing_df, finsen_processed], ignore_index=True)
    print(f"   Before deduplication: {len(combined_df)} headlines")
    
    # Drop duplicate titles
    combined_df = combined_df.drop_duplicates(subset=['Title'], keep='first')
    print(f"   After deduplication: {len(combined_df)} headlines")
    
    # Force-convert Date to datetime and drop invalid
    combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce')
    invalid_dates = combined_df['Date'].isna().sum()
    if invalid_dates:
        print(f"⚠️  Dropping {invalid_dates} rows with invalid dates")
        combined_df = combined_df.dropna(subset=['Date'])
    
    # Fill NaNs in Title and Description and ensure string type
    combined_df['Title'] = combined_df['Title'].fillna('').astype(str)
    combined_df['Description'] = combined_df['Description'].fillna('').astype(str)
    
    # Reassign tickers based on content
    combined_df = reassign_tickers(combined_df)
    
    # Sort by Date
    combined_df = combined_df.sort_values('Date').reset_index(drop=True)
    
    # Save merged dataset
    os.makedirs('data', exist_ok=True)
    output_path = os.path.join('data', 'financial_news.csv')
    combined_df.to_csv(output_path, index=False)
    print(f"✅ MERGED DATASET SAVED: {len(combined_df)} total headlines")
    
    # Display breakdown by ticker
    print("\n📊 HEADLINES BY TICKER:")
    for ticker, count in combined_df['Ticker'].value_counts().items():
        print(f"   {ticker}: {count} headlines")
    
    return combined_df

def reassign_tickers(df):
    """
    Assign a specific stock ticker label to each headline
    based on keyword matching in Title and Description.
    """
    print("🎯 REASSIGNING TICKERS BASED ON CONTENT...")
    
    ticker_keywords = {
        'TCS': ['TCS', 'Tata Consultancy', 'IT services', 'software'],
        'HDFCBANK': ['HDFC Bank', 'banking', 'loan', 'deposit'],
        'BAJFINANCE': ['Bajaj Finance', 'Bajaj', 'NBFC', 'lending'],
        'ASIANPAINT': ['Asian Paints', 'paint', 'decorative'],
        'LEMONTREE': ['Lemon Tree', 'hotel', 'hospitality'],
        'VBL': ['Varun Beverages', 'beverages', 'FMCG']
    }
    
    general_financial = [
        'stock market', 'share price', 'equity', 'investment', 'trading',
        'earnings', 'profit', 'revenue', 'quarter', 'annual report'
    ]
    
    # Lowercase keywords for faster matching
    ticker_keywords = {t: [k.lower() for k in kws] for t, kws in ticker_keywords.items()}
    general_financial = [term.lower() for term in general_financial]
    
    def match_ticker(text):
        text = text.lower()
        best_ticker = 'GENERAL'
        max_hits = 0
        
        for ticker, kws in ticker_keywords.items():
            hits = sum(text.count(k) for k in kws)
            if hits > max_hits:
                max_hits = hits
                best_ticker = ticker
        
        if best_ticker == 'GENERAL':
            hits = sum(text.count(term) for term in general_financial)
            if hits > 0:
                best_ticker = 'FINANCIAL_GENERAL'
        
        return best_ticker
    
    # Combine Title and Description for matching
    combined_text = df['Title'] + ' ' + df['Description']
    df['Ticker'] = combined_text.apply(match_ticker)
    
    return df

if __name__ == "__main__":
    merged_data = merge_all_news_data()
    
    if merged_data is not None:
        print("\n🎯 SAMPLE HEADLINES BY TICKER:")
        print("=" * 60)
        for ticker in ['TCS', 'HDFCBANK', 'BAJFINANCE', 'ASIANPAINT', 'LEMONTREE', 'VBL']:
            sample = merged_data[merged_data['Ticker'] == ticker].head(2)
            if not sample.empty:
                print(f"\n{ticker}:")
                for _, row in sample.iterrows():
                    print(f"  • {row['Title']}")
