import pandas as pd
import os
from datetime import datetime

def process_finsen_data():
    """
    Process all 4 FinSen CSV files and extract relevant financial headlines
    """
    finsen_folder = os.path.join(os.path.dirname(__file__), '..', 'data')  # Update with your actual folder path
    
    # Define your target stocks and related keywords
    indian_stocks = {
        'TCS': ['TCS', 'Tata Consultancy', 'Tata Consultancy Services'],
        'HDFC': ['HDFC', 'HDFC Bank', 'Housing Development Finance'],
        'BAJAJ': ['Bajaj', 'Bajaj Finance', 'Bajaj Finserv'],
        'ASIAN_PAINTS': ['Asian Paints', 'Asian Paint'],
        'LEMON_TREE': ['Lemon Tree', 'Lemon Tree Hotels'],
        'VARUN': ['Varun Beverages', 'VBL', 'Varun Bev']
    }
    
    # General financial keywords
    general_keywords = ['banking', 'finance', 'IT services', 'software', 'technology',
                       'stock market', 'equity', 'investment', 'trading', 'NSE', 'BSE',
                       'India', 'Indian', 'Mumbai', 'financial services', 'insurance']
    
    # All CSV files you have
    csv_files = [
        'Financial.csv',
        'Financial_Categorized.csv', 
        'Financial_Sentiment.csv',
        'Financial_Sentiment_Categorized.csv'
    ]
    
    all_processed_data = []
    
    print("🔄 Processing all FinSen CSV files...")
    
    for file_name in csv_files:
        file_path = os.path.join(finsen_folder, file_name)
        
        if not os.path.exists(file_path):
            print(f"⚠️  Warning: {file_name} not found, skipping...")
            continue
            
        print(f"\n📁 Processing {file_name}...")
        
        # Load the CSV file
        df = pd.read_csv(file_path)
        print(f"   Loaded {len(df)} rows")
        
        # Add source file identifier
        df['Source_File'] = file_name
        
        # Filter for relevant headlines
        relevant_rows = []
        
        for idx, row in df.iterrows():
            title = str(row['Title']).lower()
            content = str(row.get('Content', '')).lower()
            
            # Check for Indian stock mentions
            is_relevant = False
            matched_ticker = None
            
            # Check each stock
            for ticker, keywords in indian_stocks.items():
                for keyword in keywords:
                    if keyword.lower() in title or keyword.lower() in content:
                        is_relevant = True
                        matched_ticker = ticker
                        break
                if is_relevant:
                    break
            
            # Check for general financial keywords if no specific stock match
            if not is_relevant:
                for keyword in general_keywords:
                    if keyword.lower() in title or keyword.lower() in content:
                        is_relevant = True
                        matched_ticker = 'GENERAL_FINANCIAL'
                        break
            
            if is_relevant:
                row_data = row.copy()
                row_data['Matched_Ticker'] = matched_ticker
                row_data['Relevance_Score'] = calculate_relevance_score(title, content, indian_stocks)
                relevant_rows.append(row_data)
        
        if relevant_rows:
            file_df = pd.DataFrame(relevant_rows)
            all_processed_data.append(file_df)
            print(f"   ✅ Found {len(relevant_rows)} relevant headlines")
        else:
            print(f"   ❌ No relevant headlines found")
    
    if all_processed_data:
        # Combine all processed data
        combined_df = pd.concat(all_processed_data, ignore_index=True)
        
        # Remove duplicates based on title (since some files might have overlapping data)
        print(f"\n🔄 Removing duplicates from {len(combined_df)} total headlines...")
        combined_df = combined_df.drop_duplicates(subset=['Title'], keep='first')
        print(f"   After deduplication: {len(combined_df)} unique headlines")
        
        # Sort by relevance score
        combined_df = combined_df.sort_values('Relevance_Score', ascending=False)
        
        # Save processed data
        os.makedirs('data', exist_ok=True)
        output_file = 'data/finsen_processed.csv'
        combined_df.to_csv(output_file, index=False)
        
        print(f"\n✅ PROCESSING COMPLETE!")
        print(f"   Total relevant headlines: {len(combined_df)}")
        print(f"   Saved to: {output_file}")
        
        # Show breakdown by ticker
        print(f"\n📊 BREAKDOWN BY TICKER:")
        ticker_counts = combined_df['Matched_Ticker'].value_counts()
        for ticker, count in ticker_counts.items():
            print(f"   {ticker}: {count} headlines")
            
        return combined_df
    else:
        print("❌ No relevant data found in any files!")
        return None

def calculate_relevance_score(title, content, indian_stocks):
    """
    Calculate relevance score based on keyword matches and context
    """
    score = 0
    title_lower = title.lower()
    content_lower = content.lower()
    
    # Higher score for Indian stock mentions
    for ticker, keywords in indian_stocks.items():
        for keyword in keywords:
            if keyword.lower() in title_lower:
                score += 10  # Title mentions are more important
            elif keyword.lower() in content_lower:
                score += 5   # Content mentions
    
    # Additional points for financial context
    financial_terms = ['stock', 'share', 'market', 'trading', 'investment', 
                      'earnings', 'revenue', 'profit', 'loss', 'growth']
    
    for term in financial_terms:
        if term in title_lower:
            score += 2
        elif term in content_lower:
            score += 1
    
    return score

if __name__ == "__main__":
    processed_data = process_finsen_data()
    
    if processed_data is not None:
        print(f"\n🎯 SAMPLE PROCESSED HEADLINES:")
        print("="*60)
        for idx, row in processed_data.head(5).iterrows():
            print(f"{idx+1}. {row['Title']}")
            print(f"   Ticker: {row['Matched_Ticker']}")
            print(f"   Source: {row['Source_File']}")
            print(f"   Score: {row['Relevance_Score']}")
            print()
