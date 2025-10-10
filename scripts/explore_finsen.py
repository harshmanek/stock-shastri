import pandas as pd
import os

# Assuming you placed the FinSen files in a folder called 'finsen_data'
# Adjust the path to where you extracted the files
finsen_folder = './data/archive'  # Change this to your actual folder path

print("🔍 EXPLORING FINSEN DATASET FILES...\n")

# List all CSV files
csv_files = [f for f in os.listdir(finsen_folder) if f.endswith('.csv')]
print(f"Found {len(csv_files)} CSV files:")
for i, file in enumerate(csv_files, 1):
    print(f"{i}. {file}")

print("\n" + "="*60 + "\n")

# Explore each CSV file
for i, file in enumerate(csv_files, 1):
    file_path = os.path.join(finsen_folder, file)
    print(f"📊 FILE {i}: {file}")
    
    try:
        # Read first few rows to understand structure
        df = pd.read_csv(file_path, nrows=1000)  # Only first 1000 rows for exploration
        
        print(f"   Shape: {df.shape}")
        print(f"   Columns: {list(df.columns)}")
        
        # Check date column if exists
        date_cols = [col for col in df.columns if 'date' in col.lower()]
        if date_cols:
            df[date_cols[0]] = pd.to_datetime(df[date_cols[0]], errors='coerce')
            print(f"   Date range: {df[date_cols[0]].min()} to {df[date_cols[0]].max()}")
        
        # Check country column if exists
        country_cols = [col for col in df.columns if 'country' in col.lower()]
        if country_cols:
            countries = df[country_cols[0]].value_counts().head(5)
            print(f"   Top countries: {countries.to_dict()}")
        
        # Show sample headlines
        title_cols = [col for col in df.columns if any(word in col.lower() for word in ['title', 'headline', 'head'])]
        if title_cols:
            print(f"   Sample headlines:")
            for j, headline in enumerate(df[title_cols[0]].head(3), 1):
                print(f"     {j}. {headline}")
        
        print("\n" + "-"*50 + "\n")
        
    except Exception as e:
        print(f"   ❌ Error reading file: {e}\n")
