import mysql.connector
from mysql.connector import Error
import pandas as pd

class DatabaseManager:
    def __init__(self):
        self.connection = None
        # Database connection details
        self.host = 'localhost'
        self.user = 'root'
        self.password = 'hmrl1'
        
    def create_database(self):
        """Create the database first"""
        try:
            # Connect to MySQL server without specifying database
            connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password
            )
            
            if connection.is_connected():
                cursor = connection.cursor()
                cursor.execute("CREATE DATABASE IF NOT EXISTS stock_prediction")
                cursor.execute("SHOW DATABASES")
                databases = cursor.fetchall()
                
                print("✅ Database 'stock_prediction' created successfully!")
                print("📋 Available databases:")
                for db in databases:
                    print(f"   - {db[0]}")
                
                cursor.close()
                connection.close()
                return True
                
        except Error as e:
            print(f"❌ Error creating database: {e}")
            return False
        
    def connect(self):
        """Connect to the stock_prediction database"""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database='stock_prediction' 
            )
            
            if self.connection.is_connected():
                print("✅ Connected to stock_prediction database successfully!")
                return True
            
        except Error as e:
            print(f"❌ Error connecting to database: {e}")
            return False
    
    def create_tables(self):
        """Create all required tables"""
        if not self.connection or not self.connection.is_connected():
            print("❌ No database connection")
            return False
            
        cursor = self.connection.cursor()
        
        # Create stocks table
        stock_table = """
        CREATE TABLE IF NOT EXISTS stocks (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ticker VARCHAR(20) NOT NULL,
            date DATE NOT NULL,
            open_price DECIMAL(10,4),
            high_price DECIMAL(10,4),
            low_price DECIMAL(10,4),
            close_price DECIMAL(10,4),
            volume BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_stock_date (ticker, date)
        )
        """
        
        # Create sentiment table
        sentiment_table = """
        CREATE TABLE IF NOT EXISTS sentiment_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ticker VARCHAR(20) NOT NULL,
            date DATE NOT NULL,
            sentiment_score DECIMAL(5,4),
            tweet_count INT,
            news_count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_sentiment_date (ticker, date)
        )
        """
        
        # Create macro indicators table
        macro_table = """
        CREATE TABLE IF NOT EXISTS macro_indicators (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date DATE NOT NULL UNIQUE,
            usd_inr_rate DECIMAL(8,4),
            interest_rate DECIMAL(5,4),
            unemployment_rate DECIMAL(5,4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # Create events table
        events_table = """
        CREATE TABLE IF NOT EXISTS market_events (
            id INT AUTO_INCREMENT PRIMARY KEY,
            event_date DATE NOT NULL,
            event_type VARCHAR(50) NOT NULL,
            event_name VARCHAR(200),
            impact_window_start DATE,
            impact_window_end DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        try:
            # Execute each table creation
            cursor.execute(stock_table)
            print("✅ Created 'stocks' table")
            
            cursor.execute(sentiment_table)
            print("✅ Created 'sentiment_data' table")
            
            cursor.execute(macro_table)
            print("✅ Created 'macro_indicators' table")
            
            cursor.execute(events_table)
            print("✅ Created 'market_events' table")
            
            self.connection.commit()
            print("✅ All tables created successfully!")
            return True
            
        except Error as e:
            print(f"❌ Error creating tables: {e}")
            return False
        finally:
            cursor.close()

    def test_connection(self):
        """Test if everything is working"""
        if self.connection and self.connection.is_connected():
            cursor = self.connection.cursor()
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            
            print("\n📋 Created tables:")
            for table in tables:
                print(f"   - {table[0]}")
            
            cursor.close()
            return True
        return False

    def close_connection(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("✅ Database connection closed")

# Test the database setup
if __name__ == "__main__":
    print("🔧 Setting up database...")
    print("=" * 50)
    
    db = DatabaseManager()
    
    # Step 1: Create database
    if db.create_database():
        print("\n🔧 Connecting to database...")
        
        # Step 2: Connect to database
        if db.connect():
            print("\n🔧 Creating tables...")
            
            # Step 3: Create tables
            if db.create_tables():
                print("\n🧪 Testing connection...")
                
                # Step 4: Test everything
                db.test_connection()
                
                print("\n🎉 Database setup completed successfully!")
            else:
                print("❌ Failed to create tables")
        else:
            print("❌ Failed to connect to database")
    else:
        print("❌ Failed to create database")
    
    # Close connection
    db.close_connection()
