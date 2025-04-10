import os 
import luigi 
import pandas as pd
import sqlite3
from .config import DB_PATH, PROCESSED_DATA_PATH
from .feature_engineering import FeatureEngineering

class StoreData(luigi.Task):
    """
    Task to store processed data in SQLite database
    """
    symbol = luigi.Parameter()
    date = luigi.DateParameter()
    
    def requires(self):
        return FeatureEngineering(symbol=self.symbol, date=self.date)
    
    def output(self):
        # Creating a marker file to indicate completion
        return luigi.LocalTarget(f"{PROCESSED_DATA_PATH}/{self.symbol}_{self.date}_stored.txt")
    
    def run(self):
        # Ensure DB directory exists
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        
        input_target = self.input()
        if isinstance(input_target, list):
            input_target = input_target[0]
            
        # Load processed data
        with input_target.open('r') as f:
            df = pd.read_csv(f)
        
        # Connect to SQLite database
        conn = sqlite3.connect(DB_PATH)
        
        # Prepare date column for SQLite
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Store data in SQLite
        table_name = 'stock_data'
        
        # Check if table exists, if not create it
        cursor = conn.cursor()
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Symbol TEXT,
            Date TEXT,
            Open REAL,
            High REAL,
            Low REAL,
            Close REAL,
            Volume INTEGER,
            Daily_Return REAL,
            MA5 REAL,
            Volatility_5d REAL,
            RSI_14 REAL,
            Volume_Change REAL,
            Day_of_Week INTEGER,
            Price_Range REAL,
            Price_Range_Pct REAL
        )
        ''')
        
        # Insert data
        df.to_sql(table_name, conn, if_exists='append', index=False)
        
        # Close connection
        conn.close()
        
        # Create a marker file to indicate successful storage
        with self.output().open('w') as f:
            f.write(f"Data for {self.symbol} on {self.date} stored successfully")
        
        print(f"Successfully stored data for {self.symbol} in the database")