import os
import luigi
import pandas as pd
from .config import RAW_DATA_PATH, PROCESSED_DATA_PATH
from .fetch_stock_data import FetchStockData

class CleanValidateData(luigi.Task):
    symbol = luigi.Parameter()
    date = luigi.DateParameter()

    def requires(self):
        return FetchStockData(symbol=self.symbol, date=self.date)
    
    def output(self):
        os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
        return luigi.LocalTarget(f"{PROCESSED_DATA_PATH}/{self.symbol}_{self.date}_cleaned.csv")
    
    def run(self):
        # If input is a list, get the first item (assuming it's the only one)
        input_target = self.input()
        
        # Handle if input is a list or a single target
        if isinstance(input_target, list):
            input_target = input_target[0]
        
        # Load raw data
        with input_target.open('r') as f:
            df = pd.read_csv(f)
        
        # Clean and validate data
        # 1. Drop rows with missing values
        df_cleaned = df.dropna()
        
        # 2. Convert Date to datetime if it's not already
        df_cleaned['Date'] = pd.to_datetime(df_cleaned['Date'])
        
        # 3. Ensure all numeric columns have appropriate values
        numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in numeric_cols:
            if col in df_cleaned.columns:
                # Remove rows with negative prices (if any)
                if col != 'Volume':  # Prices should be positive
                    df_cleaned = df_cleaned[df_cleaned[col] > 0]
                else:  # Volume should be non-negative
                    df_cleaned = df_cleaned[df_cleaned[col] >= 0]
        
        # 4. Add a column with the stock symbol for identification
        df_cleaned['Symbol'] = self.symbol

        with self.output().open('w') as f:
             df_cleaned.to_csv(f, index=False)

        print(f"Successfully cleaned and validated data for {self.symbol}")

            
