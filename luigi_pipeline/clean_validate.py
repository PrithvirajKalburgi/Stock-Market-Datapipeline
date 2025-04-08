import os
import luigi
import pandas as pd
from .config import RAW_DATA_PATH, PROCESSED_DATA_PATH
from .fetch_stock_data import FetchStockData

class CleanValidateData(luigi.Task):
    symbol = luigi.Parameter()
    date = luigi.DateParameter()

    def required(self):
        return FetchStockData(symbol=self.symbol, date=self.date)
    
    def output(self):
        os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
        return luigi.LocalTarget(f"{PROCESSED_DATA_PATH}/{self.symbol}_{self.date}_cleaned.csv")
    
    def run(self):
        with self.input().open('r') as f:
            df = pd.read_csv(f)

        df_cleaned = df.dropna()

        df_cleaned['Date'] = pd.to_datetime(df_cleaned['Date'])

        numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in numeric_cols:
            if col in df_cleaned.columns:

                if col != 'Volume' :
                        df_cleaned = df_cleaned[df_cleaned[col] > 0]
                else:
                        df_cleaned = df_cleaned[df_cleaned[col] >= 0]
        
        df_cleaned['Symbol'] = self.symbol

        with self.output().open('w') as f:
             df_cleaned.to_csv(f, index=False)

        print(f"Successfully cleaned and validated data for {self.symbol}")

            
