import os
import luigi
import pandas as pd
import numpy as np
from .config import PROCESSED_DATA_PATH
from .clean_validate import CleanValidateData

class FeatureEngineering(luigi.Task):
    symbol = luigi.Parameter()
    date = luigi.DateParameter()

    def requires(self):
        return CleanValidateData(symbol=self.symbol, date=self.date)
    
    def output(self):
        return luigi.LocalTarget(f"{PROCESSED_DATA_PATH}/{self.symbol}_{self.date}_features.csv")
    
    def run (self):
        input_target = self.input()
        if isinstance(input_target, list):
            input_target = input_target[0]
            
        # Load cleaned data
        with input_target.open('r') as f:
            df = pd.read_csv(f)

        df['Date'] = pd.to_datetime(df['Date'])

        df['Daily_Return'] = df['Close'].pct_change()

        df['MA5'] = df['Close'].rolling(window=5).mean()

        df['Volatility_5d'] = df['Close'].rolling(window=5).std()

        delta = df['Close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()
        
        rs = avg_gain / avg_loss
        df['RSI_14'] = 100 - (100 / (1 + rs))

        df['Volume_Change'] = df['Volume'].pct_change()

        df['Day_of_Week'] = df['Date'].dt.dayofweek

        df['Price_Range'] = df['High'] - df['Low']
        
        df['Price_Range_Pct'] = df['Price_Range'] / df['Open'] * 100

        df_features = df.dropna()
        
        with self.output().open('w') as f:
            df_features.to_csv(f, index=False)
        
        print(f"Successfully engineered features for {self.symbol}")

        

