import os
import luigi
import yfinance as yf
import pandas as pd
from datetime import datetime
from .config import API_SOURCE, SYMBOLS, FETCH_PERIOD, RAW_DATA_PATH

class FetchStockData(luigi.Task):
    """
    Task to fetch stock data from yahoo finance (yfinance) API
    """

    symbol = luigi.Parameter()
    date = luigi.DateParameter(default=datetime.now().date())

    def output(self):
        os.makedirs(RAW_DATA_PATH, exist_ok=True)
        return luigi.LocalTarget(f"{RAW_DATA_PATH}/{self.symbol}_{self.date}.csv")
    
    def run(self):
        stock = yf.Ticker(self.symbol)
        data = stock.history(period=FETCH_PERIOD)

        data.reset_index(inplace=True)

        with self.output().open('w') as f:
            data.to_csv(f, index=False)

        print(f"Successfully fetched data for {self.symbol}")

        

