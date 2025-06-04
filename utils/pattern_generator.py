import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import datetime
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.data_handler import DataValidators
import mplfinance as mpf

class PatternGenerator:
    def __init__(self):
        self.pattern = pd.DataFrame()
        self.datavalidator = DataValidators()

    def fixed_oscillating(self,symbol,
                        open1, high1, low1, close1,
                        open2, high2, low2, close2,
                        n, volume=10, dividend=10, stocksplit=None):
        # Generate repeating OHLC patterns
        open = np.resize([float(open1), float(open2)], n)
        high = np.resize([float(high1), float(high2)], n)
        low = np.resize([float(low1), float(low2)], n)
        close = np.resize([float(close1), float(close2)], n)

        # Generate constant or repeated volume/dividend
        volume = np.full(n, volume)
        dividend = np.full(n, dividend)

        # Optional: handle stock splits if given as a repeating pattern
        if stocksplit is not None:
            stocksplit = np.resize(stocksplit, n)
        else:
            stocksplit = np.full(n, np.nan)

        # Generate date range
        dates = pd.date_range(start=datetime.datetime(2024, 1, 1), periods=n, freq='D')

        # Assemble into DataFrame
        df = pd.DataFrame({
            'Symbol':symbol,
            'Date': dates,
            'Open': open,
            'High': high,
            'Low': low,
            'Close': close,
            'Volume': volume,
            'Dividend': dividend,
            'StockSplit': stocksplit
        })
        df = df.set_index('Date')
        self.datavalidator.ohlcv_validate(df)

        return df

if __name__ == '__main__':
    generator = PatternGenerator()
    df = generator.fixed_oscillating('A',10,11,9,10,5,6,4,5,10)
    print(df)
    mpf.plot(df, type='candle', volume=True, style='yahoo', title='Synthetic OHLCV Data')

