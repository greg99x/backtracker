import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime
import os
import sys
import logging
from copy import deepcopy


# --- Add parent directory to path for importing DataStore ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.data_handler import DataStore

# --- Logger Setup ---
logger = logging.getLogger('logger')
logger.setLevel(logging.DEBUG)

# Clear any existing handlers
if logger.hasHandlers():
    logger.handlers.clear()

# Create and configure new stream handler to stdout
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.propagate = False  # Don't bubble up to root logger

logger.debug("Logger configured and ready.")

# --- Unit Tests ---
class TestDataStore(unittest.TestCase):
    def setUp(self):
        self.ds = DataStore(logger=logger)
        self.symbol = 'AAPL'
        self.test_csv = 'test_aapl.csv'
        self.mock_df = pd.DataFrame({
            'Open': [100, 101],
            'Close': [102, 103],
            'Volume': [1000, 1100]
        }, index=pd.to_datetime(['2024-01-01', '2024-01-02']))

    def tearDown(self):
        if os.path.exists(self.test_csv):
            os.remove(self.test_csv)

    @patch('yfinance.Ticker')
    def test_get_data_from_yf(self, mock_ticker_class):
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = self.mock_df
        mock_ticker_class.return_value = mock_ticker

        df = self.ds._get_data_from_yf(self.symbol, start_date='2024-01-01', end_date='2024-01-03')
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        logger.debug("Test get_data_from_yf passed.")

    @patch('yfinance.Ticker')
    def test_update_data_till_date(self, mock_ticker_class):
        self.ds.data[self.symbol] = self.mock_df.copy()

        mock_ticker = MagicMock()
        future_df = pd.DataFrame({
            'Open': [104],
            'Close': [105],
            'Volume': [1200]
        }, index=[pd.to_datetime('2024-01-03')])
        mock_ticker.history.return_value = future_df
        mock_ticker_class.return_value = mock_ticker

        result = self.ds.update_data(self.symbol, end_date=datetime(2024, 1, 3))
        self.assertTrue(result)

        updated = self.ds.data[self.symbol]
        self.assertEqual(len(updated), 3)
        self.assertIn(pd.to_datetime('2024-01-03'), updated.index)
        logger.debug("Test update_data_till_date passed.")
    
    def test_create_data_for_eventqueue(self):

        # Prepare sample data with proper OHLCV columns and datetime index
        dates = pd.to_datetime(['2023-01-01', '2023-01-03', '2023-01-02'])
        data = pd.DataFrame({
            'Date': dates,
            'Open': [100, 110, 105],
            'High': [105, 115, 110],
            'Low': [95, 105, 100],
            'Close': [102, 112, 108],
            'Volume': [1000, 1100, 1050]
        })

        data = data.set_index('Date')
        
        # Put this data into self.ds.data for a symbol
        self.ds.data['AAPL'] = data
        
        # Ensure data_for_market_event starts empty
        self.ds.data_for_market_event = pd.DataFrame(columns=[
            'Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'MarketEvent'
        ])
        self.ds.data_for_market_event = self.ds.data_for_market_event.set_index('Date')
        # Make deep copy of data before calling
        data_before = deepcopy(self.ds.data)
        
        # Call the method
        self.ds.create_data_for_eventqueue()
        
        # Check self.data was not modified
        for symbol in data_before:
            pd.testing.assert_frame_equal(self.ds.data[symbol], data_before[symbol])
        
        # Check data_for_market_event is not empty
        self.assertFalse(self.ds.data_for_market_event.empty)
        
        # Check columns exist and have correct dtype
        df = self.ds.data_for_market_event
        for col in ['Symbol', 'Open', 'High', 'Low', 'Close', 'Volume', 'MarketEvent']:
            self.assertIn(col, df.columns)
        
        # Check index is datetime
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df.index))
        
        # Check 'MarketEvent' column is int (or at least numeric)
        self.assertTrue(pd.api.types.is_numeric_dtype(df['MarketEvent']))
        
        # Check all rows have Symbol = 'AAPL'
        self.assertTrue((df['Symbol'] == 'AAPL').all())
        
        # Check index is sorted ascending
        self.assertTrue(df.index.is_monotonic_increasing)
        
        # Check the dates in index match those of original data
        original_dates = sorted(self.ds.data['AAPL'].index)
        new_dates = sorted(df.index.unique())
        self.assertEqual(original_dates, new_dates)

if __name__ == '__main__':
    unittest.main(verbosity=2)
