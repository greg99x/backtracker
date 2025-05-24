import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime
import os
import sys
import logging

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
stream_handler.setLevel(logging.DEBUG)
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

if __name__ == '__main__':
    unittest.main(verbosity=2)
