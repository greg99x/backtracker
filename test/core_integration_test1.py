import unittest
from unittest.mock import patch, MagicMock, Mock
import pandas as pd
from datetime import datetime
import os
import sys
import logging
import yfinance as yf
from time import time

# --- Add parent directory to path for importing DataStore ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.data_handler import DataHandler
from core.core import BacktestEngine
from core.broker import Broker
from core.portfolio import Portfolio
from core.position import Position
from core.strategy import FixedPriceStrategy
from core.core import EventQueue
from core.metrics import DataCollector

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

class TestCore(unittest.TestCase):

    def setUp(self):
        self.market_calendar = Mock()

        def mock_is_market_open(timestamp, symbol):
            return True
        
        self.market_calendar.is_market_open.side_effect = mock_is_market_open

        self.event_queue = EventQueue(logger=logger)

        self.data_collector = DataCollector()

        self.strategy = FixedPriceStrategy(self.event_queue,'BTC-USD',buy_price=30000.0,sell_price=45000.0,logger=logger)

        self.portfolio = Portfolio(initial_cash=11000.0,cash_reserve=1000.0,event_queue=self.event_queue,logger=logger)

        self.datahandler = DataHandler(self.event_queue,logger=logger)

        self.broker = Broker(event_queue=self.event_queue,
                             price_source=self.datahandler,
                             market_calendar=self.market_calendar,
                             commission_perc=0.003,
                             slippage_perc=0.001,
                             logger=logger)
        
        self.engine = BacktestEngine(event_queue=self.event_queue,
                                     data_handler=self.datahandler,
                                     strategy=self.strategy,
                                     broker=self.broker,
                                     portfolio=self.portfolio,
                                     logger=logger, data_collector=self.data_collector)

    
    def test_data_handler_setupflow(self):
        #Check logger works.
        self.datahandler.logger.info('DataHandler logger works.')
        
        #Check frames shapes at initiation
        self.assertFalse(self.datahandler.datastore.data)
        logger.info(f'Data: {self.datahandler.datastore.data}')

        self.datahandler.read_csv('BTC-USD',r'C:\backtester\dev\btcusd.csv')

        # Test that after reading CSV that data_for_market_event is not modified
        # Test that data is read in self.data
        self.assertIn('BTC-USD',self.datahandler.datastore.data)
        self.assertGreater(self.datahandler.datastore.data['BTC-USD'].shape[0],0)
        self.assertEqual(self.datahandler.datastore.data['BTC-USD'].shape[1],8)

        #Test that wrote CSV is shame shape as self.data
        self.datahandler.write_csv('BTC-USD','test.csv')
        reread_data = pd.read_csv('test.csv',index_col=1)
        self.assertEqual(self.datahandler.datastore.data['BTC-USD'].shape,reread_data.shape)
        #os.remove('test.csv')
        self.datahandler.logger.info('test_data_handler_setupflow end')

    def test_data_handler_yfinance_download(self):
        #Only works if VPN is disabled        
        #Test that datastore is emptied when littered manually
        self.datahandler.datastore.data['AAPL'] = 1
        self.datahandler.clear_symbol_data('AAPL')
        self.assertTrue('AAPL' not in self.datahandler.datastore.get_symbol_list())
        # Test that corrent number of rows is downloaded
        self.datahandler.fetch_yf_data('AAPL',datetime(2024,12,1),datetime(2025,2,1),interval='1d')
        self.assertEqual(self.datahandler.datastore.get_all_symbol_data('AAPL').shape,(41,8))
        logger.info(self.datahandler.datastore.get_all_symbol_data('AAPL'))
        logger.info(self.datahandler.datastore.get_all_symbol_data('AAPL').shape)
        logger.info(self.datahandler.yfinterface.yfinance_objects)

    def test_data_handler_yfinance_download(self):
        #Only works if VPN is disabled        
        #Test that datastore is emptied when littered manually
        self.datahandler.datastore.data['AAPL'] = 1
        self.datahandler.clear_symbol_data('AAPL')
        self.assertTrue('AAPL' not in self.datahandler.datastore.get_symbol_list())
        # Test that when multiple downloads are done, shape remains
        for i in range(10):
            self.datahandler.fetch_yf_data('AAPL',datetime(2024,12,1),datetime(2025,2,1),interval='1d')
            self.assertEqual(self.datahandler.datastore.get_all_symbol_data('AAPL').shape,(41,8))

    def test_data_handler_populates_eventqueue(self):
        self.datahandler.read_csv('BTC-USD',r'C:\backtester\dev\btcusd.csv')
        self.datahandler.create_event_queue_lazy()
        self.assertEqual(self.event_queue.size(),self.datahandler.datastore.get_all_symbol_data('BTC-USD').shape[0])

if __name__ == '__main__':
    unittest.main()
