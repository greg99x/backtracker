import unittest
from unittest.mock import patch, MagicMock, Mock
import pandas as pd
from datetime import datetime
import os
import sys
import logging

# --- Add parent directory to path for importing DataStore ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.data_handler import DataStore
from core.core import BacktestEngine
from core.broker import Broker
from core.portfolio import Portfolio
from core.position import Position
from core.strategy import FixedPriceStrategy
from core.core import EventQueue

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

class TestCore(unittest.TestCase):

    def setUp(self):
        self.market_calendar = Mock()

        def mock_is_market_open(timestamp, symbol):
            return True
        
        self.market_calendar.is_market_open.side_effect = mock_is_market_open

        self.event_queue = EventQueue()

        self.strategy = FixedPriceStrategy(self.event_queue,'BTC-USD',buy_price=30000.0,sell_price=45000.0,logger=logger)

        self.portfolio = Portfolio(initial_cash=11000.0,cash_reserve=1000.0,event_queue=self.event_queue,logger=logger)

        self.datahandler = DataStore(logger=logger)

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
                                     logger=logger)


    def test_data_handler_setupflow(self):
        #Check logger works.
        self.datahandler.logger.info('DataHandler logger works.')
        
        #Check frames shapes at initiation
        self.assertFalse(self.datahandler.data)
        self.assertEqual(self.datahandler.data_for_market_event.shape,(0,7))

        logger.info(f'Data_for_market_event:{self.datahandler.data_for_market_event}')
        logger.info(f'Data_for_market_event shape:{self.datahandler.data_for_market_event.shape}')
        logger.info(f'Data: {self.datahandler.data}')

        self.datahandler.read_csv('BTC-USD',r'C:\backtester\dev\btcusd.csv')

        logger.info(f'Data_for_market_event:{self.datahandler.data_for_market_event}')
        logger.info(f'Data_for_market_event shape:{self.datahandler.data_for_market_event.shape}')
        logger.info(f'Data: {self.datahandler.data}')

        
if __name__ == '__main__':
    unittest.main()
