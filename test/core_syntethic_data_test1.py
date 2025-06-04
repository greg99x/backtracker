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
from core.data_handler2 import DataHandler
from core.core import BacktestEngine
from core.broker import Broker
from core.portfolio import Portfolio
from core.position import Position
from core.strategy import FixedPriceStrategy
from core.core import EventQueue
from core.metrics import DataCollector
from utils.pattern_generator import PatternGenerator

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
    '''
    -----------------------------------------------------------------------------------------
    | This unit is working with a syntethic fixed pattern, to validate engine core operation.
    | Date: 04.06.2025.
    | In this test, price1 and price2 are defined where:
    | price1_open = price1_close
    | price2_open = price2_close
    | price1 and price2 are repeated n times.
    | For a set of slippage, comission, invested amount, strategy
    | the engine must produce deterministic predictable results.
    -----------------------------------------------------------------------------------------
    '''

    def setUp(self):
        # Open==Close=10
        self.open1 = 10
        self.high1 = 11
        self.low1 = 9
        self.close = 10
        # Open==Close==5
        self.open1 = 5
        self.high1 = 6
        self.low1 = 5
        self.close = 5
        self.days = 10
        #Buy on day2 sell on day3
        self.buy_price = 6
        self.sell_price = 9

        self.pattern_generator = PatternGenerator()
        pattern = self.pattern_generator.fixed_oscillating(
            'A',self.open1,self.high1,self.low1,self.close1,
            self.open2,self.high2,self.low2,self.close2,self.days)


        self.market_calendar = Mock()

        def mock_is_market_open(timestamp, symbol):
            return True
        
        
        self.market_calendar.is_market_open.side_effect = mock_is_market_open

        self.event_queue = EventQueue(logger=logger)

        self.data_collector = DataCollector()

        self.strategy = FixedPriceStrategy(self.event_queue,'A',buy_price=30000.0,sell_price=45000.0,logger=logger)

        self.portfolio = Portfolio(initial_cash=11000.0,cash_reserve=1000.0,event_queue=self.event_queue,logger=logger,data_collector=self.data_collector)

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

    
    def test_run_engine(self):
        self.datahandler.read_csv('BTC-USD',r'C:\backtester\dev\btcusd.csv')
        self.datahandler.create_event_queue_lazy()
        created = self.portfolio.create_new_position('BTC-USD')
        self.engine.run_backtest()
        log1 = pd.DataFrame(self.data_collector.portfolio_log)
        log2 = pd.DataFrame(self.data_collector.fill_log)
        log3 = pd.DataFrame(self.data_collector.event_log)
        log4 = pd.DataFrame(self.data_collector.position_log)
        log1.to_csv('log1.csv')
        log2.to_csv('log2.csv')
        log3.to_csv('log3.csv')
        log4.to_csv('log4.csv')
        
if __name__ == '__main__':
    unittest.main()
