import unittest
from unittest.mock import patch, MagicMock, Mock
import pandas as pd
from datetime import datetime
import os
import sys
import logging
import yfinance as yf
from time import time
import cProfile

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

class SpeedTest:
    '''
    -----------------------------------------------------------------------------------------
    | This unit is working with a syntethic fixed pattern, to check engine core speed.
    | Date: 05.06.2025.
    | In this test, price1 and price2 are defined where:
    | price1_open = price1_close
    | price2_open = price2_close
    | price1 and price2 are repeated n times.
    | For a set of slippage, comission, invested amount, strategy
    | the engine must produce deterministic predictable results.
    -----------------------------------------------------------------------------------------
    '''

    def __init__(self):
        # Open==Close=10
        self.open1 = 10
        self.high1 = 11
        self.low1 = 9
        self.close1 = 10
        # Open==Close==5
        self.open2 = 5
        self.high2 = 6
        self.low2 = 5
        self.close2 = 5
        self.days = 10000 # ~270year of daily data
        #Buy on day2 sell on day3
        self.buy_price = 6
        self.sell_price = 9
        #Portfolio setup
        self.cash = 10000
        self.cash_reserve = 1000
        self.starting_cash = 0
        self.closing_cash = 0
        self.gain = 0

        self.pattern_generator = PatternGenerator()
        self.pattern = self.pattern_generator.fixed_oscillating(
            'A',self.open2,self.high2,self.low2,self.close2,
            self.open1,self.high1,self.low1,self.close1,self.days)


        self.market_calendar = Mock()
        def mock_is_market_open(timestamp, symbol):
            return True
        
        self.market_calendar.is_market_open.side_effect = mock_is_market_open

        self.event_queue = EventQueue(logger=logger)

        self.data_collector = DataCollector()

        self.strategy = FixedPriceStrategy(self.event_queue,'A',buy_price=self.buy_price,
                                           sell_price=self.sell_price,logger=logger)

        self.portfolio = Portfolio(initial_cash=self.cash,cash_reserve=self.cash_reserve,
                                   event_queue=self.event_queue,logger=logger,data_collector=self.data_collector)

        self.datahandler = DataHandler(self.event_queue,logger=logger)

        self.broker = Broker(event_queue=self.event_queue,
                             price_source=self.datahandler,
                             market_calendar=self.market_calendar,
                             commission_perc=0.0,
                             slippage_perc=0.0,
                             logger=logger)
        
        self.engine = BacktestEngine(event_queue=self.event_queue,
                                     data_handler=self.datahandler,
                                     strategy=self.strategy,
                                     broker=self.broker,
                                     portfolio=self.portfolio,
                                     logger=logger, data_collector=self.data_collector)

        self.portfolio.enable_snapshots = False
        self.portfolio.enable_trade_log = False
        self.engine.on_step = False
    
    def setup_run(self):
        '''
        Test that calculated cash matches theoretical value
        Price of asset 'A' oscillates between OHLCV1 and OHLCV2 every day
        Assuming that order is filled at close prices, the spread in one trade is close2-close1
        If the simulation is run for n days (n//2)*(spread-fees)*quantity can be made
        '''
        self.broker.commission_perc = 0.005
        self.broker.slippage_perc = 0.005

        # Fixed quantity that will be bought every trade
        buy_quantity = 10.0 
        # Spread that can be made with fixed price strategy in one buy-sell
        spread = self.close1-self.close2
        buy_side_fees = self.close2 * (self.broker.commission_perc + self.broker.slippage_perc)
        sell_side_fees = self.close1 * (self.broker.commission_perc + self.broker.slippage_perc)
        rounds = self.days // 2
        self.gain = (spread-sell_side_fees-buy_side_fees)*rounds*buy_quantity

        self.starting_cash = self.portfolio.cash
        self.datahandler.write_symbol_data('A',self.pattern)
        self.datahandler.create_event_queue_lazy()
        self.portfolio.create_new_position('A')
        self.portfolio.select_risk_model('FIXED')
        self.portfolio.set_fixed_quantity(buy_quantity)


    def run_engine(self):
        self.engine.run_backtest()
        self.closing_cash = self.portfolio.cash
        logger.info(f'Theoretical gains: {self.gain}')
        logger.info(f'Realized gains: {self.closing_cash-self.starting_cash}')

if __name__ == '__main__':
    test = SpeedTest()
    test.setup_run()
    cProfile.run('test.run_engine()', filename='profile.prof')

