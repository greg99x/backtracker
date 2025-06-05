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
from core.market_context import MarketContext
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
        self.close1 = 10
        # Open==Close==5
        self.open2 = 5
        self.high2 = 6
        self.low2 = 5
        self.close2 = 5
        self.days = 100
        #Buy on day2 sell on day3
        self.buy_price = 6
        self.sell_price = 9
        #Portfolio setup
        self.cash = 10000
        self.cash_reserve = 1000

        self.pattern_generator = PatternGenerator()
        self.pattern = self.pattern_generator.fixed_oscillating(
            'A',self.open2,self.high2,self.low2,self.close2,
            self.open1,self.high1,self.low1,self.close1,self.days)


        self.market_calendar = Mock()
        def mock_is_market_open(timestamp, symbol):
            return True
        
        self.market_calendar.is_market_open.side_effect = mock_is_market_open

        self.price_source = MarketContext()

        self.event_queue = EventQueue(logger=logger)

        self.data_collector = DataCollector()

        self.strategy = FixedPriceStrategy(self.event_queue,'A',buy_price=self.buy_price,
                                           sell_price=self.sell_price,logger=logger)

        self.portfolio = Portfolio(initial_cash=self.cash,price_source=self.price_source,cash_reserve=self.cash_reserve,
                                   event_queue=self.event_queue,logger=logger,data_collector=self.data_collector)

        self.datahandler = DataHandler(self.event_queue,logger=logger)

        self.broker = Broker(event_queue=self.event_queue,
                             price_source=self.price_source,
                             market_calendar=self.market_calendar,
                             commission_perc=0.0,
                             slippage_perc=0.0,
                             logger=logger)
        
        self.engine = BacktestEngine(event_queue=self.event_queue,
                                     data_handler=self.datahandler,
                                     strategy=self.strategy,
                                     broker=self.broker,
                                     portfolio=self.portfolio,
                                     logger=logger, 
                                     market_context=self.price_source,
                                     data_collector=self.data_collector)

    
    def test_run_no_fees(self):
        '''
        Test that calculated cash matches theoretical value
        Price of asset 'A' oscillates between OHLCV1 and OHLCV2 every day
        Assuming that order is filled at close prices, the spread in one trade is close2-close1
        If the simulation is run for n days (n//2)*spread*quantity can be made if fees are ignored
        '''
        self.assertEqual(self.days%2,0) # Check that trades can be closed, by checking days even
        # Fixed quantity that will be bought every trade
        buy_quantity = 10.0 
        # Spread that can be made with fixed price strategy in one buy-sell
        spread = self.close1-self.close2 
        rounds = self.days // 2
        gain = spread*rounds*buy_quantity

        starting_cash = self.portfolio.cash
        self.datahandler.write_symbol_data('A',self.pattern)
        self.datahandler.create_event_queue_lazy()
        self.portfolio.create_new_position('A')
        self.portfolio.select_risk_model('FIXED')
        self.portfolio.set_fixed_quantity(buy_quantity)
        self.broker.commission_perc = 0.0
        self.broker.slippage_perc = 0.0
        self.engine.run_backtest()
        closing_cash = self.portfolio.cash
        log1 = pd.DataFrame(self.data_collector.portfolio_log)
        log2 = pd.DataFrame(self.data_collector.fill_log)
        log3 = pd.DataFrame(self.data_collector.event_log)
        log4 = pd.DataFrame(self.data_collector.position_log)
        log1.to_csv('portfoliolog.csv')
        log2.to_csv('filllog.csv')
        log3.to_csv('eventlog.csv')
        log4.to_csv('positionlog.csv')
        logger.info(f'Theoretical gains: {gain}')
        logger.info(f'Realized gains: {closing_cash-starting_cash}')
        self.assertAlmostEqual(closing_cash-starting_cash,gain)


    def test_run_with_fees(self):
        '''
        Test that calculated cash matches theoretical value
        Price of asset 'A' oscillates between OHLCV1 and OHLCV2 every day
        Assuming that order is filled at close prices, the spread in one trade is close2-close1
        If the simulation is run for n days (n//2)*(spread-fees)*quantity can be made
        '''
        self.broker.commission_perc = 0.005
        self.broker.slippage_perc = 0.005

        self.assertEqual(self.days%2,0) # Check that trades can be closed, by checking days even
        # Fixed quantity that will be bought every trade
        buy_quantity = 10.0 
        # Spread that can be made with fixed price strategy in one buy-sell
        spread = self.close1-self.close2
        buy_side_fees = self.close2 * (self.broker.commission_perc + self.broker.slippage_perc)
        sell_side_fees = self.close1 * (self.broker.commission_perc + self.broker.slippage_perc)
        rounds = self.days // 2
        gain = (spread-sell_side_fees-buy_side_fees)*rounds*buy_quantity

        starting_cash = self.portfolio.cash
        self.datahandler.write_symbol_data('A',self.pattern)
        self.datahandler.create_event_queue_lazy()
        self.portfolio.create_new_position('A')
        self.portfolio.select_risk_model('FIXED')
        self.portfolio.set_fixed_quantity(buy_quantity)

        self.engine.run_backtest()
        closing_cash = self.portfolio.cash
        logger.info(f'Theoretical gains: {gain}')
        logger.info(f'Realized gains: {closing_cash-starting_cash}')
        self.assertAlmostEqual(closing_cash-starting_cash,gain)

    def test_run_with_multiple_fees(self):
        fee_range = [i*0.01 for i in range(10)]
        for i in fee_range:
            self.broker.commission_perc = i
            self.broker.slippage_perc = i

            self.assertEqual(self.days%2,0) # Check that trades can be closed, by checking days even
            # Fixed quantity that will be bought every trade
            buy_quantity = 10.0 
            # Spread that can be made with fixed price strategy in one buy-sell
            spread = self.close1-self.close2
            buy_side_fees = self.close2 * (self.broker.commission_perc + self.broker.slippage_perc)
            sell_side_fees = self.close1 * (self.broker.commission_perc + self.broker.slippage_perc)
            rounds = self.days // 2
            gain = (spread-sell_side_fees-buy_side_fees)*rounds*buy_quantity

            starting_cash = self.portfolio.cash
            self.datahandler.write_symbol_data('A',self.pattern)
            self.datahandler.create_event_queue_lazy()
            self.portfolio.create_new_position('A')
            self.portfolio.select_risk_model('FIXED')
            self.portfolio.set_fixed_quantity(buy_quantity)

            self.engine.run_backtest()
            closing_cash = self.portfolio.cash
            logger.info(f'Theoretical gains: {gain}')
            logger.info(f'Realized gains: {closing_cash-starting_cash}')
            self.assertAlmostEqual(closing_cash-starting_cash,gain)

if __name__ == '__main__':
    unittest.main()
