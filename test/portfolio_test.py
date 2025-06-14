import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import logging
from core.portfolio import Portfolio
import unittest
from unittest.mock import MagicMock, patch
from collections import namedtuple
from core.core import EventQueue
from core.event import Event, MarketEvent, OrderEvent, SignalEvent, FillEvent
from core.metrics import DataCollector
from core.market_context import MarketContext

# Setup logger to print to console
logger = logging.getLogger('logger')
logger.setLevel(logging.DEBUG)  # Set minimum level to DEBUG
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)  # Also handle DEBUG level logs
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Mock Position class to isolate Portfolio testing
class MockPosition:
    def __init__(self, symbol, logger=None):
        self.logger = logger
        self.symbol = symbol
        self.quantity = 10
        self.avg_cost = 100
        self.realized_pnl = 0
        self.update_position = MagicMock()
        self.update_fill = MagicMock(return_value=True)
        self.market_value = MagicMock(return_value=1000)
        self.unrealized_pnl = MagicMock(return_value=50)
        self.snapshot = MagicMock(return_value={})

class TestPortfolio(unittest.TestCase):
    def setUp(self):
        self.event_queue = EventQueue() # This is only valid as long as the queue is really only a queue!
        self.price_source = MagicMock()
        self.price_source.price = MarketContext()
        self.price_source.price = MagicMock()
        self.data_collector = DataCollector()
        self.portfolio = Portfolio(initial_cash=10000, price_source=self.price_source,
                                   cash_reserve=1000,
                                    event_queue=self.event_queue,logger=logger,
                                    data_collector=self.data_collector)
        
        # Patch Position class inside Portfolio to use our MockPosition
        patcher = patch('core.portfolio.Position', MockPosition)
        self.price_source.price.return_value = 10
        self.addCleanup(patcher.stop)
        self.mock_position_class = patcher.start()

    def test_create_new_position_success(self):
        self.portfolio.logger.info('test_create_new_position_success')
        created = self.portfolio.create_new_position('AAPL')
        self.assertTrue(created)
        self.assertIn('AAPL', self.portfolio.positions)
        self.assertIsInstance(self.portfolio.positions['AAPL'], MockPosition)

    def test_create_new_position_exists(self):
        self.portfolio.logger.info('test_create_new_position_exists')
        self.portfolio.positions['AAPL'] = MockPosition('AAPL')
        created = self.portfolio.create_new_position('AAPL')
        self.assertFalse(created)

    def test_filleventrejected(self):
        self.portfolio.logger.info('test_filleventrejected')
        self.portfolio.cash = 999
        self.portfolio.positions['AAPL'] = MockPosition('AAPL')
        fill = FillEvent(123,'AAPL',1,'BUY',1000,0,0)
        self.portfolio.handle_event(fill)
        reject = self.event_queue.get()
        self.assertEqual(reject.type,'FillDeclined')
        self.assertGreater(self.portfolio.cash,0)

    def test_filleventrejected2(self):
        self.portfolio.logger.info('test_filleventrejected2')
        self.portfolio.cash = 999
        self.portfolio.positions['AAPL'] = MockPosition('AAPL')
        fill = FillEvent(123,'AAPL',1,'BUY',999,10,0)
        self.portfolio.handle_event(fill)
        reject = self.event_queue.get()
        self.assertEqual(reject.type,'FillDeclined')
        self.assertGreater(self.portfolio.cash,0)

    def test_filleventrejected3(self):
        self.portfolio.logger.info('test_filleventrejected3')
        self.portfolio.cash = 999
        self.portfolio.positions['AAPL'] = MockPosition('AAPL')
        fill = FillEvent(123,'AAPL',1,'BUY',999,0,10)
        self.portfolio.handle_event(fill)
        reject = self.event_queue.get()
        self.assertEqual(reject.type,'FillDeclined')
        self.assertGreater(self.portfolio.cash,0)


    def test_filleventrejected4(self):
        self.portfolio.logger.info('test_filleventrejected4')
        self.portfolio.cash = 999
        self.portfolio.positions['AAPL'] = MockPosition('AAPL')
        fill = FillEvent(123,'AAPL',1,'BUY',999,5,5)
        self.portfolio.handle_event(fill)
        reject = self.event_queue.get()
        self.assertEqual(reject.type,'FillDeclined')
        self.assertGreater(self.portfolio.cash,0)

    def test_update_market_updates_prices_and_positions(self):
        self.portfolio.logger.info('test_update_market_updates_prices_and_positions')
        self.portfolio.positions['AAPL'] = MockPosition('AAPL')
        event = MarketEvent(symbol='AAPL', open=150, timestamp=123456789)
        
        self.portfolio.handle_event(event)
        self.assertGreaterEqual(self.portfolio.total_invested_value, 0)

    def test_generate_order_puts_order_event_in_queue(self):
        self.portfolio.logger.info('test_generate_order_puts_order_event_in_queue')
        self.portfolio.positions['AAPL'] = MockPosition('AAPL')
        self.portfolio.cash = 10000
        self.portfolio.cash_reserve = 0
        event = SignalEvent(symbol='AAPL', timestamp=123, signal_type='BUY')
        self.portfolio.handle_event(event)
        
        self.assertFalse(self.event_queue.is_empty())
        order_event = self.event_queue.get()
        self.assertEqual(order_event.symbol, 'AAPL')
        self.assertEqual(order_event.direction, 'BUY')

    def test_update_fill_updates_cash_and_position(self):
        self.portfolio.logger.info('test_update_fill_updates_cash_and_position')
        self.portfolio.enable_snapshots = True #Make sure snapshot will be created
        self.portfolio.positions['AAPL'] = MockPosition('AAPL')
        fill_event = FillEvent(
            symbol='AAPL', quantity=5, fill_price=105,
            commission=10, slippage=2, timestamp=123456, direction='BUY'
        )
        old_cash = self.portfolio.cash
        self.portfolio.handle_event(fill_event)

        # Confirm position.update_fill called
        self.portfolio.positions['AAPL'].update_fill.assert_called_with(fill_event)
        
        # Cash deducted by commission + slippage + fillprice*quantity
        self.assertEqual(self.portfolio.cash, old_cash - 10 - 2 - 5*105)
        
        # Cumulated values updated
        self.assertEqual(self.portfolio.cumulated_commission, 10)
        self.assertEqual(self.portfolio.cumulated_slippage, 2)

if __name__ == '__main__':
    unittest.main()
