import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.portfolio import Portfolio
import unittest
from unittest.mock import MagicMock, patch
from collections import namedtuple
from queue import Queue

# Assume your Portfolio class is imported like this:
# from your_module import Portfolio

# Minimal mock event classes
MarketEvent = namedtuple('MarketEvent', ['type', 'symbol', 'price', 'timestamp'])
FillEvent = namedtuple('FillEvent', ['type', 'symbol', 'quantity', 'fill_price', 'commission', 'slippage', 'timestamp', 'direction', 'currency', 'order_id', 'fee'])
SignalEvent = namedtuple('SignalEvent', ['type', 'symbol', 'timestamp', 'signal_type'])

# Mock Position class to isolate Portfolio testing
class MockPosition:
    def __init__(self, symbol):
        self.symbol = symbol
        self.quantity = 10
        self.avg_price = 100
        self.realized_pnl = 0
        self.update_position = MagicMock()
        self.update_fill = MagicMock(return_value=True)
        self.market_value = MagicMock(return_value=1000)
        self.unrealized_pnl = MagicMock(return_value=50)

class TestPortfolio(unittest.TestCase):
    def setUp(self):
        self.event_queue = Queue() # This is only valid as long as the queue is really only a queue!
        self.portfolio = Portfolio(initial_cash=10000, cash_reserve=1000, event_queue=self.event_queue)
        
        # Patch Position class inside Portfolio to use our MockPosition
        patcher = patch('core.portfolio.Position', MockPosition)
        self.addCleanup(patcher.stop)
        self.mock_position_class = patcher.start()

    def test_create_new_position_success(self):
        created = self.portfolio.create_new_position('AAPL')
        self.assertTrue(created)
        self.assertIn('AAPL', self.portfolio.positions)
        self.assertIsInstance(self.portfolio.positions['AAPL'], MockPosition)

    def test_create_new_position_exists(self):
        self.portfolio.positions['AAPL'] = MockPosition('AAPL')
        created = self.portfolio.create_new_position('AAPL')
        self.assertFalse(created)

    '''
    def test_create_new_position_exists2(self):
        created = self.portfolio.create_new_position('AAPL')
        created = self.portfolio.create_new_position('AAPL')
        self.assertFalse(created)
    '''
    def test_update_market_updates_prices_and_positions(self):
        self.portfolio.positions['AAPL'] = MockPosition('AAPL')
        event = MarketEvent(type='MARKET', symbol='AAPL', price=150, timestamp=123456789)
        
        self.portfolio.update_market(event)
        self.assertEqual(self.portfolio.current_prices['AAPL'], 150)
        self.portfolio.positions['AAPL'].update_position.assert_called_with(event)
        self.assertGreaterEqual(self.portfolio.total_invested_value, 0)
        self.assertEqual(self.portfolio.timestamp, 123456789)
        self.assertTrue(len(self.portfolio.history) > 0)

    def test_generate_order_puts_order_event_in_queue(self):
        self.portfolio.positions['AAPL'] = MockPosition('AAPL')
        self.portfolio.current_prices['AAPL'] = 100
        self.portfolio.cash = 10000
        self.portfolio.cash_reserve = 0
        
        event = SignalEvent(type='SIGNAL', symbol='AAPL', timestamp=123, signal_type='BUY')
        self.portfolio.generate_order(event)
        
        self.assertFalse(self.event_queue.empty())
        order_event = self.event_queue.get()
        self.assertEqual(order_event.symbol, 'AAPL')
        self.assertEqual(order_event.direction, 'BUY')

    def test_update_fill_updates_cash_and_position(self):
        self.portfolio.positions['AAPL'] = MockPosition('AAPL')
        fill_event = FillEvent(
            type='FILL', symbol='AAPL', quantity=5, fill_price=105,
            commission=10, slippage=2, timestamp=123456, direction='BUY',
            currency='USD', order_id='order123', fee=12
        )
        old_cash = self.portfolio.cash
        self.portfolio.update_fill(fill_event)
        
        # Confirm position.update_fill called
        self.portfolio.positions['AAPL'].update_fill.assert_called_with(fill_event)
        
        # Cash deducted by commission + slippage
        self.assertEqual(self.portfolio.cash, old_cash - 10 - 2)
        
        # Cumulated values updated
        self.assertEqual(self.portfolio.cumulated_commission, 10)
        self.assertEqual(self.portfolio.cumulated_slippage, 2)

        # Snapshot and trade log recorded
        self.assertTrue(len(self.portfolio.history) > 0)
        self.assertTrue(len(self.portfolio.trade_log) > 0)

if __name__ == '__main__':
    unittest.main()
