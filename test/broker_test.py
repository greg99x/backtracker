import unittest
from unittest.mock import Mock
from datetime import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.broker import Broker
from core.event import OrderEvent
import logging

# Setup logger to print to console
logger = logging.getLogger('logger')
logger.setLevel(logging.DEBUG)  # Set minimum level to DEBUG
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)  # Also handle DEBUG level logs
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

class TestBroker(unittest.TestCase):
    def setUp(self):
        self.event_queue = Mock()
        self.price_source = Mock()
        self.market_calendar = Mock()

        self.broker = Broker(
            event_queue=self.event_queue,
            price_source=self.price_source,
            market_calendar=self.market_calendar,
            commission_perc=0.01,   # 1%
            slippage_perc=0.005,     # 0.5%
            logger=logger
        )

        self.current_time = datetime(2024, 1, 1, 10, 0)
        self.order_event = OrderEvent(
            timestamp=self.current_time,
            symbol='AAPL',
            quantity=100,
            order_type='MARKET',
            direction='BUY'
        )

        self.order_event2 = OrderEvent(
            timestamp=self.current_time,
            symbol='BTC',
            quantity=100,
            order_type='MARKET',
            direction='BUY'
        )

    def test_order_rejected_with_negative_price(self):
        self.price_source.get_price.return_value = -0.001
        self.market_calendar.is_market_open.return_value = True
        self.broker.handle_event(self.order_event, self.current_time)
        self.assertEqual(self.broker.pending_orders.size(),0)
    def test_order_delayed_when_market_closed(self):
        self.market_calendar.is_market_open.return_value = False

        self.broker.handle_event(self.order_event, self.current_time)

        # Order should be added to pending
        self.assertEqual(self.broker.pending_orders.size(), 1)

    def test_order_filled_when_market_open(self):
        self.market_calendar.is_market_open.return_value = True
        self.price_source.get_price.return_value = 100.0

        self.broker.handle_event(self.order_event, self.current_time)

        # Check that a FillEvent was put in event_queue
        self.assertTrue(self.event_queue.put.called)
        fill_event = self.event_queue.put.call_args[0][0]
        self.assertEqual(fill_event.symbol, 'AAPL')
        self.assertEqual(fill_event.fill_price, 100.0)
        self.assertAlmostEqual(fill_event.commission, 100.0)  # 1% of 100 * 100
        self.assertAlmostEqual(fill_event.slippage, 0.5)      # 0.5% of 100

    def test_pending_order_filled_on_market_open(self):
        # First, delay the order
        self.market_calendar.is_market_open.return_value = False
        self.broker.handle_event(self.order_event, self.current_time)

        self.assertEqual(self.broker.pending_orders.size(), 1)

        # Now simulate market open
        self.market_calendar.is_market_open.return_value = True
        self.price_source.get_price.return_value = 200.0
        market_event = Mock()
        market_event.type = 'MARKET'

        # Should now fill the pending order
        self.broker.handle_event(market_event, self.current_time)
        self.assertEqual(self.broker.pending_orders.size(), 0)
        self.assertTrue(self.event_queue.put.called)


if __name__ == '__main__':
    unittest.main()
