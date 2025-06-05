import unittest
from unittest.mock import Mock, MagicMock
from datetime import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.broker import Broker
from core.core import EventQueue
from core.event import OrderEvent
from core.market_context import MarketContext
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
        self.event_queue = EventQueue()
        self.market_calendar = Mock()
        self.price_source = MarketContext()
        self.price_source.price = MagicMock()

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


    def test_order_price(self):
        self.broker.logger.info('test_order_price')
        self.market_calendar.is_market_open.return_value = True
        self.price_source.price.return_value = -0.001
        self.broker.handle_event(self.order_event)
        self.assertEqual(self.broker.event_queue.size(),0)

        self.price_source.price.return_value = 0
        self.broker.handle_event(self.order_event)
        self.assertEqual(self.broker.event_queue.size(),0)

        self.price_source.price.return_value = None
        self.broker.handle_event(self.order_event)
        self.assertEqual(self.broker.event_queue.size(),0)

        self.price_source.price.return_value = 'price'
        self.broker.handle_event(self.order_event)
        self.assertEqual(self.broker.event_queue.size(),0)

        self.price_source.price.return_value = int(10)
        self.broker.handle_event(self.order_event)
        self.assertEqual(self.broker.event_queue.size(),1)

        self.price_source.price.return_value = float(10)
        self.broker.handle_event(self.order_event)
        self.assertEqual(self.broker.event_queue.size(),2)


    def test_order_direction(self):
        self.broker.logger.info('test_order_direction')
        self.price_source.price.return_value = 10
        self.market_calendar.is_market_open.return_value = True
        self.order_event.direction = 'LONG'
        self.broker.handle_event(self.order_event)
        self.assertEqual(self.broker.event_queue.size(),0)
        self.order_event.direction = 'SHORT'
        self.broker.handle_event(self.order_event)
        self.assertEqual(self.broker.event_queue.size(),0)
        self.order_event.direction = 'BUY'
        self.broker.handle_event(self.order_event)
        self.assertEqual(self.broker.event_queue.size(),1)
        self.order_event.direction = 'SELL'
        self.broker.handle_event(self.order_event)
        self.assertEqual(self.broker.event_queue.size(),2)

    def test_order_type(self):
        self.broker.logger.info('test_order_type')
        self.price_source.price.return_value = 10
        self.market_calendar.is_market_open.return_value = True

        self.order_event.order_type = 'LIMIT'
        self.broker.handle_event(self.order_event)
        self.assertEqual(self.broker.event_queue.size(),0)
        self.order_event.order_type = 'MARKET'
        self.broker.handle_event(self.order_event)
        self.assertEqual(self.broker.event_queue.size(),1)

    def test_order_quantity(self):
        self.broker.logger.info('test_order_quantity')
        self.price_source.price.return_value = 10
        self.market_calendar.is_market_open.return_value = True
        self.order_event.quantity = -1    
        self.broker.handle_event(self.order_event)
        self.assertEqual(self.broker.event_queue.size(),0)

        self.order_event.quantity = 0  
        self.broker.handle_event(self.order_event)
        self.assertEqual(self.broker.event_queue.size(),0)

        self.order_event.quantity = 'sdfsf'
        self.broker.handle_event(self.order_event)
        self.assertEqual(self.broker.event_queue.size(),0)

    def test_order_delayed_when_market_closed(self):
        self.broker.logger.info('test_order_delayed_when_market_closed')
        self.market_calendar.is_market_open.return_value = False

        self.broker.handle_event(self.order_event)

        # Order should be added to pending
        self.assertEqual(self.broker.pending_orders.size(), 1)

    def test_order_filled_when_market_open(self):
        self.broker.logger.info('test_order_filled_when_market_open')
        self.event_queue.put = MagicMock()

        self.market_calendar.is_market_open.return_value = True
        self.price_source.price.return_value = 100.0

        self.broker.handle_event(self.order_event)

        # Check that a FillEvent was put in event_queue
        self.assertTrue(self.event_queue.put.called)
        fill_event = self.event_queue.put.call_args[0][0]
        self.assertEqual(fill_event.symbol, 'AAPL')
        self.assertEqual(fill_event.fill_price, 100.0)
        self.assertAlmostEqual(fill_event.commission, 100.0)  # 1% of 100 * 100
        self.assertAlmostEqual(fill_event.slippage, 50)      # 0.5% of 100 * 100

    def test_pending_order_not_filled_on_closed_marketevent(self):
        self.broker.logger.info('test_pending_order_not_filled_on_closed_marketevent')
        self.event_queue.put = MagicMock()
        # First, delay the order
        self.market_calendar.is_market_open.return_value = False
        self.broker.handle_event(self.order_event)

        self.assertEqual(self.broker.pending_orders.size(), 1)
        # Send market event when market is still closed
        market_event = Mock()
        market_event.type = 'MARKET'
        market_event.timestamp = self.order_event.timestamp
        self.broker.handle_event(market_event)
        self.assertEqual(self.broker.pending_orders.size(), 1)

        # Now market opens, and order is sent
        self.market_calendar.is_market_open.return_value = True
        self.broker.handle_event(market_event)
        self.assertEqual(self.broker.pending_orders.size(),0)

    def test_pending_order_filled_on_market_open(self):
        self.broker.logger.info('test_pending_order_filled_on_market_open')
        self.event_queue.put = MagicMock()
        # First, delay the order
        self.market_calendar.is_market_open.return_value = False
        self.broker.handle_event(self.order_event)

        self.assertEqual(self.broker.pending_orders.size(), 1)

        # Now simulate market open
        self.market_calendar.is_market_open.return_value = True
        self.price_source.price.return_value = 200.0
        market_event = Mock()
        market_event.type = 'MARKET'
        market_event.timestamp = self.order_event.timestamp

        # Should now fill the pending order
        self.broker.handle_event(market_event)
        self.assertEqual(self.broker.pending_orders.size(), 0)
        self.assertTrue(self.event_queue.put.called)


if __name__ == '__main__':
    unittest.main()
