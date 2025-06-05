import unittest
from unittest.mock import Mock, MagicMock
from datetime import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from core.broker import Broker
from core.core import EventQueue
from core.market_context import MarketContext
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
        self.event_queue = EventQueue()
        self.price_source = MarketContext()
        self.price_source.price = MagicMock()
        self.market_calendar = Mock()
        self.event_queue.put = MagicMock()


        def mock_is_market_open(timestamp, symbol):
            if symbol == 'BTC':
                return timestamp.hour >= 9
            elif symbol == 'AAPL':
                return timestamp.hour >= 10

        self.market_calendar.is_market_open.side_effect = mock_is_market_open


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


    def test_two_orders_in_cue(self):
        self.broker.logger.info('test_two_orders_in_cue')
        market_event = Mock()
        market_event.type = 'MARKET'
        #set time before market open
        self.order_event.timestamp = datetime(2024, 1, 1, 8, 0)
        self.order_event2.timestamp = datetime(2024, 1, 1, 8, 0)
        self.price_source.price.return_value = 1.0

        #push 2 orders with different market opens
        self.broker.handle_event(self.order_event)
        self.assertEqual(self.broker.pending_orders.size(), 1)
        self.broker.handle_event(self.order_event2)
        self.assertEqual(self.broker.pending_orders.size(),2)

    
    def test_two_orders_with_only_one_open_market(self):
        self.broker.logger.info('test_two_orders_with_only_one_open_market')
        self.price_source.price.return_value = 1.0
        self.order_event.timestamp = datetime(2024, 1, 1, 8, 0) # just for clarity
        self.order_event2.timestamp = datetime(2024, 1, 1, 8, 0) # just for clarity

        
        #push 2 orders with different market opening hours
        self.broker.handle_event(self.order_event)
        self.assertEqual(self.broker.pending_orders.size(), 1)
        self.broker.handle_event(self.order_event2)
        self.assertEqual(self.broker.pending_orders.size(),2)

        #set time so only 1 market is open
        market_event = Mock()
        market_event.type = 'MARKET'
        market_event.timestamp = datetime(2024, 1, 1, 9, 0)
        self.broker.handle_event(market_event)
        self.assertEqual(self.broker.pending_orders.size(), 1)
        fill_event = self.event_queue.put.call_args[0][0]
        self.assertEqual(fill_event.symbol, 'BTC')

        #set time so both market are open
        market_event = Mock()
        market_event.type = 'MARKET'
        market_event.timestamp = datetime(2024, 1, 1, 10, 0)
        self.broker.handle_event(market_event)
        self.assertEqual(self.broker.pending_orders.size(), 0)
        fill_event = self.event_queue.put.call_args[0][0]
        self.assertEqual(fill_event.symbol, 'AAPL') 
    

if __name__ == '__main__':
    unittest.main()
