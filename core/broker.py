# broker.py

from core.event import FillEvent
from core.core import EventQueue
import logging


class Broker:
    def __init__(self, event_queue, price_source, market_calendar, commission_perc=0.001, slippage_perc=0.0005, logger=None):
        """
        Simulated broker to execute orders.

        :param event_queue: Queue to send FillEvents back to
        :param price_source: Object with a `get__price(symbol,time)` method
        :param commission_perc: Proportional commission (e.g. 0.001 = 0.1%)
        :param slippage_perc: Proportional slippage (e.g. 0.0005 = 0.05%)
        :param market_calendar: object that can check if market is open at a given time
        """
        self.event_queue = event_queue
        self.price_source = price_source
        self.market_calendar = market_calendar
        self.commission_perc = commission_perc
        self.slippage_perc = slippage_perc
        self.logger = logger or logging.getLogger(__name__)
        self.pending_orders = EventQueue()
        self.logger.info('Broker created...')

    def handle_event(self, event, current_time):
        '''
        Processes 2 types of event:
        -MarketEvent: checks pending orders, and processes them
        -OrderEvent: checks if the market event, if open calls _fill_order, if not, puts in pending order
        -current_time is needed to check if market is open at the given time
        '''
        if event.type == 'ORDER':
            self._handle_order_event(event,current_time)

        elif event.type == 'MARKET':
            self._handle_market_event(event,current_time)
        else:
            self.logger.debug('Unknown order type in broker.process_order_chain')
            raise TypeError('Unknown order type in broker.process_order_chain')


    def _handle_order_event(self,order_event,current_time):
        symbol = order_event.symbol
        if not self.market_calendar.is_market_open(current_time,symbol):
            self.logger.info(f"OrderEvent: Market closed. Delaying order: {order_event} at {current_time}")
            self.pending_orders.put(order_event)
        else:
            self._fill_order(order_event, current_time)

    def _handle_market_event(self,market_event,current_time):
        requeue = []
        while not self.pending_orders.is_empty():
            order_event = self.pending_orders.get()
            symbol = order_event.symbol

            if self.market_calendar.is_market_open(current_time,symbol):
                self._fill_order(order_event, current_time)
            else:
                requeue.append(order_event)
                self.logger.info(f"MarketEvent: Market closed. Delaying order: {order_event} at {current_time}")
        for order_event in requeue:

            self.pending_orders.put(order_event)

    def _fill_order(self, order_event, current_time):
        if order_event.type != 'ORDER':
            self.logger.warning(f"Received non-order event in broker: {order_event}")
            return None

        symbol = order_event.symbol
        quantity = order_event.quantity
        direction = order_event.direction
        timestamp = current_time

        if quantity <= 0:
            self.logger.debug(f'Order quantity can not be negative or zero for {symbol} {timestamp} {direction}')
            return None
        if direction not in ('BUY','SELL'):
            self.logger.debug(f'Order event must be BUY or SELL but was {direction}')
            return None

        # Get current market price
        price = self.price_source.get_price(symbol, current_time)

        if price is None or type(price) != float:
            self.logger.debug(f"No price, or invalid price found for symbol: {symbol} {price} {type(price)}")
            return None

        # Apply slippage
        # For now, slippage is not included in the fill_price but treated as a separate fee
        slippage = self.slippage_perc * price
        fill_price = price # + slippage if direction == 'BUY' else price - slippage

        # Commission
        commission = self.commission_perc * quantity * fill_price

        fill_event = FillEvent(
            timestamp=timestamp,
            symbol=symbol,
            quantity=quantity,
            direction=direction,
            fill_price=fill_price,
            commission=commission,
            slippage=slippage,
        )

        self.event_queue.put(fill_event)
        self.logger.info(f'Filled order: {symbol},{quantity},{fill_price},{direction}')