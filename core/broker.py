# broker.py

from core.event import Event, MarketEvent, OrderEvent, SignalEvent, FillEvent
from core.core import EventQueue
import logging


class Broker:
    def __init__(self, event_queue, price_source, market_calendar, commission_perc=0.001, slippage_perc=0.0005, logger=None):
        """
        Simulated broker to ecute orders.

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

    def handle_event(self, event: Event) -> None:
        '''
        Listenst to the event broadcast of the core engine, and routes the appropriate events inside the module.
        '''
        if event.type == 'MARKET':
            self._handle_market_event(event)
        elif event.type == 'SIGNAL':
            return None
        elif event.type == 'FILL':
            return None
        elif event.type == 'ORDER':
           self._handle_order_event(event)

    def _handle_order_event(self,event: OrderEvent) -> None:
        if event.order_type != 'MARKET':
            self.logger.warning(f'Order type: {event.order_type} not supported.')
            return None
        try:
            quantity = float(event.quantity)
        except Exception as e:
            self.logger.warning(f'Order quantity must be castable to float: {event.quantity}')
            return None
        
        if event.quantity <= 0:
            self.logger.warning(f'Order quantity can not be negative or zero for {event.symbol}')
            return None
        
        if event.direction not in ('BUY','SELL'):
            self.logger.warning(f'Order event must be BUY or SELL but was {event.direction}')
            return None
        
        symbol = event.symbol
        current_time = event.timestamp
        if not self.market_calendar.is_market_open(current_time,symbol):
            self.logger.info(f"OrderEvent: Market closed. Delaying order: {event} at {current_time}")
            self.pending_orders.put(event)
        else:
            fill_event = self._fill_order(event, current_time)
            if fill_event is not None:
                self.event_queue.put(fill_event)

    def _handle_market_event(self,event: MarketEvent) -> None:
        current_time = event.timestamp
        requeue = []
        while not self.pending_orders.is_empty():
            order_event = self.pending_orders.get()
            symbol = order_event.symbol

            if self.market_calendar.is_market_open(current_time,symbol):
                fill_event = self._fill_order(order_event, current_time)
                if fill_event is not None:
                    self.event_queue.put(fill_event)
            else:
                requeue.append(order_event)
                self.logger.info(f"MarketEvent: Market closed. Delaying order: {order_event} at {current_time}")
        for order_event in requeue:
            self.pending_orders.put(order_event)

    def _fill_order(self, order_event, current_time):

        symbol = order_event.symbol
        quantity = order_event.quantity
        direction = order_event.direction
        timestamp = current_time

        # Get current market price
        price = self.price_source.get_price(symbol, current_time)
        try:
            price = float(price)
        except Exception as e:
            self.logger.debug(f"Price: {price} for order event {symbol} is not castable float")
            return None

        if price <= 0:
            self.logger.warning(f'Price for order {symbol} can not be zero or negative')
            return None

        if price is None:
            self.logger.warning(f"No price found for symbol: {symbol} {price} {type(price)}")
            return None

        # Apply slippage
        # For now, slippage is not included in the fill_price but treated as a separate fee
        slippage = self.slippage_perc * quantity * price
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
        return fill_event