# base_strategy.py
from abc import ABC, abstractmethod
from core.event import Event, MarketEvent, OrderEvent, SignalEvent, FillEvent
import logging


class BaseStrategy(ABC):
    def __init__(self, event_queue, data_handler=None, logger=None):
        self.event_queue = event_queue
        self.data_handler = data_handler
        self.name = self.__class__.__name__
        self.logger = logger or logging.getLogger(__name__)

    @abstractmethod
    def handle_event(self, event: Event) -> None:
        """React to a market event and optionally generate signal(s)."""
        raise NotImplementedError("Subclasses must implement on_market_event")




class FixedPriceStrategy(BaseStrategy):
    def __init__(self, event_queue, symbol, buy_price, sell_price, data_handler=None, logger=None):
        """
        Parameters:
        - event_queue: Queue used to communicate with the rest of the engine.
        - data_handler: Optional access to historical data, if needed.
        """
        super().__init__(event_queue)
        self.symbol = symbol
        self.buy_price = buy_price
        self.sell_price = sell_price
        self.in_position = False  # Track if we're holding a position

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
            return None
        
    def _handle_market_event(self, event):
        if event.symbol != self.symbol:
            return None

        price = event.price  # Assuming event has a `price` attribute
        timestamp = event.timestamp

        if not self.in_position and price <= self.buy_price:
            signal = SignalEvent(timestamp, self.symbol, 'BUY')
            self.in_position = True
            self.logger.info(f"[{timestamp}] Buy signal triggered at {price}")
            self.event_queue.put(signal)

        elif self.in_position and price >= self.sell_price:
            signal = SignalEvent(timestamp, self.symbol, 'SELL')
            self.in_position = False
            self.logger.info(f"[{timestamp}] Sell signal triggered at {price}")
            self.event_queue.put(signal)
        
        return None


