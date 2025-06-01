# core/event.py

from datetime import datetime
import logging
from abc import ABC, abstractmethod

class Event(ABC):
    def __init__(self):
        self.type = 'GENERIC'

    @abstractmethod
    def __str__(self):
        return f"{self.__class__.__name__}"


class MarketEvent(Event):
    def __init__(self, timestamp, symbol, open, high=None, low=None, close=None, volume=None):
        super().__init__()
        self.type = 'MARKET'
        self.timestamp = timestamp
        self.symbol = symbol
        self.price = open
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

    def __str__(self):
        return f"MarketEvent({self.timestamp}, {self.symbol}, {self.price})"


class SignalEvent(Event):
    def __init__(self, timestamp, symbol, signal_type):
        super().__init__()
        self.type = 'SIGNAL'
        self.timestamp = timestamp
        self.symbol = symbol
        self.signal_type = signal_type  # 'BUY', 'SELL'

    def __str__(self):
        return f"SignalEvent({self.timestamp}, {self.symbol}, {self.signal_type})"


class OrderEvent(Event):
    def __init__(self, timestamp, symbol, order_type, quantity, direction):
        super().__init__()
        self.type = 'ORDER'
        self.timestamp = timestamp
        self.symbol = symbol
        self.order_type = order_type  # 'MARKET', 'LIMIT', etc.
        self.quantity = quantity
        self.direction = direction    # 'BUY' or 'SELL'

    def __str__(self):
        return (f"OrderEvent({self.timestamp}, {self.symbol}, "
                f"{self.order_type}, {self.quantity}, {self.direction})")


class FillEvent(Event):
    def __init__(self, timestamp, symbol, quantity, direction, fill_price, commission=0.0, slippage=0.0):
        super().__init__()
        self.type = 'FILL'
        self.timestamp = timestamp 
        self.symbol = symbol
        self.quantity = quantity # should be positive for both sell and buy
        self.direction = direction  # 'BUY' or 'SELL'
        self.fill_price = fill_price 
        self.commission = commission # should be positive, and added to total cost
        self.slippage = slippage # should be positive, and added to total cost

    def __str__(self):
        total_price = self.quantity * self.fill_price
        total_cost = total_price + self.commission + self.slippage
        return (
            f"FillEvent({self.timestamp}, {self.symbol}, Quantity: {self.quantity}, "
            f"Direction: {self.direction}, Price: {self.fill_price}, "
            f"Commission: {self.commission}, Slippage: {self.slippage}, "
            f"Total Price: {total_price:.2f}, Total Cost: {total_cost:.2f})"
        )

