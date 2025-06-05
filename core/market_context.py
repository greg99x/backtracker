import pandas as pd
from datetime import datetime
from core.event import Event
import logging

class MarketContext:
    '''
    Class for keeping latest current prices and timestamps from MarketEvent stream.
    Othet modules can access this latest data from MarketContext.
    This helps to separate the datastram from DataHandler, and ensures that the only
    point of contact with the core engine and the data_handler is the MarketEvent.
    Other classes eg. Broker, Portfolio can querry price from MarketContext
    '''
    def __init__(self):
        self.current_data = {}
        self.current_time = None
    

    def handle_event(self,event:Event) -> None:
        '''
        Listenst to the event broadcast of the core engine, and routes the appropriate events inside the module.
        '''
        if event.type == 'MARKET':
            self._handle_market_event(event)

    def _handle_market_event(self,event:Event) -> None:
        self.current_time = event.timestamp
        self.current_data[event.symbol] = {
            'Open':event.open,
            'High':event.high,
            'Low':event.low,
            'Close':event.close,
            'Volume':event.volume
        }

    def time(self) -> datetime:
        return self.current_time

    def price(self,symbol: str, mode='Close') -> float:
        if symbol not in self.current_data.keys():
            return None
        
        if mode == 'Close':
            return self.current_data[symbol]['Close']
        
        elif mode == 'Open':
            return self.current_data[symbol]['Open']
        
        elif mode == 'High':
            return self.current_data[symbol]['High']
        
        elif mode == 'Low':
            return self.current_data[symbol]['Low']
        
        else:
            self.logger.warning(f'MarketContext: unknown mode in price querry: {mode}')