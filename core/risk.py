from abc import ABC, abstractmethod
from core.event import SignalEvent
import logging

class AbcRiskManager(ABC):
    '''
    Abstract class for defining risk management strategies.
    Primary task is to decide order size when portfolio generates OrderEvent
    '''
    def __init__(self,logger=None):
        self.name = self.__class__.__name__
        self.logger = logger or logging.getLogger(__name__)
    
    @abstractmethod
    def decide_order_sizing(self,portfolio_snapshot: dict, current_prices: dict,
                             positions: dict, event: SignalEvent) -> float:
        order_size = 10.0
        return order_size
    
class RiskManager(AbcRiskManager):
    def __init__(self,logger=None):
        super().__init__(logger)
        self.strategy_list = ['MAX','FIXED']
        self.strategy = 'MAX'
        self.fixed_amount = 10

    def decide_order_sizing(self,portfolio_snapshot: dict, current_prices: dict,
                             positions: dict, event: SignalEvent) -> float:
        if self.strategy == 'MAX':
            return self._max_amount(portfolio_snapshot,current_prices,positions,event)
        elif self.strategy == 'FIXED':
            return self._fixed_amount(portfolio_snapshot,current_prices,positions,event)

    def _fixed_amount(self,portfolio_snapshot: dict, current_prices: dict,
                             positions: dict, event: SignalEvent) -> float:        
        return self.fixed_amount
    
    def _max_amount(self,portfolio_snapshot: dict, current_prices: dict,
                             positions: dict, event: SignalEvent) -> float:
        '''
        This is a dummy sizing strategy.
        If a BUY signal comes, it will stake the whole available cash
        If a SELL signal comes, it will close the whole position
        Returns None if trade should not be executed
        '''
        cash = portfolio_snapshot['cash']
        cash_reserve = portfolio_snapshot['cash_reserve']
        pos = positions[event.symbol]
        if event.signal_type == 'BUY':
            free_cash = cash - cash_reserve
            current_price = current_prices[event.symbol]
            return free_cash/current_price
        
        elif event.signal_type == 'SELL':
            return pos.quantity
    
    def select_riskmodel(self,strategy: str) -> bool:
        if strategy not in self.strategy_list:
            return False
        
        self.strategy = strategy
        return True
