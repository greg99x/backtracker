import queue
from datetime import datetime, timezone
import logging
from core.event import OrderEvent
from core.position import Position
from core.event import Event, MarketEvent, OrderEvent, SignalEvent, FillEvent

class Portfolio:
    def __init__(self, initial_cash, cash_reserve, event_queue, logger=None, data_collector=None):
        self.logger = logger or logging.getLogger(__name__)
        self.cash = initial_cash
        self.cash_reserve = cash_reserve
        self.event_queue = event_queue
        self.data_collector = data_collector
        self.positions = {} #holder for instances of Position class
        self.current_prices = {}    # symbol -> latest price
        self.total_invested_value = 0.0
        self.timestamp = None # Last timestamp from event, not guaranteed to be up to date, handle with care!
        self.enable_snapshots = True
        self.enable_trade_log = True
        self.cumulated_slippage = 0.0
        self.cumulated_commission = 0.0

    def handle_event(self, event: Event) -> None:
        '''
        Listenst to the event broadcast of the core engine, and routes the appropriate events inside the module.
        '''
        if event.type == 'MARKET':
            self._handle_market_event(event)
        elif event.type == 'SIGNAL':
            self._handle_signal_event(event)
        elif event.type == 'FILL':
            self._handle_fill_event(event)
        elif event.type == 'ORDER':
            return None

    def _handle_market_event(self, event: MarketEvent) -> None:
        """
        Update the current prices and mark-to-market value of a single position
        based on a new market event.
        Consume: MarketEvent Emmit: None
        """
        if event.type != 'MARKET' or not self._position_has_keys(event.symbol):
            return
            
        symbol = event.symbol
        price = event.price
        self.timestamp = event.timestamp

        # Store latest price
        self.current_prices[symbol] = price

        # Update position based on new event
        #self.positions[symbol].update_position(event)

        # Update total market value
        self._update_total_market_value()

        # Create a snapshot if needed
        if self.enable_snapshots and self.data_collector is not None:
            self._record_portfolio_snapshot()
            self._record_positions_snapshot()


    def _handle_signal_event(self, event: SignalEvent) -> None:
        """
        Translate a signal into a sized order.
        Consume: SignalEvent Emmit: OrderEvent | None
     
        """
        #If portfolio has a sudden growth, recalculate the cash reserve so trading stays possible
        self._resize_cash_reserve()

        # return OrderEvent or None
        if event.type != 'SIGNAL' or not self._position_has_keys(event.symbol):
            return

        # check if trade should be executed
        quantity = self._decide_order_sizing(event)
        if not quantity:
            return 

        timestamp = event.timestamp
        symbol = event.symbol
        order_type = 'MARKET' # Expand on this later with different options
        direction = event.signal_type # BUY, SELL

        order = OrderEvent(timestamp,symbol,order_type,quantity,direction)
        self.event_queue.put(order)

    def _handle_fill_event(self, event: FillEvent) -> None:
        """
        Apply a fill: update positions, cash, cumulated commission and slippage, 
        Consume: FillEvent Emmit: None
        """
        symbol = event.symbol
        
        # Check if position exists
        if not self._position_has_keys(symbol):
            self.logger.warning(f'Order filled for non existing position: {symbol}')
            return None
        
        # Let the position proccess the fill event
        fill_ok = self.positions[symbol].update_fill(event)
        if fill_ok:
            self._deduct_order_value_from_cash(event.fill_price, event.quantity, event.direction)
            self._deduct_fee_from_cash(event.commission)
            self._deduct_fee_from_cash(event.slippage)
            self._update_total_market_value()
            self._update_cumulated_commission(event)
            self._update_cumulated_slippage(event)

            if self.enable_trade_log:
                self._update_trade_log(event)

    def create_new_position(self, symbol):
        '''
        Method creates new empty position for a given symbol
        Should be called before any Event is processed, otherwise the methods in core modules will not take action.
        Param: symbol
        Return: True if new position is created
                False if position already exists
        '''
        if not self._position_has_keys(symbol):
            self.positions[symbol] = Position(symbol,logger=self.logger)
            return True
        else:
            self.logger.warning(f'Position for {symbol} already exists')
            return False

    def _update_total_market_value(self):
        # Recalculate total market value
        self.total_invested_value = sum(
            pos.market_value(self.current_prices[sym])
            for sym, pos in self.positions.items()
            if sym in self.current_prices
        )

    def _update_cumulated_slippage(self,event):
        if not event.type == 'FILL':
            self.logger.warning('_update_cumulated_slippage received event with not type FILL')
            return
        self.cumulated_slippage += event.slippage

    def _update_cumulated_commission(self,event):
        if not event.type == 'FILL':
            self.logger.warning('_update_cumulated_slippage received event with not type FILL')
            return
        self.cumulated_commission += event.commission
    
    def _deduct_fee_from_cash(self,amount):
        if amount < 0:
            self.logger.warning('Fee amount can not be less then zero.')
            return
        self.cash -= amount
        if self.cash < 0:
            self.logger.warning('Cash is negative after fee deduction')

    def _position_has_keys(self, symbol):
        return symbol in self.positions
    
    def _record_portfolio_snapshot(self):
        """ Save a snapshot of the portfolio at a point in time."""
        snapshot = {
            'timestamp': self.timestamp,
            'cash': self.cash,
            'equity': self.total_invested_value}
        
        self.data_collector.portfolio_snapshot(snapshot)

    def _record_positions_snapshot(self):
        """ Save a snapshot of the positions at a point in time."""
        for _, pos in self.positions.items():
            snapshot = pos.snapshot()
            #Positions dont keep time, so it has to be added manually to log!
            snapshot['timestamp'] = self.timestamp
            self.data_collector.position_snapshot(snapshot)

    def _update_trade_log(self, fill_event):
        self.data_collector.fill_snapshot(fill_event.snapshot())

    def _resize_cash_reserve(self):
        self.cash_reserve = self.cash * 0.1
    
    def _decide_order_sizing(self,event):
        '''
        This is a dummy sizing strategy.
        If a BUY signal comes, it will stake the whole available cash
        If a SELL signal comes, it will close the whole position
        Returns None if trade should not be executed
        '''
        pos = self.positions[event.symbol]
        if event.signal_type == 'BUY':
            free_cash = self.cash - self.cash_reserve
            current_price = self.current_prices[event.symbol]
            if not current_price or current_price <= 0:
                self.logger.warning(f'Price for ticker {event.symbol} is invalid')
                return None
            return free_cash/current_price
        
        elif event.signal_type == 'SELL':
            return pos.quantity
        else:
            self.logger.warning(f'Currently not implemented signal type {event.signal_type}')
            return None

    def _deduct_order_value_from_cash(self,price,quantity,direction):
        if direction == 'BUY':
            self.cash -= price*quantity
        elif direction == 'SELL':
            self.cash += price*quantity