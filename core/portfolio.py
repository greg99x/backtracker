import queue
from datetime import datetime, timezone
import logging
from core.event import OrderEvent
from core.position import Position

class Portfolio:
    def __init__(self, initial_cash, cash_reserve, event_queue, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.cash = initial_cash
        self.cash_reserve = cash_reserve
        self.event_queue = event_queue
        self.positions = {} #holder for instances of Position class
        self.current_prices = {}    # symbol -> latest price
        self.history = [] #snapshots from portfolio
        self.trade_log = []
        self.total_invested_value = 0.0
        self.timestamp = None # Last timestamp from event, not guaranteed to be up to date, handle with care!
        self.enable_snapshots = True
        self.enable_trade_log = True
        self.cumulated_slippage = 0.0
        self.cumulated_commission = 0.0

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

    def update_market(self, event):
        """
        Update the current prices and mark-to-market value of a single position
        based on a new market event.

        Parameters:
        - event: MarketEvent instance containing price and symbol info.
        """
        if event.type != 'MARKET' or not self._position_has_keys(event.symbol):
            return
            
        symbol = event.symbol
        price = event.price
        self.timestamp = event.timestamp

        # Store latest price
        self.current_prices[symbol] = price

        # Update position based on new event
        self.positions[symbol].update_position(event)

        # Update total market value
        self._update_total_market_value()

        # Create a snapshot if needed
        if self.enable_snapshots:
            self._record_snapshot()

    def _update_total_market_value(self):
        # Recalculate total market value
        self.total_invested_value = sum(
            pos.market_value(self.current_prices[sym])
            for sym, pos in self.positions.items()
            if sym in self.current_prices
        )

    def _update_cumulated_slippage(self,event):
        if not event.type == 'FILL':
            self.logger.debug('_update_cumulated_slippage received event with not type FILL')
            return
        self.cumulated_slippage += event.slippage

    def _update_cumulated_commission(self,event):
        if not event.type == 'FILL':
            self.logger.debug('_update_cumulated_slippage received event with not type FILL')
            return
        self.cumulated_commission += event.commission
    
    def _deduct_fee_from_cash(self,amount):
        if amount < 0:
            self.logger.debug('Fee amount can not be less then zero.')
            return
        self.cash -= amount
        if self.cash < 0:
            self.logger.debug('Cash is negative after fee deduction')

    def _position_has_keys(self, symbol):
        return symbol in self.positions
    
    def _record_snapshot(self):
        """
        Save a snapshot of the portfolio at a point in time.
        """
        snapshot = {
            'timestamp': self.timestamp,
            'cash': self.cash,
            'equity': self.total_invested_value,
            'positions': {}
        }

        for sym, pos in self.positions.items():
            if sym in self.current_prices:
                price = self.current_prices[sym]
            else:
                self.logger.info('Current price does not exist for snapshot yet')
                price = 0.0
            snapshot['positions'][sym] = {
                'quantity': pos.quantity,
                'avg_price': pos.avg_price,
                'market_value': pos.market_value(price),
                'unrealized PnL': pos.unrealized_pnl(price)
            }

        self.history.append(snapshot)

    def _update_trade_log(self, fill_event):

        trade = {
            'timestamp': fill_event.timestamp,
            'symbol': fill_event.symbol,
            'quantity': fill_event.quantity,
            'price': fill_event.fill_price,
            'currency': fill_event.currency,
            'side': fill_event.direction,
            'commission': fill_event.commission,
            'slippage': fill_event.slippage,
            'order_id': fill_event.order_id
        }
        self.trade_log.append(trade)

    def generate_order(self, event):
        """
        Translate a signal into a sized order.
        Parameters:
        - event: SignalEvent instance containing price and symbol info.
        Generates OrderEvent and puts it in the event_queue      
        """
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

        signal = OrderEvent(timestamp,symbol,order_type,quantity,direction)
        self._send_signal(signal)

    def _send_signal(self, signal_event):
        self.event_queue.put(signal_event)

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
                self.logger.debug(f'Price for ticker {event.symbol} is invalid')
                return None
            return free_cash/current_price
        
        elif event.signal_type == 'SELL':
            return pos.quantity
        else:
            self.logger.debug(f'Currently not implemented signal type {event.signal_type}')
            return None

    def update_fill(self, fill_event):
        """
        Apply a fill: update positions, cash, cumulated commission and slippage, 
        """
        self.timestamp = fill_event.timestamp
        symbol = fill_event.symbol
        
        # Check if position exists
        if not self._position_has_keys(symbol):
            self.logger.debug(f'Order filled for non existing position: {symbol}')
            return None
        
        # Let the position proccess the fill event
        fill_ok = self.positions[symbol].update_fill(fill_event)
        if fill_ok:
            self._deduct_fee_from_cash(fill_event.commission)
            self._deduct_fee_from_cash(fill_event.slippage)
            self._update_total_market_value()
            self._update_cumulated_commission(fill_event)
            self._update_cumulated_slippage(fill_event)
            
            if self.enable_snapshots:
                self._record_snapshot()

            if self.enable_trade_log:
                self._update_trade_log(fill_event)





