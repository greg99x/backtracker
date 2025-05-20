import queue
from datetime import datetime, timezone
import logging
from core.event import OrderEvent

class Portfolio:
    def __init__(self, initial_cash, cash_reserve, event_queue, symbols, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.cash = initial_cash
        self.cash_reserve = cash_reserve
        self.event_queue = event_queue
        self.positions = {sym: Position(sym) for sym in symbols} # symbol -> Position instance
        self.current_prices = {}    # symbol -> latest price
        self.history = []
        self.trade_log = []
        self.total_invested_value = 0.0
        self.timestamp = None # Last timestamp from event, handle with care!
        self.enable_snapshots = True
        self.enable_trade_log = True

    def update_market(self, event):
        """
        Update the current prices and mark-to-market value of a single position
        based on a new market event.

        Parameters:
        - event: MarketEvent instance containing price and symbol info.
        """
        if event.type != 'MARKET' or event.symbol not in self.positions.keys():
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
            price = self.current_prices[sym]
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
            'fee': fill_event.fee,
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
        if event.type != 'SIGNAL' or event.symbol not in self.positions.keys():
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
            return free_cash/self.current_prices
        
        elif event.signal_type == 'SELL':
            return pos.quantity
        else:
            self.logger.debug(f'Currently not implemented signal type {event.signal_type}')
            return None

    def update_fill(self, fill_event):
        """Apply a fill: update positions, cash, realized PnL."""
        # adjust position qty and cost basis
        # update cash and log trade
        self.timestamp = fill_event.timestamp
        symbol = fill_event.symbol
        quantity = fill_event.quantity
        direction = fill_event.direction  # 'BUY' or 'SELL'
        fill_price = fill_event.fill_price
        commission = fill_event.commission
        slippage = fill_event.slippage

    def final_report(self):
        """Compute final metrics, print or return results."""
        # e.g. CAGR, Sharpe, max drawdown



class Position:
    def __init__(self, symbol, quantity=0, avg_price=0.0):
        self.symbol = symbol
        self.quantity = 0.0          # Total number of shares/contracts held
        self.avg_cost = 0.0          # Average cost per unit
        self.realized_pnl = 0.0      # Realized profit/loss from closed trades

    def update_fill(self, fill_event):
        """
        Update the position based on a fill event.
        
        fill_event should have:
        - fill_event.quantity (positive for buy, negative for sell)
        - fill_event.fill_price
        - fill_event.commission (optional)
        """
        fill_qty = fill_event.quantity
        fill_price = fill_event.fill_price
        commission = getattr(fill_event, 'commission', 0.0)

        if fill_qty == 0:
            return  # No update for zero quantity fills

        # If adding to position (buy)
        if fill_qty > 0:
            total_cost = self.avg_cost * self.quantity + fill_price * fill_qty + commission
            self.quantity += fill_qty
            self.avg_cost = total_cost / self.quantity if self.quantity != 0 else 0.0

        # If reducing position (sell)
        else:
            qty_to_close = -fill_qty
            if qty_to_close > self.quantity:
                raise ValueError("Trying to sell more than held")
            # Realized PnL = (Sell price - avg cost) * qty sold - commission
            pnl = (fill_price - self.avg_cost) * qty_to_close - commission
            self.realized_pnl += pnl
            self.quantity -= qty_to_close
            if self.quantity == 0:
                self.avg_cost = 0.0

    def market_value(self, current_price):
        """Calculate the current market value of the position."""
        return self.quantity * current_price

    def unrealized_pnl(self, current_price):
        """Calculate unrealized PnL based on current price."""
        return (current_price - self.avg_cost) * self.quantity

    def __str__(self):
        return (f"Position(qty={self.quantity}, avg_cost={self.avg_cost:.2f}, "
                f"realized_pnl={self.realized_pnl:.2f})")
