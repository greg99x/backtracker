import logging

class Position:
    def __init__(self, symbol, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.symbol = symbol
        self.quantity = 0.0          # Total number of shares/contracts held
        self.avg_cost = 0.0          # Average cost per unit
        self.realized_pnl = 0.0      # Realized profit/loss from closed trades
        self.cumulated_commission = 0.0
        self.cumulated_slippage = 0.0

    def update_fill(self, fill_event):
        """
        Update the position based on a fill event.
        
        fill_event should have:
        -fill_event.direction
        -fill_event.quantity
        -fill_event.fill_price
        -fill_event.commission
        -fill_event.slippage
        Return: False if update failed
                True if update succeded
        """
        direction = fill_event.direction
        fill_qty = fill_event.quantity
        fill_price = fill_event.fill_price
        commission = fill_event.commission
        slippage = fill_event.slippage

        if fill_qty == 0:
            return  # No update for zero quantity fills

        # If adding to position (buy)
        if direction == 'BUY':
            total_cost = self.avg_cost * self.quantity + fill_price * fill_qty + commission + slippage
            self.quantity += fill_qty
            self.avg_cost = total_cost / self.quantity if self.quantity != 0 else 0.0
            self.cumulated_commission += commission
            self.cumulated_slippage += slippage
            return True

        # If reducing position (sell)
        elif direction == 'SELL':
            qty_to_close = fill_qty
            if qty_to_close > self.quantity:
                self.logger.debug(f'Trying to sell more then held')
                return False
            # Realized PnL = (Sell price - avg cost) * qty sold - commission
            pnl = (fill_price - self.avg_cost) * qty_to_close - commission - slippage
            self.realized_pnl += pnl
            self.quantity -= qty_to_close
            self.cumulated_commission += commission
            self.cumulated_slippage += slippage
            if self.quantity == 0:
                self.avg_cost = 0.0
            return True
        else:
            self.logger.debug(f'Invalid direction in fill event')
            return False

    def market_value(self, current_price):
        """Calculate the current market value of the position."""
        return self.quantity * current_price

    def unrealized_pnl(self, current_price):
        """Calculate unrealized PnL based on current price."""
        return (current_price - self.avg_cost) * self.quantity

    def __str__(self):
        return (f"Position(symbol={self.symbol}, qty={self.quantity:.4f}, avg_cost={self.avg_cost:.2f}, "
                f"realized_pnl={self.realized_pnl:.2f})")
