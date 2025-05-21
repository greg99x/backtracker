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
