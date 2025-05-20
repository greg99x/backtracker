class Portfolio:
    def __init__(self, initial_cash, event_queue, symbols):
        self.cash = initial_cash
        self.event_queue = event_queue
        self.positions = {sym: Position(sym) for sym in symbols} # symbol -> Position instance
        self.current_prices = {}    # symbol -> latest price
        self.history = []
        self.trade_log = []
        self.total_market_value = 0.0
        self.timestamp = None

    def update_market(self, market_event):
            """
            Update the current prices and mark-to-market value of all open positions
            based on a new market event.

            Parameters:
            - market_event: MarketEvent instance containing price and symbol info.
            """
            symbol = market_event.symbol
            price = market_event.price
            self.timestamp = market_event.timestamp

            if symbol in self.positions:
                # Store latest price
                self.current_prices[symbol] = price

            # Update position market value if we hold it
            if symbol in self.positions:
                position = self.positions[symbol]
                market_value = position.market_value(price)
                unrealized_pnl = position.unrealized_pnl(price)

                # (Optional) You can log or store these values somewhere
                print(f"[{self.timestamp}] {symbol} Price: {price:.2f}, "
                    f"Market Value: {market_value:.2f}, "
                    f"Unrealized PnL: {unrealized_pnl:.2f}")

            # Recalculate total market value
            self.total_market_value = sum(
                pos.market_value(self.current_prices[sym])
                for sym, pos in self.positions.items()
                if sym in self.current_prices
            )

    def record_snapshot(self, timestamp, market_data=None):
        """
        Save a snapshot of the portfolio at a point in time.

        market_data: dict mapping symbols to latest prices.
        """
        snapshot = {
            'timestamp': timestamp,
            'cash': self.cash,
            'equity': self.calculate_total_equity(market_data),
            'positions': {}
        }

        for symbol, pos in self.positions.items():
            market_price = market_data.get(symbol) if market_data else None
            snapshot['positions'][symbol] = pos.to_snapshot(market_price)

        self.history.append(snapshot)

    def update_fill(self, fill_event):

        trade = {
            'timestamp': fill_event.timestamp,
            'symbol': fill_event.symbol,
            'quantity': fill_event.quantity,
            'price': fill_event.fill_price,
            'side': fill_event.direction,
            'fee': fill_event.fee,
            'order_id': fill_event.order_id
        }
        self.trade_log.append(trade)

    def generate_order(self, signal_event):
        """Translate a signal into a sized order."""
        # risk checks, sizing logic
        # return OrderEvent or None

    def update_fill(self, fill_event):
        """Apply a fill: update positions, cash, realized PnL."""
        # adjust position qty and cost basis
        # update cash and log trade

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
