# engine.py

import queue
from datetime import datetime, timezone
import logging

class BacktestEngine:
    def __init__(self, data_handler, strategy, broker, portfolio, logger=None):
        self.event_queue = EventQueue()
        self.data_handler = data_handler(event_queue=self.event_queue)
        self.strategy = strategy(event_queue=self.event_queue)
        self.broker = broker(event_queue=self.event_queue)
        self.portfolio = portfolio(event_queue=self.event_queue)
        self.logger = logger or logging.getLogger(__name__)
        self.on_step = None
        self.current_time = None
        self.running = True
        self.event_handlers = {
            'MARKET': self._handle_market_event,
            'SIGNAL': self._handle_signal_event,
            'ORDER': self._handle_order_event,
            'FILL': self._handle_fill_event
        }

    def run_backtest(self):
        """
        Runs the full event-driven backtest across all historical data.
        Handles all event types and updates strategy, portfolio, and broker accordingly.
        """
        self.logger.info("Starting backtest...")
        self.start_time = datetime.utcnow()

        try:
            while self.data_handler.has_next():
                # 1. Fetch the next market event
                market_event = self.data_handler.get_next_event()
                if market_event is None:
                    break

                self.current_time = market_event.timestamp
                self.event_queue.put(market_event)

                # 2. Process all events in the queue
                while not self.event_queue.is_empty():
                    event = self.event_queue.get()
                    handler = self.event_handlers.get(event.type)

                    if handler:
                        handler(event)
                    else:
                        self.logger.warning(f"Unknown event type: {event.type}")

                # 3. Optional per-step hooks (e.g. logging, risk checks)
                if self.on_step:
                    self.on_step(self.current_time, self)

        except Exception as e:
            self.logger.error(f"Backtest failed at {self.current_time}: {e}", exc_info=True)
            raise

        finally:
            self.end_time = datetime.now(timezone.utc)
            self.logger.info(f"Backtest completed in {(self.end_time - self.start_time).total_seconds():.2f}s")
            self.output_results()


        def _handle_market_event(self, event):
            # Strategy should look at market event and either create or not create a signal
            self.strategy.on_market_event(event)
            # Portfolio should update current account finances
            self.portfolio.update_market(event)
            # Broker should handle pending orders
            self.broker.handle_event(event,self.current_time)

        def _handle_signal_event(self, event):
            order = self.portfolio.generate_order(event)
            if order:
                self.event_queue.put(order)

        def _handle_order_event(self, event):
            self.broker.handle_event(event,self.current_time)

        def _handle_fill_event(self, event):
            self.portfolio.update_fill(event)



class EventQueue:
    def __init__(self):
        self._queue = queue.Queue()

    def put(self, event):
        """Add an event to the queue."""
        self._queue.put(event)

    def get(self):
        """Remove and return the next event from the queue.
        Returns None if the queue is empty."""
        try:
            return self._queue.get_nowait()
        except queue.Empty:
            return None

    def is_empty(self):
        """Return True if the queue is empty, False otherwise."""
        return self._queue.empty()

    def size(self):
        """Return the current size of the queue."""
        return self._queue.qsize()
