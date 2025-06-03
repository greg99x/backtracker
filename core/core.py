# engine.py

from queue import LifoQueue
from datetime import datetime, timezone
from core.event import Event, MarketEvent, OrderEvent, SignalEvent, FillEvent
from core.metrics import DataCollector
import logging

class BacktestEngine:
    def __init__(self, event_queue, data_handler, strategy, 
                 broker, portfolio, data_collector, logger=None):
        self.event_queue = event_queue
        self.data_handler = data_handler
        self.strategy = strategy
        self.broker = broker
        self.portfolio = portfolio
        self.data_collector = data_collector
        self.logger = logger or logging.getLogger(__name__)
        self.on_step = True
        self.current_time = None
        self.running = True


    def run_backtest(self):
        """
        Runs the full event-driven backtest across all historical data.
        Handles all event types and updates strategy, portfolio, and broker accordingly.
        """
        self.logger.info("Starting backtest...")
        self.start_time = datetime.now(timezone.utc)

        try:
            while not self.event_queue.is_empty():
                # 2. Process event in the queue
                event = self.event_queue.get()
                self.broadcast(event)
                if self.on_step:
                    self.data_collector.event_snapshot(event.snapshot())

        except Exception as e:
            self.logger.error(f"Backtest failed at {self.current_time}: {e}", exc_info=True)
            raise

        finally:
            self.end_time = datetime.now(timezone.utc)
            self.logger.info(f"Backtest completed in {(self.end_time - self.start_time).total_seconds():.2f}s")

    def broadcast(self, event: Event) -> None:
        self.current_time = event.timestamp
        self.broker.handle_event(event)
        self.portfolio.handle_event(event)
        self.strategy.handle_event(event)


class EventQueue:
    def __init__(self,logger=None):
        self.logger = logger or logging.getLogger(__name__)    
        self._queue = LifoQueue()

    def put(self, event):
        """Add an event to the queue."""
        self._queue.put(event)

    def get(self):
        """Remove and return the next event from the queue.
        Returns None if the queue is empty."""
        try:
            event = self._queue.get()
            return event
        except self._queue.Empty:
            return None

    def is_empty(self):
        """Return True if the queue is empty, False otherwise."""
        return self._queue.empty()

    def size(self):
        """Return the current size of the queue."""
        return self._queue.qsize()
