class DataCollector:
    def __init__(self):
        self.portfolio_log = []
        self.position_log = []
        self.event_log = []
        self.fill_log = []

    def portfolio_snapshot(self, snapshot: dict) -> None:
        self.portfolio_log.append(snapshot)

    def position_snapshot(self, snapshot: dict) -> None:
        self.position_log.append(snapshot)

    def event_snapshot(self, snapshot: dict) -> None:
        self.event_log.append(snapshot)

    def fill_snapshot(self, snapshot: dict) -> None:
        self.fill_log.append(snapshot)

