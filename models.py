from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Tuple, List, Dict

@dataclass
class Event:
    event_id: str
    name: str
    start: str
    end: str
    status: str
    creation_date: Optional[str] = None
    closing_date: Optional[str] = None
    markets: Optional[List[str]] = None

    def __eq__(self, other):
        if not isinstance(other, Event):
            return NotImplemented
        return self.start == other.start and self.end == other.end

    def __hash__(self):
        return hash((self.start, self.end))

@dataclass
class OB:
    token_id: str
    market: str
    asset_id: str
    last_update: datetime
    bids: List[Dict]
    asks: List[Dict]

    @property
    def bb(self):
        return max(self.bids, key=lambda x: x["price"])["price"]

    @property
    def ba(self):
        return min(self.asks, key=lambda x: x["price"])["price"]

