from dataclasses import dataclass
from typing import Dict, List, Any
from abc import ABC, abstractmethod

@dataclass
class MarketSnapshot:
    """A unified view of the market state passed to the UI/Calc engines."""
    is_ready: bool
    index_prices: Dict[str, float]
    tickers: Dict[str, Any]
    config: List[Dict]
    instruments_by_undl: Dict[str, List[dict]]

class ExchangeAdapter(ABC):
    """
    The interface that ALL vendors (Deribit, Binance, etc.) must implement.
    """
    def __init__(self, manager, instrument_config_path=None):
        self.manager = manager
        self.instrument_config_path = instrument_config_path
        self.connected = False

    def is_alive(self):
        """Check if the underlying thread is running."""
        return hasattr(self, 'thread') and self.thread and self.thread.is_alive()

    @abstractmethod
    def start(self):
        """Start the WebSocket thread."""
        pass

    @abstractmethod
    def stop(self):
        """Close connections."""
        pass

    @abstractmethod
    def get_option_chain(self, undl_config) -> List[dict]:
        """
        Synchronously fetch instruments via HTTP for initialization.
        """
        pass

    @abstractmethod
    def get_latest_price(self, instrument_name: str) -> float:
        """
        Synchronously fetch a single price via HTTP (for bootstrapping spot/index).
        Returns 0.0 if failed.
        """
        pass

    @abstractmethod
    def subscribe(self, channels: List[str]):
        """Send a subscription command to the exchange."""
        pass