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
    instruments_by_tab: Dict[str, List[dict]]

class ExchangeAdapter(ABC):
    """
    The interface that ALL vendors (Deribit, Binance, etc.) must implement.
    """
    def __init__(self, manager):
        self.manager = manager
        self.connected = False

    @abstractmethod
    def start(self):
        """Start the WebSocket thread."""
        pass

    @abstractmethod
    def stop(self):
        """Close connections."""
        pass

    @abstractmethod
    def get_option_chain(self, tab_config) -> List[dict]:
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

    @abstractmethod
    def get_reference_tickers(self, tab_config) -> List[str]:
        """
        Returns a list of symbols (Index, Spot, Perp) that serve as 
        the underlying price reference for a given tab configuration.
        Example: ['BTC-PERPETUAL', 'BTC_USDC']
        """
        pass