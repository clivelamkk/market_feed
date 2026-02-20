# Market Feed - User Guide

This guide provides a detailed overview of the `market-feed` library from a user's perspective. It covers the main components, methods, and data structures you will interact with.

## Core Component: `FeedManager`

The `FeedManager` is the central class for managing all data feed operations. It handles adapter initialization, data fetching, normalization, and state management.

### Initialization

```python
from market_feed import FeedManager

feed = FeedManager(keys_path="keys.json", api_keys=None, log_level=0)
```

**`__init__(self, keys_path="keys.json", api_keys=None, log_level=0)`**

- **Purpose:** Creates a new `FeedManager` instance.
- **Parameters:**
    - `keys_path` (str, optional): The file path to your `keys.json` file. This file should contain the API credentials for the exchanges you want to use. Defaults to `"keys.json"`.
    - `api_keys` (dict, optional): A dictionary containing API keys. If provided, it will be used instead of loading keys from `keys_path`. This is useful for environments where loading from files is not ideal. Defaults to `None`.
    - `log_level` (int, optional): Controls the verbosity of the feed's console output.
        - `0` (Default): No real-time ticker logging.
        - `1`: Log only primary tickers (e.g., spot, perpetuals).
        - `2`: Log all incoming tickers, including options.
- **`keys.json` Format:**
    The `keys.json` file allows you to specify multiple accounts per exchange. If an `account` is not specified when registering a market, the `"default"` account is used.
    ```json
    {
        "deribit": {
            "default": { "client_id": "...", "client_secret": "..." },
            "account_2": { "client_id": "...", "client_secret": "..." }
        },
        "bloomberg": {
            "default": {}
        }
    }
    ```

### Core Methods

**`register_market(self, feed_config: dict)`**

- **Purpose:** Registers a new market to be tracked. This is the primary method for defining your data requirements. The manager will automatically select or create the appropriate adapter based on the `source` specified.
- **Parameters:**
    - `feed_config` (dict): A dictionary defining the market feed.
- **`feed_config` Structure:**
    ```python
    {
        "register_name": "BTC_Deribit", # A unique name for this feed configuration.
        "base_symbol": "BTC",           # The base currency or asset (e.g., 'BTC', 'ETH', 'SPY').
        "settlement": "coin",           # The settlement type ('coin' or 'usd').
        "source": "deribit",            # The data source ('deribit', 'bloomberg', etc.).
        "account": "default"            # (Optional) The account to use from your keys.json.
    }
    ```

**`start_stream(self)`**

- **Purpose:** Starts all underlying adapter connections (WebSockets) in background threads. You must call this method to begin receiving real-time data.

**`stop_stream(self)`**

- **Purpose:** Stops all active data streams and closes all connections gracefully.

**`get_snapshot(self) -> MarketSnapshot`**

- **Purpose:** Returns a `MarketSnapshot` object, which is a thread-safe, unified representation of the current state of all registered markets.
- **Returns:** A `MarketSnapshot` data object.

**`subscribe_custom(self, source: str, tickers: List[str])`**

- **Purpose:** Manually subscribe to a list of specific tickers for a given data source. This is useful for dynamically adding subscriptions without registering a new market.
- **Parameters:**
    - `source` (str): The data source to use (e.g., `'deribit'`).
    - `tickers` (list): A list of instrument names to subscribe to (e.g., `['BTC-PERPETUAL', 'ETH-27FEB26-2000-C']`).

### Data Retrieval Methods

**`get_expiries_for(self, register_name: str) -> List[str]`**

- **Purpose:** Retrieves a sorted list of all unique option expiry dates available for a given registered market.
- **Parameters:**
    - `register_name` (str): The unique name of the market feed you registered.
- **Returns:** A list of expiry date strings (e.g., `['27FEB26', '27MAR26']`).

**`get_option_chain_details(self, register_name: str) -> List[dict]`**

- **Purpose:** Fetches a detailed and structured list of all available options for a given registered market.
- **Parameters:**
    - `register_name` (str): The unique name of the market feed.
- **Returns:** A list of dictionaries, where each dictionary represents a single option contract with the following structure:
    ```python
    {
        'symbol': 'BTC-27FEB26-67000-P', # The full instrument name
        'base_currency': 'BTC',
        'expiry': '27FEB26',
        'strike': 67000.0,
        'type': 'P', # 'C' for Call, 'P' for Put
        'raw': { ... } # The original, raw data from the exchange
    }
    ```

**`get_subscription_map(self, register_name, target_dates, min_pct, max_pct)`**

- **Purpose:** A powerful method to dynamically determine a set of option subscriptions based on price proximity to the current spot price and target expiries. It returns a structured map of the selected options and automatically subscribes to them if the feed is live.
- **Parameters:**
    - `register_name` (str): The market to analyze.
    - `target_dates` (List[str]): A list of expiry dates to include (e.g., `['27FEB26']`).
    - `min_pct` (float): The minimum strike price deviation from the spot price (e.g., `-5.0` for 5% below spot).
    - `max_pct` (float): The maximum strike price deviation from the spot price (e.g., `5.0` for 5% above spot).
- **Returns:** A dictionary structuring the selected options by expiry and strike for easy consumption.

## Data Structure: `MarketSnapshot`

The `MarketSnapshot` is a `dataclass` that provides a clean, normalized view of the market state.

```python
@dataclass
class MarketSnapshot:
    is_ready: bool
    index_prices: Dict[str, float]
    tickers: Dict[str, Any]
    config: List[Dict]
    instruments_by_undl: Dict[str, List[dict]]
```

- **`is_ready` (bool):** `True` if at least one adapter is connected and receiving data.
- **`index_prices` (Dict[str, float]):** A dictionary mapping primary reference tickers (e.g., spot, perpetuals) to their latest price.
    - Example: `{'BTC-PERPETUAL': 67000.50, 'SPY': 530.25}`
- **`tickers` (Dict[str, Any]):** The core data dictionary containing the latest information for every subscribed instrument. The key is the instrument name.
    - **Ticker Data Structure:**
        ```python
        {
            'instrument_name': 'BTC-27FEB26-67000-P',
            'best_bid_price': 1200.5,
            'best_bid_amount': 10.0,
            'best_ask_price': 1205.0,
            'best_ask_amount': 8.0,
            'last_price': 1202.0,
            'stats': { ... }, # Exchange-specific statistics (e.g., volume)
            'ts': 1677502800000 # Timestamp of the update
        }
        ```
- **`config` (List[Dict]):** A list of all the market configurations that have been registered.
- **`instruments_by_undl` (Dict[str, List[dict]]):** A dictionary mapping each `register_name` to the raw list of all instruments fetched for that market during initialization.
