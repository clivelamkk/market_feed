# Market Feed (Python)

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A multi-threaded, thread-safe market data aggregator for financial applications. It unifies data from cryptocurrency exchanges (Deribit) and traditional finance terminals (Bloomberg) into a normalized stream.

This library is intended for algorithmic trading systems and research tools that require centralized market prices.

## Key Features

*   **Unified API:** Treat Deribit Bitcoin options and SPY equity options consistently.
*   **Thread-Safe:** Safe to use in multi-threaded environments. Returns immutable snapshots.
*   **Hybrid Architecture:** Combines REST (for bootstrapping full option chains) and WebSockets (for updates).
*   **Resilient:** Handles reconnection for WebSocket feeds.
*   **Bloomberg Integration:** Support for `blpapi` with symbol mapping (e.g., `SPY-20FEB26-500-C` -> `SPY US 02/20/26 C500 Equity`).
*   **Note:** Bloomberg futures options support is currently experimental as underlying feed codes vary by expiry.

## Installation

### Standard Installation

Clone the repository and install the package in editable mode:

```bash
git clone https://github.com/market_feed/market_feed.git
cd market_feed
pip install -e .
```

Or install dependencies directly:

```bash
pip install -r requirements.txt
```

### Bloomberg Support (Optional)

If you intend to use the Bloomberg adapter, you must install the `blpapi` package. This requires a valid Bloomberg Terminal login and the Desktop API (DAPI) to be running on your machine.

**Note:** The standard `pip install blpapi` often fails because the package is hosted on Bloomberg's own repository. Use the following command:

```bash
pip install --index-url=https://blpapi.bloomberg.com/repository/releases/python/simple blpapi
```

## Configuration Files

The library relies on two optional configuration files in your project root:

### 1. `keys.json` (API Credentials)

Stores API keys for private feeds.

```json
{
  "deribit": {
    "default": {
      "client_id": "YOUR_ID",
      "client_secret": "YOUR_SECRET"
    }
  },
  "binance": {
    "default": {
        "client_id": "YOUR_KEY",
        "client_secret": "YOUR_SECRET"
    }
  }
}
```

### 2. `feed_instruments.csv` (Symbol Mapping)

Defines how internal symbols map to exchange-specific tickers. This allows you to normalize symbols across your application.

**Columns:** `Symbol`, `bloomberg`, `deribit` (and other adapters).

**Special Values (Prefixes):**
*   **`Exact:VALUE`**: **Literal Mapping.** Use this when you need to specify the exact ticker string for the adapter (ignoring default formatting).
    *   *Example:* `BTC` -> `Exact:BTC_USDC` (Deribit)
*   **`Index`**: **Bloomberg Index.** Tells the adapter to append " Index" to the symbol.
    *   *Example:* `SPX` -> `SPX Index`
*   **`FuturePrefix`**: **Bloomberg Future.** Treats the symbol as a generic future root. The adapter will handle expiration matching.
    *   *Example:* `ES` -> `ESZ3 Index`

**Example:**
```csv
Symbol,bloomberg,deribit
SPX,Index,
TENCENT,Exact:0700 HK Equity,
BTC,Exact:XBTUSD Curncy,Exact:BTC_USDC
```

## Quick Start

```python
import time
from market_feed import FeedManager

# 1. Initialize the Feed Manager
# keys_path: Path to your API credentials file
# instrument_config_path: Path to your symbol mapping CSV (moved/managed by you)
feed = FeedManager(
    keys_path="keys.json", 
    instrument_config_path="feed_instruments.csv", 
    log_level=1
)

# 2. Register Adapter & Underlying Interest
feed.register_adapter('deribit')
feed.register_market({
    'register_name': 'BTC_Options',
    'base_symbol': 'BTC',
    'source': 'deribit',
    'settlement': 'coin'
})

# 3. Subscribe to Reference Tickers (Manual Step)
# Unlike the old register_underlying, you must now explicitly tell the feed what to watch.
feed.subscribe_custom('deribit', ['BTC-PERPETUAL', 'BTC_USDC'])

# Optional: If you need the full option chain data immediately:
# feed.initialize_option_chain('BTC_Options')

# 4. Start the Feed
feed.start_stream()

try:
    while True:
        # 5. Get a Thread-Safe Snapshot
        snapshot = feed.get_snapshot()
        
        if snapshot.is_ready:
            btc_price = snapshot.index_prices.get('BTC-PERPETUAL', 0)
            print(f"Current BTC Price: {btc_price}")
            
            # Access normalized ticker data
            # snapshot.tickers is a dict keyed by instrument name
            # e.g., 'BTC-29DEC23-30000-C'
            
        time.sleep(1)
except KeyboardInterrupt:
    feed.stop_stream()
```

## Documentation

*   **[User Guide](USER_GUIDE.md):** Detailed documentation of all functions, configuration options, and data structures. Read this to learn how to use the library in your application.
*   **[Developer Guide](DEVELOPER_GUIDE.md):** Architecture overview, code breakdown, and guide for contributors who want to extend the library (e.g., adding a new exchange adapter).

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

[MIT](https://choosealicense.com/licenses/mit/)
