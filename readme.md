# Market Feed (Python)

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A high-performance, thread-safe market data aggregator for financial applications. It seamlessly unifies data from cryptocurrency exchanges (Deribit) and traditional finance terminals (Bloomberg) into a single, normalized stream.

This library is designed for algorithmic trading systems, quantitative research tools, and real-time dashboards that require a reliable "source of truth" for market prices.

## Key Features

*   **Unified API:** Treat Deribit Bitcoin options and SPY equity options exactly the same.
*   **Thread-Safe:** Safe to use in multi-threaded GUI or calculation engines. Returns immutable snapshots.
*   **Hybrid Architecture:** Combines REST (for bootstrapping full option chains) and WebSockets (for low-latency updates).
*   **Resilient:** Automatic reconnection handling for WebSocket feeds.
*   **Bloomberg Integration:** Native support for `blpapi` with intelligent symbol mapping (e.g., `SPY-20FEB26-500-C` -> `SPY US 02/20/26 C500 Equity`).

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

## Quick Start

```python
import time
from market_feed import FeedManager

# 1. Initialize the Feed Manager
# keys_path is optional if using Bloomberg or public data
feed = FeedManager(keys_path="keys.json", log_level=1)

# 2. Register a Market (e.g., Deribit BTC Options)
feed.register_market({
    'register_name': 'BTC_Options',
    'source': 'deribit',
    'base_symbol': 'BTC',
    'settlement': 'coin' # 'coin' (Inverse) or 'usd' (Linear)
})

# 3. Start the Feed
feed.start_stream()

try:
    while True:
        # 4. Get a Thread-Safe Snapshot
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
