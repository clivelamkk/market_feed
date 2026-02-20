# Market Feed

A robust, modular data feed engine for financial markets. It normalizes real-time WebSocket data and HTTP snapshots from multiple exchanges and sources (e.g., Deribit, Bloomberg) into a unified format for trading applications.

## Key Benefits
- **Multi-Source Unification:** Seamlessly manage and aggregate data from different exchanges and data providers through a single, consistent interface.
- **High Performance:** Engineered for real-time trading systems, using a hybrid approach of initial REST snapshots for speed and persistent WebSockets for low-latency updates.
- **Resilient and Fault-Tolerant:** Automatically handles connections, disconnections, and reconnections, ensuring your application continues to receive data even in unstable network conditions.
- **Easy to Configure and Use:** Get started quickly with a simple configuration. The `FeedManager` handles the complexity of threading, data normalization, and state management.

## Documentation
- **[User Guide](USER_GUIDE.md):** Detailed documentation on all functions, methods, and configurations for end-users.
- **[Developer Guide](DEVELOPER_GUIDE.md):** In-depth explanation of the architecture, code structure, and process flows for developers and contributors.

## Installation

### Standard Installation
You can install the package directly from the GitHub repository:
```bash
pip install git+https://github.com/clivelamkk/market_feed.git
```

### For Local Development
If you plan to contribute to the project or need to make local modifications, clone the repository and install it in "editable" mode:
```bash
git clone https://github.com/clivelamkk/market_feed.git
cd market_feed
pip install -e .
```

### Updating the Package
To update to the latest version, use the `--upgrade` flag with pip:
```bash
pip install --upgrade git+https://github.com/clivelamkk/market_feed.git
```

## Optional Dependencies

### Bloomberg `blpapi`
To connect to Bloomberg as a data source, you must have the `blpapi` library installed. This is an optional dependency and is only required if you intend to use the Bloomberg adapter.

**Requirements:**
- A licensed Bloomberg Terminal with a valid login.
- The Bloomberg Desktop API must be installed and running.

**Installation:**
Install the `blpapi` library using the official Bloomberg repository URL:
```bash
pip install --index-url=https://blpapi.bloomberg.com/repository/releases/python/simple blpapi
```

## Basic Usage

```python
from market_feed import FeedManager

# 1. Initialize the Manager
# The manager can load API keys from a 'keys.json' file or from a dictionary.
feed = FeedManager(keys_path="keys.json")

# 2. Register the markets you want to track
# This example sets up a feed for BTC (coin-settled) from Deribit.
btc_config = {
    "register_name": "BTC_Deribit",
    "base_symbol": "BTC",
    "settlement": "coin",
    "source": "deribit"
}
feed.register_market(btc_config)

# If you have Bloomberg configured, you can also add a feed for an equity like SPY.
spy_config = {
    "register_name": "SPY_BBG",
    "base_symbol": "SPY",
    "settlement": "usd",
    "source": "bloomberg"
}
feed.register_market(spy_config)


# 3. Start the data stream
# This will initiate all connections in the background.
feed.start_stream()

# 4. Access normalized market data
# The get_snapshot() method provides a unified view of the market.
import time
time.sleep(5) # Allow some time for data to arrive
snapshot = feed.get_snapshot()

print("--- Index Prices ---")
for symbol, price in snapshot.index_prices.items():
    print(f"{symbol}: {price}")

print("\n--- Tickers ---")
for symbol, ticker_data in snapshot.tickers.items():
    print(f"{symbol}: Bid={ticker_data.get('best_bid_price', 'N/A')}, Ask={ticker_data.get('best_ask_price', 'N/A')}")

# 5. Stop the stream when done
feed.stop_stream()
```

## License
This project is licensed under the MIT License. See the [LICENSE.md](LICENSE.md) file for details.
