# Market Feed

A robust, modular data feed engine for cryptocurrency markets. It normalizes real-time WebSocket data and HTTP snapshots from multiple exchanges (Deribit, etc.) into a unified format for trading applications.

## Features
- **Hybrid Data Fetching:** Uses HTTP for instant initialization and WebSockets for real-time updates.
- **Unified Interface:** normalized `MarketSnapshot` structure regardless of the underlying exchange.
- **Fault Tolerant:** Handles threading, locking, and reconnects internally.
- **Configurable:** Driven by simple JSON configuration files or direct injection.

## Installation

### For Development (Local)
Clone the repository and install it in "editable" mode so changes are reflected immediately:
```
git clone https://github.com/clivelamkk/market_feed.git
cd market_feed
pip install -e .
```

### For Production
Install directly from GitHub into any other project:
```
pip install git+https://github.com/clivelamkk/market_feed.git
```

### Usage
Basic Initialization

```
from market_feed import FeedManager

# Option 1: Pass paths to config files
feed = FeedManager(config_path="config/market_config.json", keys_path="config/keys.json")

# Option 2: Inject credentials directly (Secure)
my_keys = {"client_id": "...", "client_secret": "..."}
feed = FeedManager(config_path="market_config.json", api_keys=my_keys)

# Start the feed
feed.start_stream()

# Get Data
snapshot = feed.get_snapshot()
print(f"BTC Price: {snapshot.index_prices.get('BTC_USDC')}")
```

### Configuration
`market_config.json` Define which instruments to track.

```
[
    {
        "tab_name": "BTC",
        "base_symbol": "BTC",
        "settlement": "coin",
        "source": "deribit"
    }
]
```

### License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.