# Market Feed - User Guide

This guide provides a detailed reference for the public API of the `market_feed` library. It covers how to configure feeds, interpret data, and manage the feed lifecycle.

## Table of Contents
1.  [The FeedManager Class](#1-the-feedmanager-class)
2.  [Configuration Dictionaries](#2-configuration-dictionaries)
3.  [Data Structures (The Snapshot)](#3-data-structures-the-snapshot)
4.  [Advanced Usage](#4-advanced-usage)

---

## 1. The FeedManager Class

The `FeedManager` is your primary interface. You should typically instantiate one manager for your entire application.

### Initialization

```python
feed = FeedManager(keys_path="keys.json", api_keys=None, log_level=0)
```

*   **`keys_path` (str, optional):** Path to a JSON file containing API credentials. Default is `"keys.json"`.
*   **`api_keys` (dict, optional):** A dictionary of API keys to override the file.
    *   *Structure:* `{"deribit": {"account_name": {"client_id": "...", "client_secret": "..."}}}`
*   **`log_level` (int):** Controls verbosity.
    *   `0`: Silent (Errors only).
    *   `1`: Info (Bootstrapping, Connection status).
    *   `2`: Debug (Logs all incoming ticker updates to `feed_debug.log`).

### Core Methods

#### `register_market(feed_config: dict)`

Tells the manager to prepare a connection for a specific market sector (e.g., "BTC Options on Deribit").

*   **Behavior:**
    1.  Validates the configuration.
    2.  Initializes the appropriate adapter (Deribit, Bloomberg, etc.) if not already active.
    3.  **Bootstraps Data:** Synchronously fetches the full list of instruments (Option Chain) and initial reference prices via HTTP. This ensures you have a complete "map" of the market before the first tick arrives.
    4.  If `start_stream()` was already called, the new adapter starts immediately.
*   **Arguments:** `feed_config` (See [Section 2](#2-configuration-dictionaries)).

#### `start_stream()`

Starts all registered adapters in background threads. They will begin connecting to WebSockets and streaming data.

#### `stop_stream()`

Gracefully shuts down all adapter threads and closes network connections.

#### `get_snapshot() -> MarketSnapshot`

Returns a **thread-safe, immutable copy** of the current market state.

*   **Why use this?** In a multi-threaded environment (like a UI or a trading engine), reading a dictionary while another thread writes to it causes crashes. The snapshot guarantees consistency.
*   **Performance:** This operation involves a memory copy. It is fast enough for 10-50Hz loops, but avoid calling it in a tight `while True` loop without a `time.sleep()`.

#### `subscribe_custom(source: str, tickers: List[str])`

Manually subscribes to a specific list of instruments on a given source.

*   **Use Case:** You want to track "BTC-PERPETUAL" and "ETH-PERPETUAL" specifically, without registering a full option chain.
*   **Arguments:**
    *   `source`: 'deribit', 'bloomberg', etc.
    *   `tickers`: List of symbol strings (e.g., `['BTC-PERPETUAL', 'SPY US Equity']`).

#### `get_subscription_map(register_name, target_dates, min_pct, max_pct)`

Helper function to generate a filtered list of instruments to subscribe to.

*   **Arguments:**
    *   `register_name`: The name used in `register_market`.
    *   `target_dates`: List of expiry strings (e.g., `['29DEC23', '26JAN24']`).
    *   `min_pct` / `max_pct`: Filter strikes based on moneyness (e.g., -10% to +10% from spot).
*   **Returns:** A dictionary structure organizing the filtered instruments.

---

## 2. Configuration Dictionaries

The `feed_config` dictionary is critical for `register_market`.

### Common Fields

| Field | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `register_name` | str | **Yes** | A unique ID for this market (e.g., "Deribit_BTC", "BBG_SPY"). Used for lookups later. |
| `source` | str | **Yes** | The adapter to use: `'deribit'`, `'bloomberg'`, `'binance'`. |
| `base_symbol` | str | **Yes** | The underlying asset symbol (e.g., `'BTC'`, `'ETH'`, `'SPY'`). |

### Source-Specific Fields

#### Deribit
*   **`account` (str, optional):** Key into your `keys.json` to select credentials. Default: `'default'`.
*   **`settlement` (str):**
    *   `'coin'`: Inverse contracts (e.g., BTC-margined).
    *   `'usd'`: Linear contracts (e.g., USDC-margined).

#### Bloomberg
*   **`base_symbol`:** The root ticker (e.g., `'SPY'`, `'SPX'`, `'ES1'`). The adapter will try to find `"SPY US Equity"` or `"SPX Index"`.

---

## 3. Data Structures (The Snapshot)

The `MarketSnapshot` object returned by `get_snapshot()` has the following attributes:

### `tickers` (Dict[str, Dict])

The core data store. Keys are **normalized instrument names**.

**Format:**
```python
{
    "BTC-29DEC23-30000-C": {
        "instrument_name": "BTC-29DEC23-30000-C",
        "last_price": 0.05,
        "best_bid_price": 0.045,
        "best_bid_amount": 10.0,
        "best_ask_price": 0.055,
        "best_ask_amount": 5.0,
        "ts": 1708765432100,  # Timestamp (ms)
        "stats": { ... }      # Exchange-specific stats (vol, high/low)
    },
    ...
}
```

### `index_prices` (Dict[str, float])

A fast lookup for the "Spot" or "Index" price of the underlyings.

**Example:**
```python
{
    "BTC-PERPETUAL": 65432.10,
    "SPY": 505.20
}
```

### `instruments_by_undl` (Dict[str, List[dict]])

A static map of the "Universe". This is populated during the `register_market` bootstrap phase. It lists *all* available instruments, even if you haven't subscribed to their price updates yet.

**Use Case:** Use this to populate a "Strike Selector" dropdown in your UI.

### `is_ready` (bool)

`True` if at least one adapter is successfully connected and streaming. `False` if offline or initializing.

---

## 4. Advanced Usage

### Instrument Name Normalization

The library strives to use a single format for all options, regardless of the source:

`{SYMBOL}-{DATE}-{STRIKE}-{TYPE}`

*   **Symbol:** BTC, ETH, SPY
*   **Date:** DDMMMYY (e.g., 20FEB26). Always uppercase.
*   **Strike:** Float or Int (e.g., 500, 65000).
*   **Type:** C (Call) or P (Put).

**Examples:**
*   Deribit: `BTC-29DEC23-50000-C` (Natively supported)
*   Bloomberg: `SPY US 02/20/26 C500 Equity` -> `SPY-20FEB26-500-C` (Converted automatically)

### Debugging with `feed_debug.log`

If you set `log_level=2`, the manager will append every single ticker ingestion event to `feed_debug.log`.
*   **Format:** `[HH:MM:SS] Ingest {SYMBOL}: Last={...} Bid={...} Ask={...}`
*   **Warning:** This file grows very fast. Only use for short debugging sessions.
