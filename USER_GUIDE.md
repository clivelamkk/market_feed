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
feed = FeedManager(
    keys_path="keys.json", 
    instrument_config_path="feed_instruments.csv", 
    log_level=0
)
```

*   **`keys_path` (str, optional):** Path to a JSON file containing API credentials. Default is `"keys.json"`.
*   **`instrument_config_path` (str, optional):** Path to the symbol mapping CSV. Default is `"feed_instruments.csv"`.
*   **`log_level` (int):** Controls verbosity.
    *   `0`: Silent (Errors only).
    *   `1`: Info (Bootstrapping, Connection status).
    *   `2`: Debug (Logs all incoming ticker updates to `feed_debug.log`).

### Core Methods (FeedManager Class)

#### `register_adapter(source: str, account: str = 'default') -> ExchangeAdapter`

Explicitly creates or retrieves an adapter for a given source/account.

*   **Use Case:** Creates and initializes the connection logic. It is the first step in the "Initialization/Connection" workflow.
*   **Arguments:**
    *   `source`: 'deribit', 'bloomberg', etc.
    *   `account`: The account identifier from `keys.json`. Defaults to `'default'`.

#### `register_market(feed_config: dict)`

Tells the FeedManager about a market configuration (e.g., BTC, SPY).

*   **Prerequisite:** You MUST call `register_adapter` for the corresponding source/account first.
*   **Behavior:** Updates internal configuration only. Does NOT automatically subscribe.
*   **Next Steps:** You must manually call `subscribe_custom()` to start receiving data for the underlying tickers.
*   **Arguments:** `feed_config` (See [Section 2](#2-configuration-dictionaries)).

#### `initialize_option_chain(register_name: str)`

Explicitly fetches the full option chain for a registered underlying.

*   **Use Case:** Call this if you need the full list of option instruments (e.g., for a strike selector or chain visualization).
*   **Behavior:** Performs a synchronous HTTP request to the exchange to download all active instruments for the underlying.
*   **Arguments:** `register_name` (The unique ID provided in `register_market`).

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

#### `get_subscription_map(register_name, target_dates, min_pct, max_pct, spot_price=None)`

Helper function to generate a filtered list of instruments to subscribe to.

*   **Arguments:**
    *   `register_name`: The name used in `register_market`.
    *   `target_dates`: List of expiry strings (e.g., `['29DEC23', '26JAN24']`).
    *   `min_pct` / `max_pct`: Filter strikes based on moneyness (e.g., -10% to +10% from spot).
    *   `spot_price` (float, optional): The reference spot price. If `None` or `0`, the manager will attempt to guess it from cached data.
*   **Returns:** A dictionary structure organizing the filtered instruments.

---

## 2. Configuration

### Configuration Dictionaries (Runtime)

The `feed_config` dictionary is critical for `register_market`.

#### Common Fields

| Field | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `register_name` | str | **Yes** | A unique ID for this market (e.g., "Deribit_BTC", "BBG_SPY"). Used for lookups later. |
| `source` | str | **Yes** | The adapter to use: `'deribit'`, `'bloomberg'`, `'binance'`. |
| `base_symbol` | str | **Yes** | The underlying asset symbol (e.g., `'BTC'`, `'ETH'`, `'SPY'`). |

#### Source-Specific Fields

*   **Deribit:**
    *   `account` (str, optional): Key into your `keys.json`. Default: `'default'`.
    *   `settlement` (str): `'coin'` (Inverse) or `'usd'` (Linear).
*   **Bloomberg:**
    *   `base_symbol`: The root ticker (e.g., `'SPY'`). The adapter will try to find `"SPY US Equity"` or `"SPX Index"`.

### Configuration Files (Static)

The library relies on two files to configure adapters and symbol mappings. You must provide the path to these files when initializing the `FeedManager`.

#### 1. `keys.json`
Stores authentication details for exchanges. Pass the path to this file as `keys_path="..."`.

**Format:**
```json
{
  "deribit": {
    "default": { "client_id": "...", "client_secret": "..." },
    "trading_subaccount": { "client_id": "...", "client_secret": "..." }
  }
}
```

#### 2. `feed_instruments.csv`
A CSV file that defines **symbol normalization rules**. It maps your internal "clean" symbols to vendor-specific "messy" tickers.

**Location:** You must provide the path to this file via the `instrument_config_path` argument in `FeedManager()`.
*   Example: `feed = FeedManager(instrument_config_path="path/to/my_feed_instruments.csv")`

**Columns:**
*   `Symbol`: Your internal application symbol (e.g., `BTC`, `SPX`, `TENCENT`).
*   `[ADAPTER_NAME]`: Column for each adapter (e.g., `bloomberg`, `deribit`).

**Value Prefixes (Specific Meanings):**

These prefixes instruct the adapter on how to interpret the value in the CSV.

| Prefix | Adapter | Description | Why Use This? |
| :--- | :--- | :--- | :--- |
| **`Exact:`** | All | **Literal Mapping.** The value after the colon is used exactly as-is by the adapter. | Use when the adapter's default logic fails or when you have a specific ticker code (e.g., `Exact:BTC_USDC`). |
| **`Index`** | Bloomberg | **Index Suffix.** Appends " Index" to the symbol. | Use for standard indices like SPX, NDX, VIX where the ticker is just the symbol + " Index". |
| **`FuturePrefix`** | Bloomberg | **Future Logic.** Treats the symbol as a generic future prefix. | Use for futures where the contract code changes by month (e.g., `ES` -> `ESZ3 Index`). The adapter will handle expiration matching. |
| *(Empty)* | All | **Default Behavior.** The adapter applies its standard formatting rules. | Use for standard equities (e.g., `SPY` -> `SPY US Equity`). |

**Example Content:**
```csv
Symbol,bloomberg,deribit
SPX,Index,
ES,FuturePrefix,
BTC,Exact:XBTUSD Curncy,Exact:BTC_USDC
ETH.PERP,Exact:ETHUSD Curncy,Exact:ETH-PERPETUAL
TENCENT,Exact:0700 HK Equity,
```

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

A static map of the "Universe". This is populated during the `initialize_option_chain` phase. It lists *all* available instruments, even if you haven't subscribed to their price updates yet.

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
