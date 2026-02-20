# Market Feed - Developer Guide

This document provides a technical deep-dive into the architecture, code structure, and data flow of the `market-feed` library. It is intended for developers who want to contribute to the project, modify its behavior, or create new exchange adapters.

## Core Architectural Principles

The system is designed around a few key principles:
- **Separation of Concerns:** The `FeedManager` acts as the central coordinator, while `ExchangeAdapter` implementations handle the specifics of each data source. This makes the system modular and easy to extend.
- **Data Normalization:** Raw data from different sources is always converted into a standardized `MarketSnapshot` and common ticker structure. The `FeedManager` is responsible for this normalization (`ingest_ticker` method), not the adapters.
- **Thread Safety:** All shared data within the `FeedManager` (e.g., `_tickers`, `_index_prices`) is protected by a single `threading.Lock` to prevent race conditions. Adapters run in their own background threads.
- **Hybrid Data Model:** The system uses a two-pronged approach for data acquisition:
    1.  **HTTP for Bootstrapping:** On registration, the manager makes synchronous HTTP calls via the adapter (`get_option_chain`, `get_latest_price`) to fetch the initial state (e.g., the full option chain) and reference prices. This ensures the application has a complete dataset from the start.
    2.  **WebSocket for Real-Time:** After bootstrapping, the manager uses WebSockets for low-latency, real-time updates for the subscribed instruments.

## Code Architecture and Components

### 1. `FeedManager` (`manager.py`)

The `FeedManager` is the public-facing entry point and the brain of the system.

**Responsibilities:**
-   **State Management:** It holds the central, normalized state of all market data, including `_tickers`, `_index_prices`, and `_instruments_by_undl`.
-   **Adapter Lifecycle:** It creates, starts, stops, and manages all `ExchangeAdapter` instances. Each adapter is keyed by a unique identifier: `f"{source}:{account}"`.
-   **Configuration:** It processes user-provided `feed_config` dictionaries to determine which adapters to launch and what data to fetch.
-   **Data Ingestion and Normalization:** The `ingest_ticker` method is the single entry point for all data coming from all adapters. It receives raw data, normalizes it into the standard ticker format, and merges it into the main `_tickers` state dictionary.
-   **Public API:** It exposes a clean API to the user for starting/stopping the feed, registering markets, and retrieving data (`get_snapshot`).

**Internal Data Flow for `register_market`:**
1.  A `feed_config` is received.
2.  `_get_or_create_adapter` is called, which initializes a new adapter if one for the specified `source:account` doesn't already exist.
3.  The adapter's synchronous methods are called to bootstrap data:
    -   `get_option_chain` fetches the full list of tradable instruments (e.g., all options for an underlying). This data is stored in `_instruments_by_undl`.
    -   `get_reference_tickers` gets the list of primary price references (e.g., `BTC-PERPETUAL`).
    -   `get_latest_price` is called for each reference ticker to bootstrap the `_index_prices` cache.
4.  If the manager is already running, the new adapter's `start()` method is called immediately.

### 2. `ExchangeAdapter` (`base.py`)

This is an abstract base class (`ABC`) that defines the contract every exchange-specific adapter must implement.

**Responsibilities:**
-   **Implement the Interface:** Provide concrete implementations for all `@abstractmethod`s.
-   **Exchange-Specific Logic:** Handle the unique details of connecting to an exchange's WebSocket and REST APIs.
-   **Data Translation (Outbound):** Translate generic subscription requests from the `FeedManager` into the specific format required by the exchange's API. For example, the `subscribe` method in the Deribit adapter formats channel names as `"ticker.{instrument_name}.100ms"`.
-   **Data Forwarding (Inbound):** Receive raw messages from the exchange's WebSocket, perform minimal parsing (e.g., `json.loads`), and immediately forward the raw data object to the `manager.ingest_ticker()` method. **Adapters should not normalize data.**

**Abstract Methods to Implement:**
-   `start()`: Start the main WebSocket connection and processing loop in a new thread.
-   `stop()`: Cleanly shut down the WebSocket connection and thread.
-   `get_option_chain(...)`: Implement the HTTP logic to fetch all instruments for a given underlying.
-   `get_latest_price(...)`: Implement the HTTP logic to fetch a single price for a single instrument.
-   `subscribe(...)`: Implement the logic to send a subscription message over the WebSocket.
-   `get_reference_tickers(...)`: Return a list of the key underlying/index instrument names for a given asset.

### 3. Concrete Adapters (`adapters/deribit.py`, `adapters/bloomberg.py`)

These classes provide the concrete implementation of the `ExchangeAdapter` interface for a specific data source.

**Example: `DeribitAdapter`**
-   **Connection:** Uses the `websocket-client` library to connect to Deribit's WebSocket API.
-   **Authentication:** Sends an auth request on connection if API keys are provided.
-   **Subscriptions:** The `subscribe` method takes a list of instrument names, maps them to Deribit's format (e.g., `ticker.BTC-PERPETUAL.100ms`), and sends the JSON-RPC subscription request.
-   **Data Handling:** The `_on_message` callback receives WebSocket data, loads it from JSON, and passes the `data` payload directly to `self.manager.ingest_ticker(data)`.

**Example: `BloombergAdapter`**
-   **Safe Import:** The adapter is designed to work even if the `blpapi` library is not installed (`HAS_BLPAPI` flag). All methods will gracefully do nothing in this case.
-   **Ticker Translation:** A significant portion of this adapter's logic is dedicated to translating between the app's internal ticker format (e.g., `SPY-20FEB26-688-C`) and the Bloomberg format (e.g., `SPY US 02/20/26 C688 Equity`).
-   **Correlation IDs:** It uses `blpapi.CorrelationId` to reliably map asynchronous responses from the Bloomberg API back to the application's internal instrument name. This is a critical pattern for working with `blpapi`.
-   **Reference Data vs. Market Data:** It uses the correct Bloomberg service for the job: `//blp/refdata` for bootstrapping the option chain and `//blp/mktdata` for real-time subscriptions.

## High-Level Data Flow (Real-Time Update)

1.  **Exchange:** A trade occurs on the exchange for a subscribed instrument (e.g., BTC-PERPETUAL).
2.  **WebSocket Message:** The exchange sends a WebSocket message containing the updated ticker data to the client.
3.  **Adapter Thread:** The corresponding adapter's `_on_message` method (running in a background thread) receives the raw message.
4.  **Forward to Manager:** The adapter performs minimal parsing (e.g., `json.loads`) and immediately calls `self.manager.ingest_ticker(raw_data_object)`.
5.  **Manager Locks and Ingests:** The `FeedManager` acquires its `_lock`.
    -   It finds the instrument name in the `raw_data_object`.
    -   It retrieves the existing ticker data for that instrument from its `_tickers` dictionary.
    -   It merges the new fields from the update into the existing data, ensuring no data is lost from partial updates.
    -   It updates the `timestamp`.
    -   If the update is for a reference ticker, it may also update the `_index_prices` cache.
6.  **Manager Unlocks:** The lock is released.
7.  **User `get_snapshot()`:** The user's application thread calls `feed.get_snapshot()`.
8.  **Snapshot Creation:** The `FeedManager` acquires the lock again, makes a deep copy of the `_tickers` and `_index_prices` dictionaries, creates a `MarketSnapshot` object, and releases the lock.
9.  **Return to User:** The thread-safe `MarketSnapshot` is returned to the user, who can now use the fresh data without worrying about race conditions.

This architecture ensures that the I/O-bound work of the adapters is isolated from the main application, and the `FeedManager` provides a simple, robust, and thread-safe interface for consuming complex market data.
