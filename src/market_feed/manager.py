import threading
import os
import json
import time
from typing import Dict, List, Any, Optional

from .base import MarketSnapshot, ExchangeAdapter
from .adapters.deribit import DeribitAdapter
# from .adapters.bloomberg import BloombergAdapter

class FeedManager:
    def __init__(self, keys_path="keys.json", instrument_config_path="feed_instruments.csv", log_level=0):
        """
        Initializes the FeedManager. It can manage multiple adapters for the same
        data source, each with its own API key.

        The `keys.json` file should be structured to support multiple accounts per vendor:
        {
            "deribit": {
                "default": { "client_id": "...", "client_secret": "..." },
                "account_2": { "client_id": "...", "client_secret": "..." }
            },
            "binance": {
                "default": { "client_id": "...", "client_secret": "..." }
            }
        }
        When registering a market, you can specify an 'account' in the feed_config.
        If no account is specified, it will use the 'default' key.

        Args:
            keys_path (str): Path to keys.json file.
            instrument_config_path (str): Path to feed_instruments.csv file.
            log_level (int): 0=None, 1=Spot/Perps Only, 2=All (Options included)
        """
        self.keys_path = keys_path
        self.instrument_config_path = instrument_config_path
        self.log_level = log_level
        self._lock = threading.Lock()
        
        self._tickers: Dict[str, Any] = {}
        self._index_prices: Dict[str, float] = {}
        self._instruments_by_undl: Dict[str, List[dict]] = {}
        self._instrument_sets: Dict[str, set] = {}
        
        self._keys = self._load_keys_from_file()
        self._market_config = []
        self.running = False
        
        # Adapters are keyed by "source:account" (e.g., "deribit:default")
        self.adapters: Dict[str, ExchangeAdapter] = {}
        
        print(f"[FeedManager] Bootstrapping... (Log Level: {self.log_level})")



    def register_adapter(self, source: str, account: str = 'default') -> Optional[ExchangeAdapter]:
        """
        Explicitly creates or retrieves an adapter for a given source/account.
        Useful if you want to initialize the connection without registering a specific market.
        """
        source = source.lower()
        account = account.lower()
        adapter_key = f"{source}:{account}"

        if adapter_key in self.adapters:
            return self.adapters[adapter_key]

        print(f"[FeedManager] Creating new adapter for {adapter_key}")
        
        adapter = None
        if source == 'deribit':
            # Get the specific account's keys, or the vendor's top-level keys, or empty dict
            vendor_keys = self._keys.get(source, {})
            account_keys = vendor_keys.get(account, {})
            client_id = account_keys.get("client_id")
            client_secret = account_keys.get("client_secret")
            adapter = DeribitAdapter(self, client_id, client_secret, instrument_config_path=self.instrument_config_path)
            
        elif source == 'binance':
            vendor_keys = self._keys.get(source, {})
            account_keys = vendor_keys.get(account, {})
            client_id = account_keys.get("client_id")
            client_secret = account_keys.get("client_secret")
            # from .adapters.binance import BinanceAdapter
            # adapter = BinanceAdapter(self, client_id, client_secret, instrument_config_path=self.instrument_config_path)
            print("[FeedManager] Warning: Binance adapter not fully implemented yet.")

        elif source == 'bloomberg':
            try:
                from .adapters.bloomberg import BloombergAdapter
                adapter = BloombergAdapter(self, instrument_config_path=self.instrument_config_path)
                print("[FeedManager] Bloomberg Adapter Initialized.")
            except ImportError as e:
                print(f"[FeedManager] Skipping Bloomberg: {e}")
            except Exception as e:
                print(f"[FeedManager] Error initializing Bloomberg: {e}")
        
        if adapter:
            self.adapters[adapter_key] = adapter
            # Auto-start if manager is already running
            if self.running and not adapter.is_alive():
                adapter.start()
        
        return adapter

    def register_market(self, feed_config: dict):
        """
        Registers a market configuration with the FeedManager.
        This enables subsequent calls like initialize_option_chain() or get_subscription_map().
        
        Note: This does NOT automatically subscribe to any tickers.
        You must manually call subscribe_custom() for the underlying/reference tickers you want.
        """
        register_name = feed_config.get('register_name')
        if not register_name:
            print("[FeedManager] Error: 'register_name' is required in feed_config.")
            return

        source = feed_config.get('source', 'deribit').lower()
        account = feed_config.get('account', 'default').lower()
        adapter_key = f"{source}:{account}"
        adapter = self.adapters.get(adapter_key)

        if not adapter:
            print(f"[FeedManager] Error: Cannot register market '{register_name}'. Adapter '{adapter_key}' not found. Call register_adapter() first.")
            return

        # Update Internal State
        with self._lock:
            self._market_config.append(feed_config)
            if register_name not in self._instruments_by_undl:
                self._instruments_by_undl[register_name] = []
                self._instrument_sets[register_name] = set()

        print(f"[FeedManager] Market '{register_name}' registered on {adapter.name}. Ready for manual subscription.")

    def initialize_option_chain(self, register_name: str):
        """
        Explicitly fetches the full option chain for a registered underlying.
        Call this after register_underlying() if you need option data.
        """
        # Find config by register_name
        cfg = next((c for c in self._market_config if c['register_name'] == register_name), None)
        if not cfg:
            print(f"[FeedManager] Error: Underlying '{register_name}' not found. Call register_underlying() first.")
            return

        source = cfg.get('source', 'deribit').lower()
        account = cfg.get('account', 'default').lower()
        adapter_key = f"{source}:{account}"
        adapter = self.adapters.get(adapter_key)
        
        if not adapter: return

        print(f"[FeedManager] Fetching option chain for {register_name}...")
        instruments = adapter.get_option_chain(cfg)
        
        with self._lock:
            count = 0
            for inst in instruments:
                nm = inst['instrument_name']
                if nm not in self._instrument_sets[register_name]:
                    self._instrument_sets[register_name].add(nm)
                    self._instruments_by_undl[register_name].append(inst)
                    count += 1
            print(f"[FeedManager] Loaded {count} instruments for {register_name}")

    def get_subscription_map(self, register_name, target_dates, min_pct, max_pct, spot_price=None):
        cfg = next((c for c in self._market_config if c['register_name'] == register_name), None)
        if not cfg: return {}
        
        source = cfg.get('source', 'deribit').lower()
        account = cfg.get('account', 'default').lower()
        adapter_key = f"{source}:{account}"
        adapter = self.adapters.get(adapter_key)
        if not adapter: return {}

        # --- GENERIC LOGIC START ---
        
        # 1. Resolve Spot Price (If not provided)
        if spot_price is None or spot_price <= 0:
            base = cfg.get('base_symbol')
            # Try to guess from common internal names or base symbol
            candidates = [
                base,
                f"{base}-PERPETUAL",
                f"{base}_USDC",
                f"{base}.PERP", # If internal mapping used
                f"{base}.USDC"
            ]
            with self._lock:
                for t in candidates:
                    if not t: continue
                    # Check Index Price First
                    price = self._index_prices.get(t, 0)
                    # Fallback to Last Price
                    if price <= 0 and t in self._tickers:
                         price = self._tickers[t].get('last_price', 0)
                    
                    if price > 0:
                        spot_price = price
                        # print(f"[FeedManager] Guessed spot for {register_name} using {t}: {spot_price}")
                        break
        
        with self._lock:
            if spot_price is None or spot_price <= 0: return {}

            lo = spot_price * (1 + min_pct / 100)
            hi = spot_price * (1 + max_pct / 100)
            
            subs_to_send = []
            structure = {}

            for inst in self._instruments_by_undl.get(register_name, []):
                nm = inst['instrument_name']
                # Parsing logic is still generic enough (CCY-DATE-STRIKE-TYPE)
                # If Binance uses different format, we'd need adapter.parse_instrument(nm)
                parts = nm.split('-')
                if len(parts) < 4: continue
                date, k, kind = parts[1], float(parts[2]), parts[3]
                
                if date in target_dates and lo <= k <= hi:
                    if date not in structure: structure[date] = {'strikes': [], 'map': {}}
                    if k not in structure[date]['map']:
                        structure[date]['map'][k] = {'C': None, 'P': None}
                        structure[date]['strikes'].append(k)
                    
                    structure[date]['map'][k][kind] = nm
                    subs_to_send.append(nm)

            for d in structure: structure[d]['strikes'].sort()
            
            if adapter.connected and subs_to_send:
                adapter.subscribe(subs_to_send)
            
            return structure
        # --- GENERIC LOGIC END ---

    def subscribe_custom(self, source: str, tickers: List[str]):
        """
        Manually subscribe to a list of arbitrary tickers on a specific source,
        using the 'default' account for that source.

        Args:
            source (str): 'deribit', 'bloomberg', etc.
            tickers (list): List of ticker strings (e.g. ['AAPL', 'MSFT', 'BTC-PERPETUAL'])
        """
        source_lower = source.lower()
        adapter_key = f"{source_lower}:default"
        adapter = self.adapters.get(adapter_key)

        if adapter:
            print(f"[FeedManager] Custom subscription to {len(tickers)} tickers on {adapter_key}")
            adapter.subscribe(tickers)
        else:
            print(f"[FeedManager] Error: Default adapter for source '{source}' not initialized. "
                  "Ensure a market from that source has been registered first.")

    def unsubscribe_options(self, register_name: str):
        """
        Unsubscribe from all options associated with a given register name.
        """
        cfg = next((c for c in self._market_config if c['register_name'] == register_name), None)
        if not cfg:
            print(f"[FeedManager] Error: Market '{register_name}' not found.")
            return

        source = cfg.get('source', 'deribit').lower()
        account = cfg.get('account', 'default').lower()
        adapter_key = f"{source}:{account}"
        adapter = self.adapters.get(adapter_key)
        
        if not adapter: return

        with self._lock:
            instruments = self._instruments_by_undl.get(register_name, [])
            subs_to_remove = [inst['instrument_name'] for inst in instruments]
            
            if adapter.connected and subs_to_remove:
                adapter.unsubscribe(subs_to_remove)
                print(f"[FeedManager] Unsubscribed from {len(subs_to_remove)} options for {register_name}.")

    def start_stream(self):
        self.running = True
        for a in self.adapters.values(): a.start()
    def stop_stream(self):
        self.running = False
        for name, adapter in self.adapters.items():
            try:
                adapter.stop()
            except Exception as e:
                print(f"[FeedManager] Error stopping {name}: {e}")
    @property
    def market_config(self): return self._market_config
    @property
    def lock(self): return self._lock
    @property
    def instruments_by_undl(self): return self._instruments_by_undl
    
    def get_snapshot(self) -> MarketSnapshot:
        is_ready = any(a.connected for a in self.adapters.values())
        with self._lock:
            return MarketSnapshot(
                is_ready=is_ready, index_prices=self._index_prices.copy(),
                tickers=self._tickers.copy(), config=self._market_config,
                instruments_by_undl={k: v[:] for k, v in self._instruments_by_undl.items()}
            )

    def get_expiries_for(self, register_name):
        with self._lock:
            dates = set()
            if register_name not in self._instruments_by_undl: return []
            for i in self._instruments_by_undl[register_name]:
                parts = i['instrument_name'].split('-')
                if len(parts) > 1: dates.add(parts[1])
            def sorter(d_str):
                from datetime import datetime
                try: return datetime.strptime(d_str, "%d%b%y")
                except: return datetime.max
            return sorted(list(dates), key=sorter)

    def get_option_chain_details(self, register_name):
        """
        Returns a structured list of all options in the underlying.
        Each item includes:
        - symbol: str (e.g. BTC-27FEB26-67000-P)
        - base_currency: str
        - expiry: str (e.g. 27FEB26)
        - strike: float
        - type: str ('C' or 'P')
        """
        results = []
        with self._lock:
            instruments = self._instruments_by_undl.get(register_name, [])
            for inst in instruments:
                nm = inst['instrument_name']
                parts = nm.split('-')
                # Expected format: SYMBOL-DATE-STRIKE-TYPE
                if len(parts) >= 4:
                    try:
                        base = parts[0]
                        expiry = parts[1]
                        strike = float(parts[2])
                        kind = parts[3]
                        
                        results.append({
                            'symbol': nm,
                            'base_currency': base,
                            'expiry': expiry,
                            'strike': strike,
                            'type': kind,
                            'raw': inst
                        })
                    except:
                        pass # Skip malformed names
        return results
            
    def ingest_ticker(self, raw_data):
        nm = raw_data['instrument_name']
        
        # Prepare the new normalized object, but don't finalize it yet
        new_data = {
            'instrument_name': nm,
            'best_bid_price': raw_data.get('best_bid_price'),
            'best_bid_amount': raw_data.get('best_bid_amount'),
            'best_ask_price': raw_data.get('best_ask_price'),
            'best_ask_amount': raw_data.get('best_ask_amount'),
            'last_price': raw_data.get('last_price'),
            'stats': raw_data.get('stats', {}),
            'ts': raw_data.get('timestamp')
        }

        # DEBUG: Log based on Level
        should_log = False
        if self.log_level >= 2:
            should_log = True
        elif self.log_level == 1:
            if nm in ['BTC', 'BTC.PERP', 'ETH', 'ETH.PERP', 'SPY', 'QQQ']:
                should_log = True
                
        if should_log:
            try:
                with open("feed_debug.log", "a") as f:
                    import datetime
                    t = datetime.datetime.now().strftime("%H:%M:%S")
                    # Log what we *received* (raw new_data) to understand the partial updates
                    f.write(f"[{t}] Ingest {nm}: Last={new_data['last_price']} Bid={new_data['best_bid_price']} Ask={new_data['best_ask_price']}\n")
            except: pass
            
        with self._lock:
            # MERGE LOGIC:
            # If we already have this ticker, update only the fields that are NOT None in the new data.
            if nm in self._tickers:
                current = self._tickers[nm]
                for k, v in new_data.items():
                    if v is not None:
                        current[k] = v
                # Ensure stats are merged or updated appropriately (simple overwrite for now)
                if new_data['stats']:
                    current['stats'] = new_data['stats']
                # Always update timestamp if provided
                if new_data['ts']:
                    current['ts'] = new_data['ts']
            else:
                # New ticker, just store it
                self._tickers[nm] = new_data

            # Update index prices if applicable
            if "PERPETUAL" in nm or "USDC" in nm:
                px = raw_data.get('index_price') or raw_data.get('last_price') or 0
                if px > 0: self._index_prices[nm] = px

    def on_adapter_reconnect(self, source_name): pass

    def _load_keys_from_file(self):
        if not (self.keys_path and os.path.exists(self.keys_path)):
            return {}
        try:
            with open(self.keys_path, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"[FeedManager] Error loading keys from {self.keys_path}: {e}")
            return {}