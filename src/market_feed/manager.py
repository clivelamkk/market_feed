import threading
import os
import json
import time
from typing import Dict, List, Any, Optional

from .base import MarketSnapshot, ExchangeAdapter
from .adapters.deribit import DeribitAdapter
# from .adapters.bloomberg import BloombergAdapter

class FeedManager:
    def __init__(self, keys_path="keys.json", api_keys=None, log_level=0):
        """
        Args:
            keys_path (str): Path to keys.json
            api_keys (dict): Direct keys dict
            log_level (int): 0=None, 1=Spot/Perps Only, 2=All (Options included)
        """
        self.keys_path = keys_path
        self.log_level = log_level
        self._lock = threading.Lock()
        
        self._tickers: Dict[str, Any] = {}
        self._index_prices: Dict[str, float] = {}
        self._instruments_by_undl: Dict[str, List[dict]] = {}
        self._instrument_sets: Dict[str, set] = {}
        
        # self._keys = api_keys if api_keys else self._load_keys_from_file()
        self._keys = api_keys if api_keys is not None else self._load_keys_from_file()
        self._market_config = []
        self.running = False
        
        self.adapters: Dict[str, ExchangeAdapter] = {}
        self._init_adapters()
        
        print(f"[FeedManager] Bootstrapping... (Log Level: {self.log_level})")
        # self._bootstrap_instruments() # No config initially
        # self._bootstrap_prices()

    def _init_adapters(self):
        # 1. Identify which sources are actually requested in the config
        #    e.g. {'deribit', 'binance'}
        active_sources = {cfg.get('source', 'deribit').lower() for cfg in self._market_config}
        
        # 2. Initialize only the required adapters
        if 'deribit' in active_sources and 'deribit' not in self.adapters:
            # We assume keys are flat for now: "client_id", "client_secret"
            # Future improvement: "deribit_id", "binance_key", etc.
            d_id = self._keys.get("client_id")
            d_secret = self._keys.get("client_secret")
            self.adapters['deribit'] = DeribitAdapter(self, d_id, d_secret)
            
        if 'binance' in active_sources and 'binance' not in self.adapters:
            # Assuming you created a BinanceAdapter class
            # from .adapters.binance import BinanceAdapter
            # self.adapters['binance'] = BinanceAdapter(self)
            print("[FeedManager] Warning: Binance requested but adapter not imported yet.")
            
        if 'bloomberg' in active_sources and 'bloomberg' not in self.adapters:
            try:
                from .adapters.bloomberg import BloombergAdapter
                self.adapters['bloomberg'] = BloombergAdapter(self)
                print("[FeedManager] Bloomberg Adapter Initialized.")
            except ImportError as e:
                # This catches the error raised by bloomberg.py if blpapi is missing
                print(f"[FeedManager] Skipping Bloomberg: {e}")
            except Exception as e:
                print(f"[FeedManager] Error initializing Bloomberg: {e}")

    def register_market(self, feed_config: dict):
        """
        Registers a new market/feed configuration and initializes its data.
        Call this to express interest in specific underlyings/sources.
        
        Args:
            feed_config (dict): A dictionary containing feed settings. 
                                Example: {'register_name': 'BTC Options', 'source': 'deribit', ...}
        """
        register = feed_config.get('register_name')
        if not register:
            print("[FeedManager] Error: 'register_name' is required in feed_config.")
            return

        with self._lock:
            # 1. Update Config & Structures
            self._market_config.append(feed_config)
            if register not in self._instruments_by_undl:
                self._instruments_by_undl[register] = []
                self._instrument_sets[register] = set()
        
        # 2. Ensure Adapter exists
        self._init_adapters()
        
        # 3. Bootstrap Data for this specific feed
        source = feed_config.get('source', 'deribit').lower()
        adapter = self.adapters.get(source)
        
        if adapter:
            print(f"[FeedManager] Bootstrapping feed for {register} ({source})...")
            # Bootstrap Instruments
            instruments = adapter.get_option_chain(feed_config)
            with self._lock:
                for inst in instruments:
                    nm = inst['instrument_name']
                    if nm not in self._instrument_sets[register]:
                        self._instrument_sets[register].add(nm)
                        self._instruments_by_undl[register].append(inst)
            
            # Bootstrap Prices (Reference Tickers)
            tickers = adapter.get_reference_tickers(feed_config)
            for t in tickers:
                px = adapter.get_latest_price(t)
                if px > 0:
                    with self._lock: self._index_prices[t] = px
                    print(f"[FeedManager] Bootstrapped {t}: {px}")
            
            # 4. If manager is already running, start the new adapter immediately
            if self.running:
                adapter.start()
        else:
            print(f"[FeedManager] Error: Adapter for source '{source}' could not be initialized.")

    def get_subscription_map(self, register_name, target_dates, min_pct, max_pct):
        cfg = next((c for c in self._market_config if c['register_name'] == register_name), None)
        if not cfg: return {}
        
        source = cfg.get('source', 'deribit').lower()
        adapter = self.adapters.get(source)
        if not adapter: return {}

        # --- GENERIC LOGIC START ---
        # Ask adapter: "What are the reference tickers for this underlying?"
        ref_tickers = adapter.get_reference_tickers(cfg)
        
        with self._lock:
            # Try to find a valid spot price from the reference list
            spot = 0
            for t in ref_tickers:
                spot = self._index_prices.get(t, 0)
                if spot > 0: break
            
            if spot == 0: return {}

            lo = spot * (1 + min_pct / 100)
            hi = spot * (1 + max_pct / 100)
            
            # Start subscription list with the reference tickers
            # (Note: Adapter specific prefix 'ticker.' is still here, 
            # ideally that should also be in adapter, but this is acceptable for now)
            # subs_to_send = [f"ticker.{t}.100ms" for t in ref_tickers]
            subs_to_send = list(ref_tickers)
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
                    # subs_to_send.append(f"ticker.{nm}.100ms")
                    subs_to_send.append(nm)

            for d in structure: structure[d]['strikes'].sort()
            
            if adapter.connected:
                adapter.subscribe(subs_to_send)
            
            return structure
        # --- GENERIC LOGIC END ---

    def subscribe_custom(self, source: str, tickers: List[str]):
        """
        Manually subscribe to a list of arbitrary tickers on a specific source.
        Useful for adding spot/index tickers that aren't part of the option chain.
        
        Args:
            source (str): 'deribit', 'bloomberg', etc.
            tickers (list): List of ticker strings (e.g. ['AAPL', 'MSFT', 'BTC-PERPETUAL'])
        """
        adapter = self.adapters.get(source.lower())
        if adapter:
            print(f"[FeedManager] Custom subscription to {len(tickers)} tickers on {source}")
            adapter.subscribe(tickers)
        else:
            print(f"[FeedManager] Error: Source '{source}' not initialized.")

    def _bootstrap_prices(self):
        """Generic price bootstrapping using Adapter logic."""
        for cfg in self._market_config:
            source = cfg.get('source', 'deribit').lower()
            adapter = self.adapters.get(source)
            if not adapter: continue
            
            # Ask adapter for tickers
            tickers = adapter.get_reference_tickers(cfg)
            
            for t in tickers:
                px = adapter.get_latest_price(t)
                if px > 0:
                    with self._lock: self._index_prices[t] = px
                    print(f"[FeedManager] Bootstrapped {t}: {px}")

    # ... (Rest of the methods: get_snapshot, ingest_ticker, etc. remain the same) ...
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
            
    def _bootstrap_instruments(self):
        for cfg in self._market_config:
            source = cfg.get('source', 'deribit').lower()
            adapter = self.adapters.get(source)
            if not adapter: continue
            instruments = adapter.get_option_chain(cfg)
            register = cfg['register_name']
            with self._lock:
                for inst in instruments:
                    nm = inst['instrument_name']
                    if nm not in self._instrument_sets[register]:
                        self._instrument_sets[register].add(nm)
                        self._instruments_by_undl[register].append(inst)

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
        if self.keys_path and os.path.exists(self.keys_path):
        # if os.path.exists(self.keys_path):
            try:
                with open(self.keys_path, 'r') as f: return json.load(f)
            except: pass
        return {}