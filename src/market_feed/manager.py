import threading
import os
import json
import time
from typing import Dict, List, Any, Optional

from .base import MarketSnapshot, ExchangeAdapter
from .adapters.deribit import DeribitAdapter
from .adapters.bloomberg import BloombergAdapter

class FeedManager:
    def __init__(self, config_path="market_config.json", keys_path="keys.json", api_keys=None):
        self.config_path = config_path
        self.keys_path = keys_path
        self._lock = threading.Lock()
        
        self._tickers: Dict[str, Any] = {}
        self._index_prices: Dict[str, float] = {}
        self._instruments_by_tab: Dict[str, List[dict]] = {}
        self._instrument_sets: Dict[str, set] = {}
        
        self._keys = api_keys if api_keys else self._load_keys_from_file()
        self._market_config = self._load_config()
        
        for cfg in self._market_config:
            t = cfg['tab_name']
            self._instruments_by_tab[t] = []
            self._instrument_sets[t] = set()

        self.adapters: Dict[str, ExchangeAdapter] = {}
        self._init_adapters()
        
        print("[FeedManager] Bootstrapping...")
        self._bootstrap_instruments()
        self._bootstrap_prices()

    def _init_adapters(self):
        # 1. Identify which sources are actually requested in the config
        #    e.g. {'deribit', 'binance'}
        active_sources = {cfg.get('source', 'deribit').lower() for cfg in self._market_config}
        
        # 2. Initialize only the required adapters
        if 'deribit' in active_sources:
            # We assume keys are flat for now: "client_id", "client_secret"
            # Future improvement: "deribit_id", "binance_key", etc.
            d_id = self._keys.get("client_id")
            d_secret = self._keys.get("client_secret")
            self.adapters['deribit'] = DeribitAdapter(self, d_id, d_secret)
            
        if 'binance' in active_sources:
            # Assuming you created a BinanceAdapter class
            # from .adapters.binance import BinanceAdapter
            # self.adapters['binance'] = BinanceAdapter(self)
            print("[FeedManager] Warning: Binance requested but adapter not imported yet.")
            
        if 'bloomberg' in active_sources:
            try:
                self.adapters['bloomberg'] = BloombergAdapter(self)
                print("[FeedManager] Bloomberg Adapter Initialized.")
            except ImportError:
                print("❌ Error: 'blpapi' library not found. Cannot start Bloomberg adapter.")

    def get_subscription_map(self, tab_name, target_dates, min_pct, max_pct):
        cfg = next((c for c in self._market_config if c['tab_name'] == tab_name), None)
        if not cfg: return {}
        
        source = cfg.get('source', 'deribit').lower()
        adapter = self.adapters.get(source)
        if not adapter: return {}

        # --- GENERIC LOGIC START ---
        # Ask adapter: "What are the reference tickers for this tab?"
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
            subs_to_send = [f"ticker.{t}.100ms" for t in ref_tickers]
            structure = {}

            for inst in self._instruments_by_tab.get(tab_name, []):
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
                    subs_to_send.append(f"ticker.{nm}.100ms")

            for d in structure: structure[d]['strikes'].sort()
            
            if adapter.connected:
                adapter.subscribe(subs_to_send)
            
            return structure
        # --- GENERIC LOGIC END ---

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
        for a in self.adapters.values(): a.start()
    def stop_stream(self):
        for a in self.adapters.values(): a.stop()
    @property
    def market_config(self): return self._market_config
    @property
    def lock(self): return self._lock
    @property
    def instruments_by_tab(self): return self._instruments_by_tab
    
    def get_snapshot(self) -> MarketSnapshot:
        is_ready = any(a.connected for a in self.adapters.values())
        with self._lock:
            return MarketSnapshot(
                is_ready=is_ready, index_prices=self._index_prices.copy(),
                tickers=self._tickers.copy(), config=self._market_config,
                instruments_by_tab={k: v[:] for k, v in self._instruments_by_tab.items()}
            )

    def get_expiries_for(self, tab_name):
        with self._lock:
            dates = set()
            if tab_name not in self._instruments_by_tab: return []
            for i in self._instruments_by_tab[tab_name]:
                parts = i['instrument_name'].split('-')
                if len(parts) > 1: dates.add(parts[1])
            def sorter(d_str):
                from datetime import datetime
                try: return datetime.strptime(d_str, "%d%b%y")
                except: return datetime.max
            return sorted(list(dates), key=sorter)
            
    def _bootstrap_instruments(self):
        for cfg in self._market_config:
            source = cfg.get('source', 'deribit').lower()
            adapter = self.adapters.get(source)
            if not adapter: continue
            instruments = adapter.get_instruments(cfg)
            tab = cfg['tab_name']
            with self._lock:
                for inst in instruments:
                    nm = inst['instrument_name']
                    if nm not in self._instrument_sets[tab]:
                        self._instrument_sets[tab].add(nm)
                        self._instruments_by_tab[tab].append(inst)

    def ingest_ticker(self, raw_data):
        nm = raw_data['instrument_name']
        norm = {
            'instrument_name': nm,
            'best_bid_price': raw_data.get('best_bid_price'),
            'best_bid_amount': raw_data.get('best_bid_amount'),
            'best_ask_price': raw_data.get('best_ask_price'),
            'best_ask_amount': raw_data.get('best_ask_amount'),
            'last_price': raw_data.get('last_price'),
            'stats': raw_data.get('stats', {}),
            'ts': raw_data.get('timestamp')
        }
        with self._lock:
            self._tickers[nm] = norm
            if "PERPETUAL" in nm or "USDC" in nm:
                px = raw_data.get('index_price') or raw_data.get('last_price') or 0
                if px > 0: self._index_prices[nm] = px

    def on_adapter_reconnect(self, source_name): pass
    def _load_keys_from_file(self):
        if os.path.exists(self.keys_path):
            try:
                with open(self.keys_path, 'r') as f: return json.load(f)
            except: pass
        return {}
    def _load_config(self):
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r') as f: return json.load(f)
            except: pass
        return []