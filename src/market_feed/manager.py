import threading
import os
import json
import time
from typing import Dict, List, Any, Optional

from .base import MarketSnapshot, ExchangeAdapter
from .adapters.deribit import DeribitAdapter

class FeedManager:
    """
    Central controller.
    Can load keys from a file OR accept them directly from the calling app.
    """
    def __init__(
        self, 
        config_path: str = "market_config.json", 
        keys_path: str = "keys.json", 
        api_keys: Optional[Dict[str, str]] = None
    ):
        self.config_path = config_path
        self.keys_path = keys_path
        
        self._lock = threading.Lock()
        
        # --- Shared State ---
        self._tickers: Dict[str, Any] = {}
        self._index_prices: Dict[str, float] = {}
        self._instruments_by_tab: Dict[str, List[dict]] = {}
        self._instrument_sets: Dict[str, set] = {}
        
        # --- Load Credentials ---
        # Priority: 1. Passed arguments, 2. File path
        self._keys = api_keys if api_keys else self._load_keys_from_file()
        
        # --- Load Config ---
        self._market_config = self._load_config()
        
        # Initialize Data Containers
        for cfg in self._market_config:
            t = cfg['tab_name']
            self._instruments_by_tab[t] = []
            self._instrument_sets[t] = set()

        # --- Initialize Adapters ---
        self.adapters: Dict[str, ExchangeAdapter] = {}
        self._init_adapters()
        
        # --- Bootstrapping (Blocking) ---
        print("[FeedManager] Bootstrapping instruments...")
        self._bootstrap_instruments()
        
        print("[FeedManager] Bootstrapping prices...")
        self._bootstrap_prices()

    def _init_adapters(self):
        # 1. Deribit
        # Keys might be empty/None, adapter handles graceful degradation (public only)
        d_id = self._keys.get("client_id") if self._keys else None
        d_secret = self._keys.get("client_secret") if self._keys else None
        
        self.adapters['deribit'] = DeribitAdapter(self, d_id, d_secret)

    def start_stream(self):
        for a in self.adapters.values(): a.start()

    def stop_stream(self):
        for a in self.adapters.values(): a.stop()

    # --- PUBLIC API ---

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
                is_ready=is_ready,
                index_prices=self._index_prices.copy(),
                tickers=self._tickers.copy(),
                config=self._market_config,
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

    def get_subscription_map(self, tab_name, target_dates, min_pct, max_pct):
        cfg = next((c for c in self._market_config if c['tab_name'] == tab_name), None)
        if not cfg: return {}
        
        source = cfg.get('source', 'deribit').lower()
        adapter = self.adapters.get(source)
        if not adapter: return {}

        base = cfg['base_symbol']
        is_usd = cfg['settlement'] == 'usd'
        
        if is_usd:
            index_key = f"{base}_USDC" 
            perp_key = f"{base}_USDC-PERPETUAL"
        else:
            index_key = f"{base}-PERPETUAL" 
            perp_key = f"{base}-PERPETUAL"

        with self._lock:
            spot = self._index_prices.get(index_key, 0)
            if spot == 0: spot = self._index_prices.get(perp_key, 0)
            if spot == 0 and not is_usd: spot = self._index_prices.get(f"{base}_USDC", 0)
            
            if spot == 0: return {}

            lo = spot * (1 + min_pct / 100)
            hi = spot * (1 + max_pct / 100)
            
            subs_to_send = [f"ticker.{index_key}.100ms", f"ticker.{perp_key}.100ms"]
            structure = {}

            for inst in self._instruments_by_tab.get(tab_name, []):
                nm = inst['instrument_name']
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

    # --- INTERNAL LOGIC ---

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
            print(f"[FeedManager] Loaded {len(instruments)} for {tab} via {source}")

    def _bootstrap_prices(self):
        required_tickers = set()
        for cfg in self._market_config:
            base = cfg['base_symbol']
            is_usd = cfg['settlement'] == 'usd'
            source = cfg.get('source', 'deribit').lower()
            
            if is_usd:
                required_tickers.add((source, f"{base}_USDC"))
                required_tickers.add((source, f"{base}_USDC-PERPETUAL"))
            else:
                required_tickers.add((source, f"{base}-PERPETUAL"))
                required_tickers.add((source, f"{base}_USDC"))

        for source, ticker in required_tickers:
            adapter = self.adapters.get(source)
            if adapter:
                px = adapter.get_latest_price(ticker)
                if px > 0:
                    with self._lock:
                        self._index_prices[ticker] = px
                    print(f"[FeedManager] Bootstrapped {ticker}: {px}")

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

    def on_adapter_reconnect(self, source_name):
        pass

    def _load_keys_from_file(self):
        """Fallback: Try to load keys from the given file path."""
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