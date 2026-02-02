import websocket
import json
import threading
import time
import requests
from ..base import ExchangeAdapter

class DeribitAdapter(ExchangeAdapter):
    WS_URL = "wss://www.deribit.com/ws/api/v2"
    HTTP_URL = "https://www.deribit.com/api/v2"

    def __init__(self, manager, api_id, api_secret):
        super().__init__(manager)
        self.name = "deribit"
        self.api_id = api_id
        self.api_secret = api_secret
        self.ws = None
        self._stop_event = threading.Event()

    def start(self):
        t = threading.Thread(target=self._ws_loop, daemon=True)
        t.start()

    def stop(self):
        self._stop_event.set()
        if self.ws: self.ws.close()

    def get_instruments(self, tab_config) -> list:
        """Fetch instruments using Deribit's specific API parameters."""
        base = tab_config['base_symbol']
        settlement = tab_config['settlement']
        
        # Deribit uses 'USDC' as currency param for USD-settled options
        api_currency = base if settlement == 'coin' else "USDC"
        
        try:
            url = f"{self.HTTP_URL}/public/get_instruments?currency={api_currency}&kind=option&expired=false"
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200: 
                print(f"[Deribit] HTTP Inst Failed: {resp.status_code}")
                return []
            
            data = resp.json().get('result', [])
            
            # Client-side filtering
            filtered = []
            for i in data:
                nm = i['instrument_name']
                if settlement == 'usd':
                    if nm.startswith(f"{base}_USDC-"): filtered.append(i)
                else:
                    if nm.startswith(f"{base}-") and "_USDC" not in nm: filtered.append(i)
            
            return filtered
        except Exception as e:
            print(f"[Deribit] HTTP Exception: {e}")
            return []

    def get_latest_price(self, instrument_name: str) -> float:
        """Fetch a specific ticker price via REST."""
        try:
            url = f"{self.HTTP_URL}/public/ticker?instrument_name={instrument_name}"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                data = resp.json().get('result', {})
                # prioritize index_price, fall back to last_price
                return data.get('index_price') or data.get('last_price') or 0.0
        except:
            pass
        return 0.0

    def get_reference_tickers(self, tab_config) -> list:
        base = tab_config['base_symbol']
        is_usd = tab_config['settlement'] == 'usd'
        
        if is_usd:
            # For USD settlement, we watch the USDC Pair and Linear Perp
            return [f"{base}_USDC", f"{base}_USDC-PERPETUAL"]
        else:
            # For Coin settlement, we watch the Inverse Perp and maybe USD index
            return [f"{base}-PERPETUAL", f"{base}_USDC"]
    
    def subscribe(self, channels):
        if self.ws and self.ws.sock and self.ws.sock.connected:
            msg = {
                "jsonrpc": "2.0",
                "method": "public/subscribe",
                "id": 10,
                "params": {"channels": channels}
            }
            try: self.ws.send(json.dumps(msg))
            except: pass

    def _ws_loop(self):
        while not self._stop_event.is_set():
            try:
                print(f"[{self.name}] Connecting WS...")
                self.ws = websocket.WebSocketApp(
                    self.WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=lambda ws, e: None,
                    on_close=lambda ws, *a: setattr(self, 'connected', False)
                )
                self.ws.run_forever()
            except Exception as e:
                print(f"[{self.name}] Crash: {e}")
            time.sleep(2)

    def _on_open(self, ws):
        print(f"[{self.name}] Connected.")
        if self.api_id:
            auth = {
                "jsonrpc": "2.0", "method": "public/auth", "id": 99,
                "params": {"grant_type": "client_credentials", "client_id": self.api_id, "client_secret": self.api_secret}
            }
            try: ws.send(json.dumps(auth))
            except: pass
        
        self.connected = True
        self.manager.on_adapter_reconnect(self.name)

    def _on_message(self, ws, msg):
        try: d = json.loads(msg)
        except: return
        
        if 'params' in d:
            self.manager.ingest_ticker(d['params']['data'])