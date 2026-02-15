import threading
import time
import re
import csv
import os
from datetime import datetime
from ..base import ExchangeAdapter

# --- SAFE IMPORT (Prevents crash if blpapi is missing) ---
try:
    import blpapi
    HAS_BLPAPI = True
except ImportError:
    HAS_BLPAPI = False
    blpapi = None

class BloombergAdapter(ExchangeAdapter):
    """
    Acts as a Translation Layer:
    - Input:  Your App Ticker (SPY-20FEB26-688-C)
    - Output: Bloomberg Ticker (SPY US 02/20/26 C688 Equity)
    """
    def __init__(self, manager):
        super().__init__(manager)
        
        self.name = "bloomberg"
        self.session = None
        self._stop_event = threading.Event()
        self.active_subscriptions = set()
        
        # REGEX: Matches "SPY US 02/20/26 C688 Equity" or "SPX US ... Index"
        # Group 1: Symbol (SPY)
        # Group 2: Date (02/20/26)
        # Group 3: Type (C)
        # Group 4: Strike (688)
        # Group 5: Asset Class (Equity/Index)
        self.bbg_regex = re.compile(r"^(\w+)\s+\w+\s+(\d{1,2}/\d{1,2}/\d{2})\s+([CP])([\d\.]+)\s+(Equity|Index)$")
        
        # REGEX: Matches Underlyings like "SPY US Equity", "0700 HK Equity"
        # Group 1: Symbol (SPY, 0700)
        self.bbg_underlying_regex = re.compile(r"^(\w+)\s+(?:\w+\s+)?(Equity|Index|Comdty)$")

        # --- CONFIGURATION TABLES ---
        # 1. Exact Match: These are Indices (Suffix: Index)
        self.index_tickers = {
            "SPX", "NDX", "VIX", "RTY", "HSI", "NKY", "UKX", "CAC", "DAX", "SX5E"
        }
        # --- CONFIGURATION ---
        self.exact_map = {}       # Symbol -> Full Bloomberg Ticker
        self.index_tickers = set() # Symbols that get " Index" suffix
        self.future_prefixes = set() # Prefixes for futures logic
        
        self._load_config()

        if not HAS_BLPAPI:
            print("[Bloomberg] Warning: 'blpapi' not installed. Adapter disabled.")
            return

    def _load_config(self):
        """Loads feed_instruments.csv and looks for column 'bloomberg'."""
        csv_path = os.path.join(os.path.dirname(__file__), 'feed_instruments.csv')
        if not os.path.exists(csv_path):
            # Defaults if file missing
            self.index_tickers = {"SPX", "NDX", "VIX", "RTY", "HSI", "NKY", "UKX", "CAC", "DAX", "SX5E"}
            self.future_prefixes = {"ES", "NQ", "YM", "QR", "HI", "NK", "VG", "GX", "JB", "RX", "VX"}
            return

        try:
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                # Check if our column exists
                if self.name not in reader.fieldnames:
                    return

                for row in reader:
                    sym = row['Symbol'].strip()
                    val = row.get(self.name, '').strip()
                    
                    if not val: continue

                    if val.lower() == 'index':
                        self.index_tickers.add(sym)
                    elif val.lower() == 'futureprefix':
                        self.future_prefixes.add(sym)
                    elif val.lower().startswith('exact:'):
                        self.exact_map[sym] = val.split(':', 1)[1].strip()
        except Exception as e:
            print(f"[Bloomberg] Error loading config: {e}")

    def start(self):
        if not HAS_BLPAPI: return
        t = threading.Thread(target=self._run_session, daemon=True)
        t.start()

    def stop(self):
        self._stop_event.set()
        if self.session: self.session.stop()

    def get_reference_tickers(self, tab_config) -> list:
        # Keep it clean: The Manager expects "SPX" or "SPY", not "SPX US Equity"
        return [tab_config['base_symbol']]

    def get_latest_price(self, instrument_name: str) -> float:
        """
        Fetches price for 'SPY' or 'SPY-20FEB...' but handles the conversion internally.
        """
        if not HAS_BLPAPI: return 0.0
        
        # 1. Convert Manager Name -> Bloomberg Name
        bbg_ticker = self._convert_to_bbg(instrument_name)
        if not bbg_ticker: return 0.0

        price = 0.0
        session = blpapi.Session()
        if session.start() and session.openService("//blp/refdata"):
            try:
                service = session.getService("//blp/refdata")
                req = service.createRequest("ReferenceDataRequest")
                req.append("securities", bbg_ticker)
                req.append("fields", "LAST_PRICE")
                session.sendRequest(req)
                
                # Simple synchronous wait
                while True:
                    ev = session.nextEvent(1000)
                    for msg in ev:
                        if msg.hasElement("securityData"):
                            # Safety drill-down
                            sec = msg.getElement("securityData").getValueAsElement(0)
                            if sec.hasElement("fieldData"):
                                f = sec.getElement("fieldData")
                                if f.hasElement("LAST_PRICE"):
                                    price = f.getElementAsFloat("LAST_PRICE")
                    if ev.eventType() == blpapi.Event.RESPONSE: break
            except: pass
            finally: session.stop()
        
        return price

    def get_option_chain(self, tab_config) -> list:
        if not HAS_BLPAPI: return []
        
        base = tab_config['base_symbol']
        # Smartly construct root ticker
        root_ticker = self._convert_to_bbg(base)
        
        print(f"[Bloomberg] Fetching chain for {root_ticker}...")
        results = []
        
        session = blpapi.Session()
        if not session.start(): return []
        if not session.openService("//blp/refdata"): return []
        
        try:
            service = session.getService("//blp/refdata")
            req = service.createRequest("ReferenceDataRequest")
            req.append("securities", root_ticker)
            req.append("fields", "OPT_CHAIN") # Fetch the list of all options
            session.sendRequest(req)
            
            while True:
                ev = session.nextEvent(2000)
                if ev.eventType() in [blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE]:
                    for msg in ev:
                        if msg.hasElement("securityData"):
                            arr = msg.getElement("securityData")
                            for i in range(arr.numValues()):
                                fd = arr.getValueAsElement(i).getElement("fieldData")
                                if fd.hasElement("OPT_CHAIN"):
                                    chain = fd.getElement("OPT_CHAIN")
                                    for j in range(chain.numValues()):
                                        raw_bbg = chain.getValueAsElement(j).getElementAsString("Security Description")
                                        
                                        # CRITICAL: Parse IMMEDIATELY. 
                                        # Manager never sees "SPY US ...", only "SPY-20FEB..."
                                        parsed = self._parse_bbg_to_app(raw_bbg)
                                        if parsed: results.append(parsed)
                    if ev.eventType() == blpapi.Event.RESPONSE: break
                if ev.eventType() == blpapi.Event.TIMEOUT: break
        finally:
            session.stop()
            
        return results

    def subscribe(self, instruments: list):
        if not self.session or not HAS_BLPAPI: return
        
        subs = blpapi.SubscriptionList()
        count = 0  # <--- Track how many we actually add
        for app_ticker in instruments:
            # 1. Translate Manager Name -> Bloomberg Name
            bbg_ticker = self._convert_to_bbg(app_ticker)
            
            if bbg_ticker and bbg_ticker not in self.active_subscriptions:
                # 2. Attach the APP TICKER as the Correlation ID
                # This ensures when data comes back, it is tagged with YOUR format.
                cid = blpapi.CorrelationId(app_ticker)
                
                subs.add(bbg_ticker, "LAST_PRICE,BID,ASK,SIZE_BID,SIZE_ASK", correlationId=cid)
                self.active_subscriptions.add(bbg_ticker)
                count += 1 # <--- Increment
        
        # FIX: Only send request if we actually have new topics
        if count > 0:
            try: self.session.subscribe(subs)
            except: pass

    # --- TRANSLATION LOGIC ---

    def _convert_to_bbg(self, name):
        """Smart router to convert any App name to Bloomberg format."""
        # 0. Exact Map Override (Highest Priority)
        # Handles cases like "TENCENT" -> "0700 HK Equity" defined in CSV
        if name in self.exact_map:
            return self.exact_map[name]

        # Case A: Complex Option "SPY-20FEB26-688-C"
        if "-" in name:
            return self._to_bbg_option_ticker(name)
        
        # Case B: International Equity "0700.HK" -> "0700 HK Equity"
        if "." in name:
            parts = name.rsplit('.', 1)
            if len(parts) == 2:
                return f"{parts[0]} {parts[1]} Equity"

        # Case C: Simple Ticker
        if " " not in name:
            # 1. Exact Index Match
            if name in self.index_tickers:
                return f"{name} Index"
            
            # 2. Future Prefix Match (Must end in digit, e.g. ESU6)
            for prefix in self.future_prefixes:
                if name.startswith(prefix) and name[-1].isdigit():
                    return f"{name} Index"
            
            # 3. Default to US Equity
            return f"{name} US Equity"
            
        # Case C: Already Bloomberg format (fallback)
        return name

    def _parse_bbg_to_app(self, bbg_ticker):
        """
        Input:  SPY US 02/20/26 C688 Equity
        Output: { instrument_name: SPY-20FEB26-688-C, ... }
        """
        match = self.bbg_regex.match(bbg_ticker)
        if match:
            sym, date_str, kind, strike_str, _ = match.groups()
            
            try:
                # Parse Date: 02/20/26 -> datetime
                dt = datetime.strptime(date_str, "%m/%d/%y")
                # Format Date: 20FEB26 (Standard App Format)
                app_date = dt.strftime("%d%b%y").upper()
                exp_ts = dt.timestamp() * 1000
            except: return None
            
            # Construct the Clean Name
            app_name = f"{sym}-{app_date}-{float(strike_str):g}-{kind}"
            
            return {
                "instrument_name": app_name,
                "expiration_timestamp": exp_ts,
                "base_currency": sym,
                "quote_currency": "USD"
            }
        
        # Fallback: Try to parse as Underlying (SPY US Equity -> SPY)
        match_und = self.bbg_underlying_regex.match(bbg_ticker)
        if match_und:
            sym = match_und.group(1)
            return {
                "instrument_name": sym,
                "base_currency": sym,
                "quote_currency": "USD"
            }
            
        return None

    def _to_bbg_option_ticker(self, app_ticker):
        """
        Input:  SPY-20FEB26-688-C
        Output: SPY US 02/20/26 C688 Equity
        """
        try:
            parts = app_ticker.split('-')
            if len(parts) < 4: return None
            sym, date_str, strike, kind = parts
            
            # Convert 20FEB26 -> 02/20/26
            dt = datetime.strptime(date_str, "%d%b%y")
            bbg_date = dt.strftime("%m/%d/%y")
            
            return f"{sym} US {bbg_date} {kind}{strike} Equity"
        except:
            return None

    def _run_session(self):
        options = blpapi.SessionOptions()
        options.setServerHost("localhost")
        options.setServerPort(8194)
        self.session = blpapi.Session(options)
        if not self.session.start(): return
        if not self.session.openService("//blp/mktdata"): return
        self.connected = True
        
        while not self._stop_event.is_set():
            event = self.session.nextEvent(100)
            if event.eventType() == blpapi.Event.SUBSCRIPTION_DATA:
                for msg in event:
                    self._handle_msg(msg)

    def _handle_msg(self, msg):
        # MAGIC: We retrieve the App Ticker from the correlation ID.
        # This guarantees the Manager receives "SPY-20FEB..." and not "SPY US..."
        app_ticker = msg.correlationId().value()
        
        def get_f(f): return msg.getElementAsFloat(f) if msg.hasElement(f) else None
        
        data = {
            'instrument_name': app_ticker,
            'last_price': get_f("LAST_PRICE"),
            'best_bid_price': get_f("BID"),
            'best_ask_price': get_f("ASK"),
            'best_bid_amount': get_f("SIZE_BID"),
            'best_ask_amount': get_f("SIZE_ASK"),
            'timestamp': time.time() * 1000
        }
        # Only ingest if we have valid data
        if data['last_price'] or data['best_bid_price']:
            self.manager.ingest_ticker(data)