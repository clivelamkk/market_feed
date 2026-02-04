import threading
import time
import re
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
        if not HAS_BLPAPI:
            print("[Bloomberg] Warning: 'blpapi' not installed. Adapter disabled.")
            return

        self.name = "bloomberg"
        self.session = None
        self._stop_event = threading.Event()
        self.active_subscriptions = set()
        
        # REGEX: Matches "SPY US 02/20/26 C688 Equity"
        # Group 1: Symbol (SPY)
        # Group 2: Date (02/20/26)
        # Group 3: Type (C)
        # Group 4: Strike (688)
        self.bbg_regex = re.compile(r"^(\w+)\s+\w+\s+(\d{1,2}/\d{1,2}/\d{2})\s+([CP])([\d\.]+)\s+Equity$")

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

    def get_instruments(self, tab_config) -> list:
        if not HAS_BLPAPI: return []
        
        base = tab_config['base_symbol']
        # Construct the root ticker for chain lookup (e.g. "SPY US Equity")
        root_ticker = f"{base} US Equity"
        
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
        for app_ticker in instruments:
            # 1. Translate Manager Name -> Bloomberg Name
            bbg_ticker = self._convert_to_bbg(app_ticker)
            
            if bbg_ticker and bbg_ticker not in self.active_subscriptions:
                # 2. Attach the APP TICKER as the Correlation ID
                # This ensures when data comes back, it is tagged with YOUR format.
                cid = blpapi.CorrelationId(app_ticker)
                
                subs.add(bbg_ticker, "LAST_PRICE,BID,ASK,SIZE_BID,SIZE_ASK", correlationId=cid)
                self.active_subscriptions.add(bbg_ticker)
        
        try: self.session.subscribe(subs)
        except: pass

    # --- TRANSLATION LOGIC ---

    def _convert_to_bbg(self, name):
        """Smart router to convert any App name to Bloomberg format."""
        # Case A: Complex Option "SPY-20FEB26-688-C"
        if "-" in name:
            return self._to_bbg_option_ticker(name)
        
        # Case B: Simple Equity "SPY" -> "SPY US Equity"
        # Prevents "SPY" from failing in get_latest_price
        if " " not in name:
            return f"{name} US Equity"
            
        # Case C: Already Bloomberg format (fallback)
        return name

    def _parse_bbg_to_app(self, bbg_ticker):
        """
        Input:  SPY US 02/20/26 C688 Equity
        Output: { instrument_name: SPY-20FEB26-688-C, ... }
        """
        match = self.bbg_regex.match(bbg_ticker)
        if not match: return None
        
        sym, date_str, kind, strike_str = match.groups()
        
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