# import blpapi
import threading
import time
import re
from datetime import datetime
from ..base import ExchangeAdapter

class BloombergAdapter(ExchangeAdapter):
    """
    Connects to Bloomberg Terminal via Desktop API (Port 8194).
    """
    def __init__(self, manager):
        super().__init__(manager)
        self.name = "bloomberg"
        self.session = None
        self._stop_event = threading.Event()
        self.active_subscriptions = set()
        
        # Regex to parse BBG Tickers: "SPY US 12/20/24 P500 Equity"
        # Group 1: Symbol, Group 2: M/D/y, Group 3: Type(C/P), Group 4: Strike
        self.bbg_regex = re.compile(r"^(\w+)\s+\w+\s+(\d{1,2}/\d{1,2}/\d{2})\s+([CP])([\d\.]+)\s+Equity$")

    def start(self):
        t = threading.Thread(target=self._run_session, daemon=True)
        t.start()

    def stop(self):
        self._stop_event.set()
        if self.session: self.session.stop()

    def get_reference_tickers(self, tab_config) -> list:
        # Returns the raw base symbol (e.g., "SPY")
        base = tab_config['base_symbol']
        return [base]

    def get_instruments(self, tab_config) -> list:
        """
        Fetches OPT_CHAIN from Bloomberg to get all strikes/expiries.
        """
        base = tab_config['base_symbol']
        root_ticker = f"{base} US Equity"
        
        print(f"[Bloomberg] Fetching option chain for {root_ticker}...")
        
        results = []
        try:
            # We need a temporary session for this synchronous request if the main one isn't ready
            # Or use the main session if you handle threading carefully. 
            # For safety/simplicity here, we create a specialized request session.
            session = blpapi.Session()
            if not session.start(): return []
            if not session.openService("//blp/refdata"): return []
            
            refDataService = session.getService("//blp/refdata")
            request = refDataService.createRequest("ReferenceDataRequest")
            request.append("securities", root_ticker)
            request.append("fields", "OPT_CHAIN") # <--- The Magic Field
            
            # Optional: Filter by exchange if needed, but usually not required for SPY
            # overrides = request.getElement("overrides")
            # ovr = overrides.appendElement()
            # ovr.setElement("fieldId", "CHAIN_EXCHANGE")
            # ovr.setElement("value", "US") 

            session.sendRequest(request)
            
            # Process Response
            while True:
                ev = session.nextEvent(2000)
                if ev.eventType() == blpapi.Event.RESPONSE or ev.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                    for msg in ev:
                        if msg.hasElement("securityData"):
                            sec_data_array = msg.getElement("securityData")
                            for i in range(sec_data_array.numValues()):
                                sec_data = sec_data_array.getValueAsElement(i)
                                if sec_data.hasElement("fieldData"):
                                    field_data = sec_data.getElement("fieldData")
                                    if field_data.hasElement("OPT_CHAIN"):
                                        chain_array = field_data.getElement("OPT_CHAIN")
                                        for j in range(chain_array.numValues()):
                                            bbg_ticker = chain_array.getValueAsElement(j).getElementAsString("Security Description")
                                            
                                            # Parse "SPY US ..." -> "SPY-DATE-STRIKE-TYPE"
                                            parsed = self._parse_bbg_to_app(bbg_ticker)
                                            if parsed:
                                                results.append(parsed)
                    if ev.eventType() == blpapi.Event.RESPONSE: break
                if ev.eventType() == blpapi.Event.TIMEOUT: break
                
        except Exception as e:
            print(f"[Bloomberg] Error fetching chain: {e}")
        finally:
            session.stop()
            
        print(f"[Bloomberg] Found {len(results)} contracts for {base}")
        return results

    def get_latest_price(self, instrument_name: str) -> float:
        # NOTE: If instrument_name is in APP format (SPY-...), convert to BBG first
        if "-" in instrument_name and " " not in instrument_name:
            instrument_name = self._to_bbg_ticker(instrument_name)
            
        # (Simplified synchronous snapshot logic)
        # In production, cache this or use the main data stream
        return 0.0 # Placeholder: Rely on WebSocket stream for prices

    def subscribe(self, channels: list):
        if not self.session: return
        
        subs = blpapi.SubscriptionList()
        for c in channels:
            # Channel comes in as "ticker.SPY-20FEB26-500-C.100ms"
            # 1. Strip prefix/suffix
            clean = c.replace("ticker.", "").replace(".100ms", "")
            
            # 2. Convert App Format -> BBG Format
            # "SPY-20FEB26-500-C" -> "SPY US 02/20/26 C500 Equity"
            bbg_ticker = self._to_bbg_ticker(clean)
            
            if bbg_ticker and bbg_ticker not in self.active_subscriptions:
                # Subscribe to Bid, Ask, Last
                fields = "LAST_PRICE,BID,ASK,SIZE_BID,SIZE_ASK"
                subs.add(bbg_ticker, fields, correlationId=blpapi.CorrelationId(clean)) # key is Clean App Name
                self.active_subscriptions.add(bbg_ticker)
        
        try: self.session.subscribe(subs)
        except: pass

    # --- HELPERS ---

    def _parse_bbg_to_app(self, bbg_ticker):
        """
        Input:  SPY US 12/20/24 P500 Equity
        Output: {instrument_name: SPY-20DEC24-500-P, expiration: ...}
        """
        match = self.bbg_regex.match(bbg_ticker)
        if not match: return None
        
        sym, date_str, kind, strike_str = match.groups()
        
        # Parse Date: 12/20/24 -> datetime -> 20DEC24
        try:
            dt = datetime.strptime(date_str, "%m/%d/%y")
            app_date = dt.strftime("%d%b%y").upper() # 20DEC24
            exp_ts = dt.timestamp() * 1000
        except: return None
        
        app_name = f"{sym}-{app_date}-{float(strike_str):g}-{kind}"
        
        return {
            "instrument_name": app_name,
            "expiration_timestamp": exp_ts,
            "base_currency": sym,
            "quote_currency": "USD"
        }

    def _to_bbg_ticker(self, app_ticker):
        """
        Input:  SPY-20DEC24-500-P
        Output: SPY US 12/20/24 P500 Equity
        """
        # Handle Reference Ticker (SPY US Equity)
        if " US Equity" in app_ticker: return app_ticker
        
        parts = app_ticker.split('-')
        if len(parts) < 4: return app_ticker # Fallback
        
        sym, date_str, strike, kind = parts
        
        # Convert 20DEC24 -> 12/20/24
        try:
            dt = datetime.strptime(date_str, "%d%b%y")
            bbg_date = dt.strftime("%m/%d/%y")
        except: return None
        
        return f"{sym} US {bbg_date} {kind}{strike} Equity"

    def _run_session(self):
        # Standard BBG Event Loop (Same as previous answer)
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
        # Retrieve the App Ticker we stored in correlationId
        app_ticker = msg.correlationId().value()
        
        def get_f(f): return msg.getElementAsFloat(f) if msg.hasElement(f) else None
        
        data = {
            'instrument_name': app_ticker, # Pass the APP format to the manager
            'last_price': get_f("LAST_PRICE"),
            'best_bid_price': get_f("BID"),
            'best_ask_price': get_f("ASK"),
            'best_bid_amount': get_f("SIZE_BID"),
            'best_ask_amount': get_f("SIZE_ASK"),
            'timestamp': time.time() * 1000
        }
        if data['last_price'] or data['best_bid_price']:
            self.manager.ingest_ticker(data)