import time
import sys
import os
import pandas as pd
import importlib
from datetime import datetime, timedelta

# Note: The following imports are for notebook environments.
# In a pure Python script, they might not display anything or might raise an error
# if you are not in a graphical environment.
try:
    from IPython.display import clear_output, display
except ImportError:
    print("Could not import IPython.display. Output will not be cleared.")
    def clear_output(wait=False):
        # Basic clear screen for terminals
        os.system('cls' if os.name == 'nt' else 'clear')
    def display(df):
        print(df.to_string())

# Ensure src is in path
sys.path.append(os.path.abspath("src"))
import market_feed.manager
import market_feed.adapters.deribit
import market_feed.adapters.bloomberg

# Force reload to pick up changes
importlib.reload(market_feed.adapters.deribit)
importlib.reload(market_feed.adapters.bloomberg)
importlib.reload(market_feed.manager)
from market_feed import FeedManager

print("[OK] Environment Ready (Modules Reloaded).")

def get_local_reference_tickers(config):
    """
    Local helper to determine reference tickers based on configuration.
    Replacing the adapter method as this is now user-space logic.
    """
    base = config['base_symbol']
    source = config.get('source', '').lower()
    
    if source == 'deribit':
        is_usd = config.get('settlement') == 'usd'
        if is_usd:
            # For USD settlement (Linear)
            # Internal names based on feed_instruments.csv convention (if any)
            # Or just raw external names if no mapping exists
            return [f"{base}.USDC", f"{base}_USDC-PERPETUAL"]
        else:
            # Coin settlement (Inverse)
            # Internal names: BTC.PERP -> BTC-PERPETUAL, BTC -> BTC_USDC
            return [f"{base}.PERP", base]
            
    elif source == 'bloomberg':
        return [base]
        
    return []

def find_and_subscribe_options(manager, undl_name, min_days=3):
    """
    Analyzes the feed to find specific options based on the current spot price.
    Returns a list of option tickers to subscribe to.
    """
    cfg = next((c for c in manager.market_config if c['register_name'] == undl_name), None)
    if not cfg: return []
    
    source = cfg.get('source', 'deribit').lower()
    account = cfg.get('account', 'default').lower()
    adapter_key = f"{source}:{account}"
    adapter = manager.adapters.get(adapter_key)

    if not adapter:
        print(f"   [WARN] Could not find adapter for {adapter_key}. Skipping options.")
        return []

    ref_tickers = get_local_reference_tickers(cfg)
    
    # 1. Get Spot Price (from cache or REST)
    spot_price = 0
    for t in ref_tickers:
        # Check Manager Cache (Websocket data)
        p = manager._index_prices.get(t, 0)
        if p == 0 and t in manager._tickers:
             p = manager._tickers[t].get('last_price', 0)
        
        # If still 0, try adapter REST fallback
        if p == 0:
            p = adapter.get_latest_price(t)
            
        if p > 0:
            spot_price = p
            # print(f"   Found Spot for {undl_name}: {t} @ {spot_price}")
            break
    
    if spot_price == 0:
        print(f"   [WARN] Could not determine spot price for {undl_name}. Skipping options.")
        return []

    # 2. Get Full Chain Details (Once)
    options_data = manager.get_option_chain_details(undl_name)
    if not options_data:
        print(f"   [WARN] No options found for {undl_name}.")
        return []

    # 3. Find Target Expiry (Min 3 days, but shortest)
    # Extract unique expiries from the parsed data
    unique_expiries = list(set(d['expiry'] for d in options_data))
    
    now = datetime.now()
    target_date_obj = now + timedelta(days=min_days)
    
    selected_expiry = None
    valid_expiries = []
    
    for d_str in unique_expiries:
        try:
            d_obj = datetime.strptime(d_str, "%d%b%y")
            if d_obj >= target_date_obj:
                valid_expiries.append((d_obj, d_str))
        except:
            pass
            
    if not valid_expiries:
        print(f"   [WARN] No expiry found >= {min_days} days for {undl_name}.")
        return []
        
    # Sort by date and pick the first (shortest)
    valid_expiries.sort()
    selected_expiry = valid_expiries[0][1]
    # print(f"   Selected Expiry: {selected_expiry}")

    # 4. Find Closest Calls/Puts
    calls = []
    puts = []
    
    for opt in options_data:
        if opt['expiry'] != selected_expiry: continue
        
        k = opt['strike']
        nm = opt['symbol']
        kind = opt['type']
        
        if kind == 'C': calls.append((k, nm))
        elif kind == 'P': puts.append((k, nm))

    calls.sort() # Ascending
    puts.sort(reverse=True) # Descending

    target_call = next((nm for k, nm in calls if k > spot_price), None)
    target_put = next((nm for k, nm in puts if k < spot_price), None)
    
    targets = []
    if target_call: targets.append(target_call)
    if target_put: targets.append(target_put)
    
    return targets


# --- CONFIGURATION ---
# Keys path can be None if using public data or existing env vars
feed = FeedManager(keys_path="keys.json", instrument_config_path="feed_instruments.csv", log_level=2)
print("[OK] FeedManager Initialized (No Config).")

active_subscriptions = set()

def test_subscription_map(manager, register_name):
    print(f"   [TEST] Testing get_subscription_map for {register_name}...")
    
    expiries = manager.get_expiries_for(register_name)
    if not expiries:
        print("   [TEST] No expiries found. Skipping.")
        return

    target_date = expiries[0]
    
    # Case A: With explicit spot
    # We use a dummy spot that should definitely return some strikes if the chain is populated
    # Let's try to get a rough center from the first available strike if we can't guess, 
    # but 50000 is okay for BTC, maybe not for ETH.
    # Let's just use the fallback spot if available to define a "reasonable" explicit spot.
    
    # We'll just pass a hardcoded reasonable value for BTC/ETH to ensure it works syntactically
    dummy_spot = 50000 if 'BTC' in register_name else (2000 if 'ETH' in register_name else 400)
    
    m1 = manager.get_subscription_map(register_name, [target_date], -10, 10, spot_price=dummy_spot)
    count_1 = len(m1.get(target_date, {}).get('strikes', []))
    print(f"   [TEST] Explicit Spot ({dummy_spot}): Found {count_1} strikes.")

    # Case B: Without explicit spot (Fallback)
    m2 = manager.get_subscription_map(register_name, [target_date], -5, 5)
    
    if m2:
        strikes = m2.get(target_date, {}).get('strikes', [])
        print(f"   [TEST] Auto Spot: Found {len(strikes)} strikes around detected spot.")
    else:
        print("   [TEST] Auto Spot: Returned empty (Spot likely not ready yet).")

def activate_feed_stage(config, stage_name):
    print(f"[INFO] ACTIVATING STAGE: {stage_name}")
    
    # 1. Register Adapter & Underlying
    source = config.get('source', 'deribit')
    account = config.get('account', 'default')
    
    feed.register_adapter(source, account)
    feed.register_market(config)
    
    # Since we need option data for this script logic:
    feed.initialize_option_chain(config['register_name'])
    
    # 2. Get Ref Tickers & Subscribe (Subscription is MANUAL now)
    # We still need to get the ref names to add to our "monitor" list
    source_lower = source.lower()
    account_lower = account.lower()
    adapter_key = f"{source_lower}:{account_lower}"
    adapter = feed.adapters.get(adapter_key)

    if not adapter:
        print(f"   [WARN] Could not find adapter for {adapter_key}. Skipping subscription.")
        return
        
    refs = get_local_reference_tickers(config)
    
    if refs:
        print(f"   Subscribing & Monitoring Refs: {refs}")
        feed.subscribe_custom(source, refs)
        active_subscriptions.update(refs)
    
    # 3. Wait a moment for Spot Price to arrive (needed for option selection)
    print("   Waiting 2s for spot data...")
    time.sleep(2)
    
    # TEST: Check subscription map logic
    test_subscription_map(feed, config['register_name'])
    
    # 4. Find & Subscribe to Options
    opts = find_and_subscribe_options(feed, config['register_name'])
    if opts:
        print(f"   Subscribing to Options: {opts}")
        feed.subscribe_custom(source, opts)
        active_subscriptions.update(opts)
    else:
        print("   No options selected (Spot missing or no chain).")

def display_monitor(duration=10):
    end = time.time() + duration
    while time.time() < end:
        clear_output(wait=True)
        snap = feed.get_snapshot()
        
        data = []
        for t in sorted(list(active_subscriptions)):
            row = {'Ticker': t, 'Bid': '-', 'Ask': '-', 'Last': '-'}
            
            # Info from Tickers (Best source for Bid/Ask)
            if t in snap.tickers:
                tk = snap.tickers[t]
                row['Bid'] = tk.get('best_bid_price', '-')
                row['Ask'] = tk.get('best_ask_price', '-')
                row['Last'] = tk.get('last_price', '-')
                
            # Fallback to Index Prices for Last if missing
            if row['Last'] == '-' and t in snap.index_prices:
                row['Last'] = snap.index_prices[t]
                
            data.append(row)
            
        df = pd.DataFrame(data)
        print(f"Time Remaining in Stage: {int(end - time.time())}s")
        if not df.empty:
            # Simple formatting
            print(df.to_string(index=False))
            if (df['Bid'] == '-').any():
                print('Available Keys:', list(snap.tickers.keys()))
        else:
            print("No data yet...")
            
        time.sleep(1)

# --- MAIN EXECUTION FLOW ---
try:
    # Start the engine
    feed.start_stream()

    # STEP 1: BTC
    activate_feed_stage({
        "register_name": "BTC_DERIBIT",
        "base_symbol": "BTC",
        "settlement": "coin",
        "source": "deribit"
    }, "1. BTC (Deribit)")
    display_monitor(15)

    # TEST UNSUBSCRIBE
    print("\n[TEST] Testing unsubscribe_options for BTC_DERIBIT...")
    feed.unsubscribe_options("BTC_DERIBIT")
    print("   Options unsubscribed. Displaying monitor for 10s to verify stability...")
    display_monitor(10)

    # STEP 2: SPY & QQQ
    activate_feed_stage({
        "register_name": "SPY_BBG",
        "base_symbol": "SPY",
        "settlement": "usd",
        "source": "bloomberg"
    }, "2a. SPY (Bloomberg)")
    
    activate_feed_stage({
        "register_name": "QQQ_BBG",
        "base_symbol": "QQQ",
        "settlement": "usd",
        "source": "bloomberg"
    }, "2b. QQQ (Bloomberg)")
    display_monitor(15)

    # STEP 3: ETH
    activate_feed_stage({
        "register_name": "ETH_DERIBIT",
        "base_symbol": "ETH",
        "settlement": "coin",
        "source": "deribit"
    }, "4. ETH (Deribit)")
    display_monitor(30)

except KeyboardInterrupt:
    print("[STOP] Interrupted by user.")
finally:
    feed.stop_stream()
    print("[OK] Feed Stopped.")
