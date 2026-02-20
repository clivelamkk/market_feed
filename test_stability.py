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

print("✅ Environment Ready (Modules Reloaded).")


def find_and_subscribe_options(manager, undl_name, min_days=3):
    """
    Analyzes the feed to find specific options based on the current spot price.
    Returns a list of option tickers to subscribe to.
    """
    cfg = next((c for c in manager.market_config if c['register_name'] == undl_name), None)
    if not cfg: return []
    
    source = cfg.get('source', 'deribit').lower()
    adapter = manager.adapters.get(source)
    ref_tickers = adapter.get_reference_tickers(cfg)
    
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
        print(f"   ⚠️ Could not determine spot price for {undl_name}. Skipping options.")
        return []

    # 2. Get Full Chain Details (Once)
    options_data = manager.get_option_chain_details(undl_name)
    if not options_data:
        print(f"   ⚠️ No options found for {undl_name}.")
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
        print(f"   ⚠️ No expiry found >= {min_days} days for {undl_name}.")
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
feed = FeedManager(keys_path="keys.json", log_level=2)
print("✅ FeedManager Initialized (No Config).")

active_subscriptions = set()

def activate_feed_stage(config, stage_name):
    print(f"🚀 ACTIVATING STAGE: {stage_name}")
    
    # 1. Add Feed
    feed.register_market(config)
    
    # 2. Get Ref Tickers & Subscribe IMMEDIATELY
    source = config['source']
    adapter = feed.adapters.get(source)
    refs = adapter.get_reference_tickers(config)
    
    if refs:
        print(f"   Subscribing to Refs: {refs}")
        feed.subscribe_custom(source, refs)
        active_subscriptions.update(refs)
    
    # 3. Wait a moment for Spot Price to arrive (needed for option selection)
    print("   Waiting 2s for spot data...")
    time.sleep(2)
    
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
    print("🛑 Interrupted by user.")
finally:
    feed.stop_stream()
    print("✅ Feed Stopped.")
