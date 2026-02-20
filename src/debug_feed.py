import sys
import os
import time

# Ensure src is in path
sys.path.append(os.path.abspath("src"))
from market_feed import FeedManager

def debug():
    print("--- DEBUG FEED ---")
    feed = FeedManager(keys_path="keys.json", api_keys={})
    
    # ADD FEED TO TRIGGER ADAPTER INIT
    feed.register_market({
        "register_name": "BTC_DERIBIT",
        "base_symbol": "BTC",
        "settlement": "coin",
        "source": "deribit"
    })
    
    feed.register_market({
        "register_name": "SPY_BBG",
        "base_symbol": "SPY",
        "settlement": "usd",
        "source": "bloomberg"
    })

    print("\n--- STARTING STREAM (10s test) ---")
    feed.start_stream()
    
    # We need to manually subscribe because debug script didn't call activate_feed_stage
    # 1. BTC
    deribit = feed.adapters.get('deribit')
    if deribit:
        refs = ['BTC.PERP', 'BTC'] # We know these are the refs
        print(f"Subscribing to {refs} on Deribit...")
        feed.subscribe_custom('deribit', refs)

    # 2. SPY
    bbg = feed.adapters.get('bloomberg')
    if bbg:
        refs = ['SPY']
        print(f"Subscribing to {refs} on Bloomberg...")
        feed.subscribe_custom('bloomberg', refs)

    # 3. ETH (Deribit)
    if deribit:
        refs_eth = ['ETH', 'ETH.PERP']
        print(f"Subscribing to {refs_eth} on Deribit...")
        feed.subscribe_custom('deribit', refs_eth)

    time.sleep(10)
    feed.stop_stream()
    print("--- END STREAM ---")

if __name__ == "__main__":
    debug()