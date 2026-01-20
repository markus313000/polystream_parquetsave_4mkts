from datetime import datetime
from zoneinfo import ZoneInfo
import time
import threading
from collections import defaultdict
from parquet import ParquetWriter, get_new_parquet_filename
from ws import WebSocketOrderBook
from getmkts import get_15m_events, load_tokens_for_condition

ASSETS = ["Bitcoin", "Ethereum", "Solana", "XRP"]  # Track multiple assets

def main(assets=ASSETS):
    wsconnections = {}
    current_events = {}
    asset_events = {}  # Cache events
    last_refresh = {}  # Track when we last refreshed
    
    REFRESH_INTERVAL = 299  # Refresh events every 5 minutes - 1 seconds

    try:
        while True:
            now = datetime.now(ZoneInfo("America/New_York"))

            for asset in assets:
                # Refresh events periodically (every 60 seconds)
                if (asset not in last_refresh or 
                    (now - last_refresh[asset]).total_seconds() >= REFRESH_INTERVAL):
                    
                    print(f"[REFRESH] Fetching new events for {asset}")
                    asset_events[asset] = get_15m_events(asset)
                    last_refresh[asset] = now
                
                # Use cached events
                events = asset_events.get(asset, [])
                
                # Find current 15m event for this asset
                new_event = next((e for e in events if e.start <= now < e.end), None)
                
                # Only switch if the event changed
                if new_event != current_events.get(asset):
                    current_events[asset] = new_event

                    if current_events[asset]:
                        yes_token = load_tokens_for_condition(current_events[asset].markets[0])[0]
                        print(f"[{now}] [{asset}] Switching to new 15m market: {yes_token}")

                        # Create a new parquet file for this market
                        parquet_file = get_new_parquet_filename(yes_token, asset)
                        print(f"[PARQUET] [{asset}] Writing to new file: {parquet_file}")

                        # Shutdown old connection for this asset if exists
                        if asset in wsconnections:
                            wsconnections[asset].shutdown()

                        # Initialize WebSocket with new parquet writer
                        wsconnections[asset] = WebSocketOrderBook(
                            channel_type="market",
                            wsurl="wss://ws-subscriptions-clob.polymarket.com",
                            data=[yes_token],
                            auth={"apiKey": None, "secret": None, "passphrase": None},
                            message_callback=None,
                            verbose=True,
                            parquet_filename=parquet_file
                        )

                        # Start WebSocket in separate thread
                        threading.Thread(
                            target=wsconnections[asset].run, 
                            daemon=True,
                            name=f"ws-{asset}"
                        ).start()
                    else:
                        print(f"[{now}] [{asset}] No active 15m market right now.")

            # Check every second for new events
            time.sleep(1)
    
    except KeyboardInterrupt:
        print("\n[MAIN] KeyboardInterrupt received, shutting down cleanly...")

    finally:
        # Shutdown all connections
        for asset, ws in wsconnections.items():
            print(f"[MAIN] Shutting down {asset}...")
            ws.shutdown()

        print("[MAIN] All shutdowns complete")


if __name__ == "__main__":
    main()
