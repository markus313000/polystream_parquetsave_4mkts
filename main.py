from datetime import datetime
from zoneinfo import ZoneInfo
import time
import threading
from collections import defaultdict
from parquet import ParquetWriter, get_new_parquet_filename
from ws import WebSocketOrderBook
from getmkts import get_window_events, load_tokens_for_condition

ASSETS = ["Bitcoin", "Ethereum", "Solana", "XRP"]  # Track multiple assets
Timeframe = "15m"  # 15m, 5m (can implement hourly but need to change name processing in getmkts)


def main(assets=ASSETS, window=Timeframe):
    wsconnections = {}
    current_events = {}
    asset_events = {}  # Cache events
    last_refresh = {}  # Track when we last refreshed
    
    REFRESH_INTERVAL = 299  # Refresh events every 5 minutes - 1 second
    RECONNECT_DELAY = 0.5  # Wait 0.5 seconds before reconnecting

    try:
        while True:
            now = datetime.now(ZoneInfo("America/New_York"))

            for asset in assets:
                # Refresh events periodically (every 5 minutes)
                if (asset not in last_refresh or 
                    (now - last_refresh[asset]).total_seconds() >= REFRESH_INTERVAL):
                    
                    print(f"[REFRESH] Fetching new events for {asset}")
                    asset_events[asset] = get_window_events(asset, window)
                    last_refresh[asset] = now
                
                # Use cached events
                events = asset_events.get(asset, [])
                
                # Find current event for this asset
                new_event = next((e for e in events if e.start <= now < e.end), None)
                
                # Check if we need to handle a new event OR reconnect a dead connection
                should_connect = False
                
                # Case 1: Event changed (new market or market ended)
                if new_event != current_events.get(asset):
                    current_events[asset] = new_event
                    should_connect = new_event is not None
                    
                    if not new_event:
                        # Market ended - shutdown connection
                        print(f"[{now}] [{asset}] No active market right now.")
                        if asset in wsconnections:
                            wsconnections[asset].shutdown()
                            del wsconnections[asset]
                        continue
                
                # Case 2: We have an active event but connection is dead
                elif new_event and asset in wsconnections and not wsconnections[asset].running:
                    print(f"[{now}] [{asset}] Connection lost - reconnecting...")
                    should_connect = True
                    time.sleep(RECONNECT_DELAY)  # Brief delay before reconnecting
                
                # Case 3: We have an active event but no connection at all
                elif new_event and asset not in wsconnections:
                    print(f"[{now}] [{asset}] No connection exists - connecting...")
                    should_connect = True
                
                # Establish/re-establish connection if needed
                if should_connect and current_events[asset]:
                    yes_token = load_tokens_for_condition(current_events[asset].markets[0])[0]
                    
                    # Determine if this is a reconnect or new market
                    is_reconnect = asset in wsconnections
                    action = "Reconnecting to" if is_reconnect else "Switching to new"
                    
                    print(f"[{now}] [{asset}] {action} market: {yes_token}")

                    # Shutdown old connection if exists
                    if asset in wsconnections:
                        try:
                            wsconnections[asset].shutdown()
                        except Exception as e:
                            print(f"[{asset}] Error during shutdown: {e}")

                    # Create a new parquet file for this market
                    # For reconnects, continue using the same file by reusing the existing writer
                    # For new markets, create a new file
                    if is_reconnect:
                        # Reuse existing parquet file name pattern but this creates a new file
                        # If you want to append to the same file, you'd need different logic
                        parquet_file = get_new_parquet_filename(yes_token, asset)
                        print(f"[PARQUET] [{asset}] Reconnect - new file: {parquet_file}")
                    else:
                        parquet_file = get_new_parquet_filename(yes_token, asset)
                        print(f"[PARQUET] [{asset}] Writing to new file: {parquet_file}")

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

            # Check every second for new events or dead connections
            time.sleep(1)
    
    except KeyboardInterrupt:
        print("\n[MAIN] KeyboardInterrupt received, shutting down cleanly...")

    finally:
        # Shutdown all connections
        for asset, ws in list(wsconnections.items()):
            print(f"[MAIN] Shutting down {asset}...")
            try:
                ws.shutdown()
            except Exception as e:
                print(f"[MAIN] Error shutting down {asset}: {e}")

        print("[MAIN] All shutdowns complete")


if __name__ == "__main__":
    main()
