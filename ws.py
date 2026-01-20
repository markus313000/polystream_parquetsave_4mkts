import json
import threading
import time
from orderbook import OrderBookHandler
from websocket import WebSocketApp
from datetime import datetime
from parquet import ParquetWriter

MARKET_CHANNEL = "market"
USER_CHANNEL = "user"

class WebSocketOrderBook:
    def __init__(self, channel_type, wsurl, data, auth, message_callback, verbose, parquet_filename=None):
        # Create lock FIRST
        self.lock = threading.Lock()
        
        if parquet_filename is None:
            parquet_filename = "live_orderbook.parquet"

        # Create SINGLE ParquetWriter instance
        self.parquet_writer = ParquetWriter(parquet_filename)
        
        # Pass lock and writer to handler
        self.order_book_handler = OrderBookHandler(
            data, 
            self.lock, 
            parquet_writer=self.parquet_writer
        )
        
        self.channel_type = channel_type
        self.wsurl = wsurl
        self.data = data
        self.auth = auth
        self.message_callback = message_callback
        self.verbose = verbose
        self.running = True  # Flag to control threads
        
        # Stats tracking
        self.message_count = 0
        self.start_time = time.time()
        
        furl = wsurl + "/ws/" + channel_type
        self.ws = WebSocketApp(
            furl,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )
        self.orderbooks = {}
        
        # Start monitoring thread
        monitor_thread = threading.Thread(target=self._monitor_thread, daemon=True)
        monitor_thread.start()

    def on_message(self, ws, message):
        # Lock is handled inside order_book_handler.process_message
        self.message_count += 1
        self.order_book_handler.process_message(message)

    def on_error(self, ws, error):
        print(f"WebSocket Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print(f"WebSocket closing: {close_status_code} - {close_msg}")
        self.running = False

    def on_open(self, ws):
        print("WebSocket connection opened")
        
        if self.channel_type == MARKET_CHANNEL:
            ws.send(json.dumps({"assets_ids": self.data, "type": MARKET_CHANNEL}))
        elif self.channel_type == USER_CHANNEL and self.auth:
            ws.send(
                json.dumps(
                    {"markets": self.data, "type": USER_CHANNEL, "auth": self.auth}
                )
            )
        else:
            print("Invalid channel configuration")
            return

        # Start ping thread
        thr = threading.Thread(target=self.ping, args=(ws,))
        thr.daemon = True
        thr.start()

    def ping(self, ws):
        """Keep-alive ping thread"""
        while self.running:
            try:
                ws.send("PING")
                time.sleep(2)
            except Exception as e:
                print(f"Ping error: {e}")
                break
    
    def _monitor_thread(self):
        """Monitor streaming performance every 30 seconds"""
        while self.running:
            time.sleep(30)
            if not self.running:
                break
            
            elapsed = time.time() - self.start_time
            msg_rate = self.message_count / elapsed if elapsed > 0 else 0
            queue_size = self.parquet_writer.get_queue_size()
            file_size = self.parquet_writer.get_file_size()
            
            print(f"\n{'='*60}")
            print(f"[MONITOR] Runtime: {elapsed/60:.1f} min")
            print(f"[MONITOR] Messages received: {self.message_count:,} ({msg_rate:.1f}/sec)")
            print(f"[MONITOR] Write queue: {queue_size:,} rows")
            print(f"[MONITOR] File size: {file_size:.2f} MB")
            print(f"{'='*60}\n")

    def run(self):
        """Start WebSocket connection (blocking)"""
        self.ws.run_forever()
    
    def shutdown(self):
        """Clean shutdown of all resources"""
        print("[WS] Shutting down WebSocket and ParquetWriter...")
        self.running = False
        
        # Close WebSocket connection
        if self.ws:
            self.ws.close()
        
        # Shutdown parquet writer
        if self.parquet_writer:
            self.parquet_writer.shutdown()
        
        print("[WS] Shutdown complete")