from datetime import datetime
from typing import Optional, Tuple, List, Dict
import json
import threading
import time

from models import OB

# REMOVED: Global writer instance - should only be created once in ws.py

class OrderBookHandler:
    def __init__(self, subscribed_asset_id: str, lock: threading.Lock, parquet_writer):
        self.subscribed_asset_id = subscribed_asset_id[0]
        self.current_orderbook = None
        self.lock = lock  # Store the lock
        self.parquet_writer = parquet_writer
        
        # Reduce print spam for high-frequency updates
        self.update_count = 0
        self.last_print_time = time.time()

    def process_message(self, message: str):
        if not message or message.startswith("PONG"):
            return

        try:
            tempmsg = json.loads(message)

            if isinstance(tempmsg, list):
                data = tempmsg[0]
                self.process_full_book_message(data)
            elif isinstance(tempmsg, dict):
                data = tempmsg
                event_type = data.get('event_type')
                if event_type == "book":
                    self.process_full_book_message(data)
                elif event_type == "price_change":
                    self.process_price_change_message(data)
                else:
                    pass #print(f"Unknown event type: {event_type}")
            else:
                pass #print(f"Unexpected message format: {tempmsg}") #THIS IS ALSO USEFUL
                return
            
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")

    def process_full_book_message(self, data: dict):
        """Process full book with thread safety"""
        #print("Received full book snapshot.")
        
        if data["asset_id"] == self.subscribed_asset_id:
            with self.lock:  # CRITICAL: Lock while updating
                self.current_orderbook = OB(
                    token_id=self.subscribed_asset_id,
                    market=data["market"],
                    asset_id=data["asset_id"],
                    last_update=datetime.now(),
                    bids=data["bids"],
                    asks=data["asks"]
                )
            #print(f"Best ask: {self.current_orderbook.ba}, Best bid: {self.current_orderbook.bb}") #THIS IS GOOD TOO

            if self.parquet_writer:
                self.enqueue_orderbook()

    def process_price_change_message(self, data: dict):
        """Process price changes with thread safety"""
        # Only print occasionally to reduce spam
        self.update_count += 1
        should_print = (time.time() - self.last_print_time) > 2.0
        
        if should_print:
            #print(f"Received price change event (update #{self.update_count})")
            self.last_print_time = time.time()
        
        price_changes = data.get("price_changes", [])

        with self.lock:  # CRITICAL: Lock while updating
            for change in price_changes:
                asset_id = change.get("asset_id")

                if asset_id == self.subscribed_asset_id:
                    side = change["side"].lower()
                    price = float(change["price"])
                    size = float(change["size"])

                    if side == "buy":
                        self.update_price_level(self.current_orderbook.bids, price, size)
                    elif side == "sell":
                        self.update_price_level(self.current_orderbook.asks, price, size)

            # Update timestamp inside lock
            if self.current_orderbook:
                self.current_orderbook.last_update = datetime.now()
                
                if should_print:
                    pass #print(self.current_orderbook)
                    #print(f'Best ask: {self.current_orderbook.ba}, Best bid: {self.current_orderbook.bb}') #USE THIS HELLO MARKUS

                if self.parquet_writer:
                    self.enqueue_orderbook()

    def update_price_level(self, order_side: List[Dict], price: float, size: float):
        """Updates price level (called within locked section)"""
        # Remove level if size is 0
        if size == 0:
            order_side[:] = [level for level in order_side if float(level["price"]) != price]
            # Removed noisy print
            return
        
        updated = False
        for level in order_side:
            if float(level["price"]) == price:
                level["size"] = str(size)
                updated = True
                break

        if not updated:
            order_side.append({"price": str(price), "size": str(size)})
            # Removed noisy print
    
    def enqueue_orderbook(self):
        """Enqueue orderbook snapshot to parquet writer (called within lock)"""
        if not self.parquet_writer:
            return

        # Already inside lock from caller, safe to read orderbook
        if not self.current_orderbook:
            return
            
        ts = self.current_orderbook.last_update.isoformat()

        sorted_bids = sorted(
        self.current_orderbook.bids, 
        key=lambda x: float(x["price"]), 
        reverse=True
        )

        sorted_asks = sorted(
        self.current_orderbook.asks, 
        key=lambda x: float(x["price"])
        )

        
        # Create single batch of rows
        rows = []
        for bid in sorted_bids:
            rows.append({
                "timestamp": ts,
                "side": "bid",
                "price": float(bid["price"]),
                "size": float(bid["size"]),
                "asset_id": self.current_orderbook.asset_id,
                "market": self.current_orderbook.market
            })
            
        for ask in sorted_asks:
            rows.append({
                "timestamp": ts,
                "side": "ask",
                "price": float(ask["price"]),
                "size": float(ask["size"]),
                "asset_id": self.current_orderbook.asset_id,
                "market": self.current_orderbook.market
            })

        # Enqueue rows as batch (faster than individual enqueues)
        for row in rows:
            self.parquet_writer.enqueue(row)


    def switch_writer(self, new_writer):
        with self.lock:
            if self.parquet_writer:
                self.parquet_writer.shutdown()
            self.parquet_writer = new_writer