import pyarrow as pa
import pyarrow.parquet as pq
import threading
import queue
import time
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo


class ParquetWriter:
    """
    Parquet-correct, high-throughput streaming writer.

    Guarantees:
    - Exactly one ParquetWriter per file
    - No reopening / no append-after-close
    - Footer written exactly once
    """

    def __init__(
        self,
        filename: str,
        flush_interval: float = 5.0,
        batch_size: int = 10_000,
        queue_maxsize: int = 500_000,
    ):
        self.filename = filename
        self.flush_interval = flush_interval
        self.batch_size = batch_size

        self.queue: queue.Queue[Dict] = queue.Queue(maxsize=queue_maxsize)
        self.running = True
        self._writer: Optional[pq.ParquetWriter] = None

        self.schema = pa.schema(
            [
                ("timestamp", pa.string()),
                ("side", pa.string()),
                ("price", pa.float64()),
                ("size", pa.float64()),
                ("asset_id", pa.string()),
                ("market", pa.string()),
            ]
        )

        # Ensure parent directory exists
        Path(self.filename).parent.mkdir(parents=True, exist_ok=True)

        # Writer thread
        self._thread = threading.Thread(
            target=self._writer_loop,
            name="parquet-writer",
            daemon=False,
        )
        self._thread.start()

        print(f"[PARQUET] Streaming writer started: {self.filename}")
        print(
            f"[PARQUET] batch_size={self.batch_size}, flush_interval={self.flush_interval}s"
        )

    # ------------------------------------------------------------------
    # Internal writer lifecycle
    # ------------------------------------------------------------------

    def _open_writer(self) -> None:
        if self._writer is not None:
            raise RuntimeError("ParquetWriter already open")

        self._writer = pq.ParquetWriter(
            self.filename,
            self.schema,
            compression="snappy",
            version="2.6",
            write_statistics=True,
        )

    def _close_writer(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None

    # ------------------------------------------------------------------
    # Writer loop
    # ------------------------------------------------------------------

    def _writer_loop(self) -> None:
        buffer: List[Dict] = []
        last_flush = time.monotonic()
        total_written = 0

        try:
            self._open_writer()

            while self.running or not self.queue.empty():
                try:
                    row = self.queue.get(timeout=0.1)
                    buffer.append(row)
                except queue.Empty:
                    pass

                now = time.monotonic()

                should_flush = (
                    len(buffer) >= self.batch_size
                    or (buffer and now - last_flush >= self.flush_interval)
                )

                if should_flush:
                    written = self._flush(buffer)
                    total_written += written
                    buffer.clear()
                    last_flush = now

            # Final flush
            if buffer:
                written = self._flush(buffer)
                total_written += written
                buffer.clear()

        finally:
            self._close_writer()
            print(
                f"[PARQUET] Writer closed cleanly — total rows written: {total_written:,}"
            )

    # ------------------------------------------------------------------
    # Flush logic
    # ------------------------------------------------------------------

    def _flush(self, rows: List[Dict]) -> int:
        if not rows:
            return 0

        if self._writer is None:
            raise RuntimeError("Writer not open during flush")

        try:
            table = pa.Table.from_pylist(rows, schema=self.schema)
            self._writer.write_table(table)

            if total := len(rows):
                qsize = self.queue.qsize()
                print(f"[PARQUET] wrote {total:,} rows (queue={qsize:,})")

            return len(rows)

        except Exception:
            # At this point the file may be invalid.
            # Do NOT attempt recovery — fail loudly.
            print("[PARQUET FATAL] Write failed — file may be corrupted")
            raise

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------


    def enqueue(self, row: Dict) -> None:
        if not self.running:
            return

        try:
            self.queue.put_nowait(row)
        except queue.Full:
            try:
                self.queue.get_nowait()  # drop oldest
                self.queue.put_nowait(row)
            except queue.Empty:
                pass

    def get_queue_size(self) -> int:
        """Return the current number of rows waiting to be written."""
        return self.queue.qsize()

    def shutdown(self, timeout: Optional[float] = None) -> None:
        print(
            f"[PARQUET] Shutdown requested ({self.queue.qsize():,} rows remaining)"
        )
        self.running = False
        self._thread.join(timeout=timeout)

        if self._thread.is_alive():
            raise RuntimeError(
                "Parquet writer did not shut down cleanly — data loss possible"
            )

    def get_file_size(self) -> float:
        if not os.path.exists(self.filename):
            return 0.0
        return os.path.getsize(self.filename) / (1024 * 1024)

def get_new_parquet_filename(
    token_id: str,
    asset: str,
    root: str = "data"
) -> str:
    now = datetime.now(ZoneInfo("America/New_York"))
    day = now.strftime("%Y%m%d")
    ts = now.strftime("%Y%m%d_%H%M%S")

    # Create folder structure: data/{day}/{asset}/
    folder = Path(root) / day / asset
    folder.mkdir(parents=True, exist_ok=True)

    return str(folder / f"{asset}_{ts}_{token_id}.parquet")
