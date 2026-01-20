#!/usr/bin/env python3
"""
Simple orderbook visualization from parquet data in the `data/` folder.

Usage: python read_orderbook.py
"""

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import sys

# ----------------------------
# USER CONFIGURATION
# ----------------------------
# Enter the name of the parquet file (without path) you want to load.
# If left empty or None, the script will automatically pick the latest file.
FILENAME = "Solana_20260120_133001_5589020107557828316939678516570839158700078041024819487498872632478648426929"  +  ".parquet"
day = "20260120"
asset = "Solana"
# Example:
# FILENAME = "orderbook_103973080867184147540932673486271420183121262862342348130962763105475368320877_20260120_000437"

DATA_FOLDER = f"data/{day}/{asset}"  # folder to search for parquet files


# ----------------------------
# UTILITIES
# ----------------------------
def find_latest_parquet(folder=DATA_FOLDER, filename=None):
    """
    Find a parquet file in the folder (optionally by filename).
    If filename is None, return the newest parquet file.
    """
    folder_path = Path(folder)
    if not folder_path.exists():
        raise FileNotFoundError(f"Folder {folder} does not exist")

    files = list(folder_path.rglob("*.parquet"))
    if filename:
        files = [f for f in files if filename in f.name]

    if not files:
        raise FileNotFoundError(f"No parquet files found in {folder} matching filename={filename}")

    # Return the newest file by modification time
    latest_file = max(files, key=lambda f: f.stat().st_mtime)
    return str(latest_file)


# ----------------------------
# ORDERBOOK VALIDATION
# ----------------------------
def validate_snapshots(df):
    """Check that each timestamp has complete orderbook snapshots"""
    print("\n" + "="*80)
    print("ORDERBOOK SNAPSHOT VALIDATION")
    print("="*80)

    snapshot_counts = df.groupby(['timestamp', 'side']).size().unstack(fill_value=0)

    print(f"\nTotal unique timestamps: {len(snapshot_counts):,}")

    if 'bid' in snapshot_counts.columns and 'ask' in snapshot_counts.columns:
        print(f"\nBid counts per snapshot: min={snapshot_counts['bid'].min()}, max={snapshot_counts['bid'].max()}, mean={snapshot_counts['bid'].mean():.1f}")
        print(f"Ask counts per snapshot: min={snapshot_counts['ask'].min()}, max={snapshot_counts['ask'].max()}, mean={snapshot_counts['ask'].mean():.1f}")

        balanced = snapshot_counts[snapshot_counts['bid'] == snapshot_counts['ask']]
        print(f"\nSnapshots with equal bid/ask counts: {len(balanced):,} ({len(balanced)/len(snapshot_counts)*100:.1f}%)")

        missing_bids = snapshot_counts[snapshot_counts['bid'] == 0]
        missing_asks = snapshot_counts[snapshot_counts['ask'] == 0]

        if len(missing_bids) > 0:
            print(f"WARNING: {len(missing_bids)} timestamps missing bids")
        if len(missing_asks) > 0:
            print(f"WARNING: {len(missing_asks)} timestamps missing asks")

        if len(missing_bids) == 0 and len(missing_asks) == 0:
            print("✓ All snapshots have both bids and asks")

    print("\n" + "="*80)
    print("SAMPLE SNAPSHOTS (COUNTS ONLY)")
    print("="*80)

    sample_times = df['timestamp'].unique()[:3]
    for ts in sample_times:
        snapshot = df[df['timestamp'] == ts]
        print(f"\nTimestamp: {ts}")
        print(f"  Bids: {len(snapshot[snapshot['side'] == 'bid'])}")
        print(f"  Asks: {len(snapshot[snapshot['side'] == 'ask'])}")

    print("="*80 + "\n")
    return snapshot_counts


def print_full_snapshot(df, index=0):
    """Print the entire contents of a snapshot for manual inspection"""
    timestamps = df['timestamp'].unique()

    if index >= len(timestamps):
        raise IndexError("Snapshot index out of range")

    ts = timestamps[index]
    snapshot = df[df['timestamp'] == ts].sort_values(['side', 'price'], ascending=[True, False])

    print("\n" + "="*80)
    print(f"FULL SNAPSHOT DUMP (index={index})")
    print(f"Timestamp: {ts}")
    print("="*80)
    print(snapshot.to_string(index=False))
    print("="*80 + "\n")


# ----------------------------
# LOAD AND PLOT
# ----------------------------
def load_and_plot(filename=None):
    """Load orderbook data and create visualizations"""

    from pathlib import Path

    if filename is None:
        filename = find_latest_parquet(folder=DATA_FOLDER)

    # Make sure to include the folder path
    file_path = Path(DATA_FOLDER) / filename
    print(f"Loading {file_path}...")
    df = pd.read_parquet(file_path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')

    print(f"Loaded {len(df):,} rows")
    print(f"Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")

    validate_snapshots(df)
    print_full_snapshot(df, index=0)

    bids = df[df['side'] == 'bid']
    asks = df[df['side'] == 'ask']

    fig, axes = plt.subplots(2, 1, figsize=(14, 8))
    fig.suptitle('Orderbook Analysis', fontsize=16, fontweight='bold')

    # Plot 1: Best Bid / Ask
    best_bids = bids.groupby('timestamp')['price'].max()
    best_asks = asks.groupby('timestamp')['price'].min()

    axes[0].plot(best_bids.index, best_bids.values, label='Best Bid', linewidth=1, alpha=0.7)
    axes[0].plot(best_asks.index, best_asks.values, label='Best Ask', linewidth=1, alpha=0.7)
    axes[0].set_ylabel('Price ($)')
    axes[0].set_title('Best Bid / Ask Over Time')
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)

    # Plot 2: Spread
    combined = pd.DataFrame({'bid': best_bids, 'ask': best_asks}).dropna()
    spread = combined['ask'] - combined['bid']
    axes[1].plot(spread.index, spread.values, linewidth=1.5)
    axes[1].set_ylabel('Spread ($)')
    axes[1].set_title('Bid-Ask Spread Over Time')
    axes[1].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.show()

    return df


# ----------------------------
# MAIN EXECUTION
# ----------------------------
if __name__ == "__main__":
    # Allow passing filename via command line
    filename = sys.argv[1] if len(sys.argv) > 1 else FILENAME

    try:
        df = load_and_plot(filename)
        print(f"\n✓ Loaded {len(df):,} rows successfully")
    except FileNotFoundError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Error: {e}")
