from datetime import datetime, timedelta, timezone
from typing import List, Optional
import requests
from zoneinfo import ZoneInfo
from models import Event
from collections import defaultdict
from py_clob_client.client import ClobClient

GAMMA_API_BASE = "https://gamma-api.polymarket.com"

def get_window_events(asset, window, limit=2):
    url = f"{GAMMA_API_BASE}/events"
    eight_days_ago = datetime.now(timezone.utc) - timedelta(days=8)
    today_str = eight_days_ago.strftime("%Y-%m-%d")
    tag = None

    if window == "1h":
        tag = 102175
    elif window == "15m":
        tag = 102467
    elif window == "5m":
        tag = 102892
    else:
        print("Not a minute window market.")

    params = {
        "closed": "false",
        "start_date_min": today_str,
        "limit": limit,
        "tag_id": tag
    }

    resp = requests.get(url, params=params)
    data = resp.json()

    events: List[Event] = []

    for i in data:
        title = i.get("title") or i.get("ticker") or i.get("slug")
        if "Up or Down" not in title:
            continue

        raw_markets = i.get("markets", [])
        condition = [
            m.get("conditionId")
            for m in raw_markets
        ]

        _, time_part = title.rsplit(" - ", 1)
        time_part = time_part.replace(" ET", "")


        # separate date and time range
        date_str, time_range = time_part.split(", ")
        start_str, end_str = time_range.split("-")

        # assume current year
        year = datetime.now().year
        tz = ZoneInfo("America/New_York")

        # parse datetimes
        start = datetime.strptime(
            f"{date_str} {year} {start_str}",
            "%B %d %Y %I:%M%p"
        ).replace(tzinfo=tz)

        end = datetime.strptime(
            f"{date_str} {year} {end_str}",
            "%B %d %Y %I:%M%p"
        ).replace(tzinfo=tz)

        # handle midnight rollover
        if end <= start:
            end += timedelta(days=1)

        ev = Event(
            event_id=i.get("id"),
            name=title,
            status="active" if i.get("active") else "closed",
            start=start,
            end=end,
            creation_date=i.get("startDate"),
            closing_date=i.get("endDate"),
            markets=condition    # <--- now conditionIds
        )

        events.append(ev)

    ASSETS = {
    "Bitcoin": "Bitcoin",
    "Ethereum": "Ethereum",
    "Solana": "Solana",
    "XRP": "XRP",
    }

    #for i in range(len(events)):
        #print(events[i].window)

    asset_events = defaultdict(list)

    for event in events:
        for asset_name in ASSETS:
            if asset_name in event.name:
                asset_events[asset_name].append(event)
                break    
    #print(asset_events[asset])
    return asset_events[asset]

def load_tokens_for_condition(condition_id):
    """
    Retrieve the two clobTokenIds for a moneyline condition.
    """
    client = ClobClient("https://clob.polymarket.com")
    market = client.get_market(condition_id)
    
    tokens = market.get("tokens", [])
    if len(tokens) != 2:
        raise ValueError(f"Condition {condition_id} does not have exactly two tokens: {tokens}")

    token_ids = [t["token_id"] for t in tokens]
    #print(f"Loaded token IDs: {token_ids}")
    return token_ids

#get_window_events("Bitcoin", window="5m")