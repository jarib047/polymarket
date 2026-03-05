from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from py_clob_client.client import ClobClient

CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID = 137


class TokenLookupError(RuntimeError):
    pass


def _parse_iso_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _best_levels(book: Any) -> tuple[Any, Any]:
    bids = getattr(book, "bids", [])
    asks = getattr(book, "asks", [])
    best_bid = max(bids, key=_level_price, default=None)
    best_ask = min(asks, key=_level_price, default=None)
    return best_bid, best_ask


def _level_to_text(level: Any) -> str:
    if level is None:
        return "None"

    price = getattr(level, "price", None)
    size = getattr(level, "size", None)
    if price is None and isinstance(level, dict):
        price = level.get("price")
        size = level.get("size")

    if price is None and size is None:
        return str(level)
    return f"price={price}, size={size}"


def _level_price(level: Any) -> float:
    price = getattr(level, "price", None)
    if price is None and isinstance(level, dict):
        price = level.get("price")
    return float(price) if price is not None else float("nan")


def load_token_cache(cache_path: Path) -> dict[str, Any]:
    if not cache_path.exists():
        raise TokenLookupError(
            f"Token cache file not found: {cache_path}. Run build_btc_5m_token_cache.py first."
        )

    data = json.loads(cache_path.read_text(encoding="utf-8"))
    markets = data.get("markets")
    if not isinstance(markets, list) or not markets:
        raise TokenLookupError(f"Token cache has no markets: {cache_path}")

    return data


def _get_interval_bounds(market: dict[str, Any]) -> tuple[datetime, datetime]:
    start_raw = market.get("interval_start_utc") or market.get("start_utc")
    end_raw = market.get("interval_end_utc") or market.get("end_utc")

    if not start_raw or not end_raw:
        raise ValueError("Missing interval start/end")

    return _parse_iso_utc(str(start_raw)), _parse_iso_utc(str(end_raw))


def select_active_market(cache_data: dict[str, Any], now_utc: datetime) -> dict[str, Any]:
    markets = cache_data.get("markets", [])

    active: dict[str, Any] | None = None
    upcoming: dict[str, Any] | None = None
    upcoming_start: datetime | None = None

    for market in markets:
        try:
            start_utc, end_utc = _get_interval_bounds(market)
        except Exception:
            continue

        if start_utc <= now_utc < end_utc:
            active = market
            break

        if now_utc < start_utc and (upcoming_start is None or start_utc < upcoming_start):
            upcoming_start = start_utc
            upcoming = market

    if active is not None:
        return active

    if upcoming is not None:
        raise TokenLookupError(
            "No active interval found in cache right now. "
            f"Next interval starts at {upcoming_start.isoformat()} UTC "
            f"({upcoming.get('title', upcoming.get('market_slug', 'unknown'))})."
        )

    raise TokenLookupError("No active or upcoming intervals found in token cache.")


def run(poll_seconds: float, refresh_seconds: float, once: bool, cache_file: str) -> None:
    client = ClobClient(CLOB_HOST, chain_id=CHAIN_ID)
    cache_path = Path(cache_file)

    cache_data: dict[str, Any] | None = None
    last_refresh = 0.0
    last_market_slug = None

    while True:
        now_epoch = time.time()
        needs_refresh = cache_data is None or (now_epoch - last_refresh) >= refresh_seconds

        if needs_refresh:
            cache_data = load_token_cache(cache_path)
            last_refresh = now_epoch

        now_utc = datetime.now(timezone.utc)
        market = select_active_market(cache_data, now_utc)

        up_id = int(market["up_token_id"])
        down_id = int(market["down_token_id"])

        if market.get("market_slug") != last_market_slug:
            last_market_slug = market.get("market_slug")
            print(
                "Using market: "
                f"{market.get('title', '')} | "
                # f"start={market.get('start_et', market.get('interval_start_utc'))} "
                # f"end={market.get('end_et', market.get('interval_end_utc'))} | "
                # f"up={up_id} down={down_id}"
            )

        up_book = client.get_order_book(up_id)
        down_book = client.get_order_book(down_id)

        up_bid, up_ask = _best_levels(up_book)
        down_bid, down_ask = _best_levels(down_book)

        print(
            "UP  :"
            f" best_bid=({_level_to_text(up_bid)})"
            f" best_ask=({_level_to_text(up_ask)})"
        )
        print(
            "DOWN:"
            f" best_bid=({_level_to_text(down_bid)})"
            f" best_ask=({_level_to_text(down_ask)})"
        )

        if once:
            return

        time.sleep(poll_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read BTC 5m token cache and fetch active interval order books"
    )
    parser.add_argument("--token-cache", default="btc_updown_5m_tokens.json")
    parser.add_argument("--poll-seconds", type=float, default=0.01)
    parser.add_argument("--refresh-seconds", type=float, default=30.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    run(
        poll_seconds=args.poll_seconds,
        refresh_seconds=args.refresh_seconds,
        once=args.once,
        cache_file=args.token_cache,
    )


if __name__ == "__main__":
    main()
