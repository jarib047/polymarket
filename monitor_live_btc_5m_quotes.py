from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from py_clob_client.client import ClobClient
from py_clob_client.exceptions import PolyApiException

CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID = 137


class MonitorError(RuntimeError):
    pass


def parse_iso_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def load_token_cache(cache_path: Path) -> dict[str, Any]:
    if not cache_path.exists():
        raise MonitorError(f"Token cache file not found: {cache_path}")
    data = json.loads(cache_path.read_text(encoding="utf-8"))
    markets = data.get("markets")
    if not isinstance(markets, list) or not markets:
        raise MonitorError(f"Token cache has no markets: {cache_path}")
    return data


def get_interval_bounds(market: dict[str, Any]) -> tuple[datetime, datetime]:
    start_raw = market.get("interval_start_utc") or market.get("start_utc")
    end_raw = market.get("interval_end_utc") or market.get("end_utc")
    if not start_raw or not end_raw:
        raise ValueError("Missing interval start/end")
    return parse_iso_utc(str(start_raw)), parse_iso_utc(str(end_raw))


def select_active_market(cache_data: dict[str, Any], now_utc: datetime) -> dict[str, Any] | None:
    for market in cache_data.get("markets", []):
        try:
            start_utc, end_utc = get_interval_bounds(market)
        except Exception:
            continue
        if start_utc <= now_utc < end_utc:
            return market
    return None


def level_price(level: Any) -> float:
    price = getattr(level, "price", None)
    if price is None and isinstance(level, dict):
        price = level.get("price")
    return float(price) if price is not None else float("nan")


def level_size(level: Any) -> float | None:
    size = getattr(level, "size", None)
    if size is None and isinstance(level, dict):
        size = level.get("size")
    if size is None:
        return None
    return float(size)


def best_levels(book: Any) -> tuple[Any, Any]:
    bids = getattr(book, "bids", [])
    asks = getattr(book, "asks", [])
    best_bid = max(bids, key=level_price, default=None)
    best_ask = min(asks, key=level_price, default=None)
    return best_bid, best_ask


def ensure_csv_header(path: Path) -> None:
    headers = [
        "ts_utc",
        "market_slug",
        "title",
        "interval_start_utc",
        "interval_end_utc",
        "up_token_id",
        "down_token_id",
        "up_best_bid",
        "up_best_ask",
        "up_best_bid_size",
        "up_best_ask_size",
        "down_best_bid",
        "down_best_ask",
        "down_best_bid_size",
        "down_best_ask_size",
    ]

    if path.exists() and path.stat().st_size > 0:
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)


def initialize_output_csv(path: Path, overwrite: bool) -> str:
    if overwrite and path.exists():
        path.unlink()
    existed = path.exists() and path.stat().st_size > 0
    ensure_csv_header(path)
    return "append" if existed and not overwrite else "create"


def fetch_order_book_with_retry(
    client: ClobClient,
    token_id: int,
    retries: int,
    retry_sleep_seconds: float,
) -> Any | None:
    for attempt in range(retries + 1):
        try:
            return client.get_order_book(token_id)
        except (PolyApiException, TimeoutError) as exc:
            if attempt >= retries:
                print(f"order_book fetch failed for token={token_id}: {exc}")
                return None
            time.sleep(retry_sleep_seconds * (attempt + 1))
    return None


def run(
    token_cache_file: str,
    output_file: str,
    overwrite: bool,
    poll_seconds: float,
    refresh_seconds: float,
    request_retries: int,
    retry_sleep_seconds: float,
    max_samples: int | None,
) -> None:
    client = ClobClient(CLOB_HOST, chain_id=CHAIN_ID)
    cache_path = Path(token_cache_file)
    output_path = Path(output_file)
    mode = initialize_output_csv(output_path, overwrite=overwrite)
    print(f"CSV mode: {mode} | file={output_path}")

    cache_data: dict[str, Any] | None = None
    last_refresh = 0.0
    sample_count = 0
    last_market_slug: str | None = None

    while True:
        now_epoch = time.time()
        now_utc = datetime.now(timezone.utc)

        needs_refresh = cache_data is None or (now_epoch - last_refresh) >= refresh_seconds
        if needs_refresh:
            cache_data = load_token_cache(cache_path)
            last_refresh = now_epoch

        market = select_active_market(cache_data, now_utc)
        if market is None:
            print(f"{now_utc.isoformat()} | no active market found in cache; waiting")
            time.sleep(poll_seconds)
            continue

        up_id = int(market["up_token_id"])
        down_id = int(market["down_token_id"])

        if market.get("market_slug") != last_market_slug:
            last_market_slug = market.get("market_slug")
            print(
                f"Switching to {market.get('market_slug')} "
                f"({market.get('interval_start_utc')} -> {market.get('interval_end_utc')})"
            )

        up_book = fetch_order_book_with_retry(
            client=client,
            token_id=up_id,
            retries=request_retries,
            retry_sleep_seconds=retry_sleep_seconds,
        )
        down_book = fetch_order_book_with_retry(
            client=client,
            token_id=down_id,
            retries=request_retries,
            retry_sleep_seconds=retry_sleep_seconds,
        )
        if up_book is None or down_book is None:
            print(f"{now_utc.isoformat()} | skipping sample due to fetch failure")
            time.sleep(poll_seconds)
            continue

        up_bid, up_ask = best_levels(up_book)
        down_bid, down_ask = best_levels(down_book)

        row = [
            now_utc.isoformat(),
            market.get("market_slug"),
            market.get("title", ""),
            market.get("interval_start_utc") or market.get("start_utc"),
            market.get("interval_end_utc") or market.get("end_utc"),
            str(up_id),
            str(down_id),
            level_price(up_bid) if up_bid is not None else None,
            level_price(up_ask) if up_ask is not None else None,
            level_size(up_bid),
            level_size(up_ask),
            level_price(down_bid) if down_bid is not None else None,
            level_price(down_ask) if down_ask is not None else None,
            level_size(down_bid),
            level_size(down_ask),
        ]

        with output_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(row)

        sample_count += 1
        print(
            f"{now_utc.isoformat()} | up bid/ask=({row[7]}, {row[8]}) "
            f"down bid/ask=({row[11]}, {row[12]})"
        )

        if max_samples is not None and sample_count >= max_samples:
            print(f"Reached max samples: {max_samples}")
            return

        time.sleep(poll_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Monitor best bid/ask for current BTC 5m Up/Down market and store to CSV"
    )
    parser.add_argument("--token-cache", default="btc_updown_5m_tokens.json")
    parser.add_argument("--output", default="live_btc_5m_quotes.csv")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Start a fresh CSV by replacing existing output file",
    )
    parser.add_argument("--poll-seconds", type=float, default=0.01)
    parser.add_argument("--refresh-seconds", type=float, default=30.0)
    parser.add_argument("--request-retries", type=int, default=2)
    parser.add_argument("--retry-sleep-seconds", type=float, default=0.1)
    parser.add_argument("--max-samples", type=int, default=None)
    args = parser.parse_args()

    run(
        token_cache_file=args.token_cache,
        output_file=args.output,
        overwrite=args.overwrite,
        poll_seconds=args.poll_seconds,
        refresh_seconds=args.refresh_seconds,
        request_retries=args.request_retries,
        retry_sleep_seconds=args.retry_sleep_seconds,
        max_samples=args.max_samples,
    )


if __name__ == "__main__":
    main()
