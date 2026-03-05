from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
TARGET_PREFIX = "btc-updown-5m-"
EASTERN = ZoneInfo("America/New_York")


def parse_iso_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def parse_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return parsed
    raise ValueError(f"Expected list or JSON list string, got {type(value).__name__}")


def map_up_down(outcomes: list[Any], token_ids: list[Any]) -> tuple[int, int]:
    if len(outcomes) != 2 or len(token_ids) != 2:
        raise ValueError(f"Expected 2 outcomes and 2 token IDs, got {len(outcomes)} and {len(token_ids)}")

    labels = [str(x).strip().lower() for x in outcomes]
    ids = [int(x) for x in token_ids]
    by_label = {labels[idx]: ids[idx] for idx in range(2)}

    up_id = by_label.get("up")
    down_id = by_label.get("down")

    if up_id is None or down_id is None:
        raise ValueError(f"Outcomes are not ['Up', 'Down']: outcomes={outcomes}")

    return up_id, down_id


def fetch_btc_5m_markets(
    session: requests.Session,
    max_pages: int,
    page_size: int,
    timeout_seconds: int,
) -> list[dict[str, Any]]:
    markets: dict[str, dict[str, Any]] = {}

    for page in range(max_pages):
        params = {
            "active": "true",
            "closed": "false",
            "tags": "crypto",
            "limit": page_size,
            "offset": page * page_size,
        }
        resp = session.get(GAMMA_EVENTS_URL, params=params, timeout=timeout_seconds)
        resp.raise_for_status()
        events = resp.json()

        if not events:
            break

        for event in events:
            event_slug = str(event.get("slug", ""))
            if not event_slug.startswith(TARGET_PREFIX):
                continue

            title = str(event.get("title", ""))
            for market in event.get("markets", []):
                market_slug = str(market.get("slug", ""))
                if not market_slug.startswith(TARGET_PREFIX):
                    continue

                end_raw = market.get("endDate")
                outcomes_raw = market.get("outcomes")
                token_ids_raw = market.get("clobTokenIds")

                if not (end_raw and outcomes_raw and token_ids_raw):
                    continue

                try:
                    end_utc = parse_iso_utc(str(end_raw))
                    interval_start_utc = end_utc - timedelta(minutes=5)
                    outcomes = parse_json_list(outcomes_raw)
                    token_ids = parse_json_list(token_ids_raw)
                    up_id, down_id = map_up_down(outcomes, token_ids)
                except Exception:
                    continue

                markets[market_slug] = {
                    "title": title,
                    "event_slug": event_slug,
                    "market_slug": market_slug,
                    "interval_start_utc": interval_start_utc,
                    "interval_end_utc": end_utc,
                    "up_token_id": up_id,
                    "down_token_id": down_id,
                }

    return list(markets.values())


def build_cache(
    all_markets: list[dict[str, Any]],
    horizon_days: int,
) -> dict[str, Any]:
    now_utc = datetime.now(timezone.utc)
    cutoff_utc = now_utc + timedelta(days=horizon_days)

    in_window = [
        m
        for m in all_markets
        if m["interval_end_utc"] > now_utc and m["interval_start_utc"] < cutoff_utc
    ]
    in_window.sort(key=lambda m: m["interval_start_utc"])

    serialized_markets = []
    for m in in_window:
        start_et = m["interval_start_utc"].astimezone(EASTERN)
        end_et = m["interval_end_utc"].astimezone(EASTERN)
        serialized_markets.append(
            {
                "title": m["title"],
                "event_slug": m["event_slug"],
                "market_slug": m["market_slug"],
                "interval_start_utc": m["interval_start_utc"].isoformat(),
                "interval_end_utc": m["interval_end_utc"].isoformat(),
                "start_et": start_et.isoformat(),
                "end_et": end_et.isoformat(),
                "up_token_id": str(m["up_token_id"]),
                "down_token_id": str(m["down_token_id"]),
            }
        )

    return {
        "generated_at_utc": now_utc.isoformat(),
        "timezone": "America/New_York",
        "horizon_days": horizon_days,
        "kept_intervals": len(serialized_markets),
        "markets": serialized_markets,
    }


def _parse_market_start(market: dict[str, Any]) -> datetime:
    start_raw = market.get("interval_start_utc") or market.get("start_utc")
    if not start_raw:
        return datetime.min.replace(tzinfo=timezone.utc)
    return parse_iso_utc(str(start_raw))


def merge_with_existing_cache(
    output_path: Path,
    new_cache: dict[str, Any],
) -> tuple[dict[str, Any], int]:
    if not output_path.exists():
        return new_cache, 0

    try:
        existing = json.loads(output_path.read_text(encoding="utf-8"))
        existing_markets = existing.get("markets", [])
    except Exception:
        return new_cache, 0

    merged_by_slug: dict[str, dict[str, Any]] = {}
    for market in existing_markets:
        slug = market.get("market_slug")
        if isinstance(slug, str) and slug:
            merged_by_slug[slug] = market

    existing_count = len(merged_by_slug)
    for market in new_cache.get("markets", []):
        slug = market.get("market_slug")
        if isinstance(slug, str) and slug:
            merged_by_slug[slug] = market

    merged_markets = list(merged_by_slug.values())
    merged_markets.sort(key=_parse_market_start)

    merged_cache = dict(new_cache)
    merged_cache["markets"] = merged_markets
    merged_cache["kept_intervals"] = len(merged_markets)
    merged_cache["merged_from_existing"] = True
    merged_cache["existing_intervals_before_merge"] = existing_count
    merged_cache["new_intervals_fetched"] = len(new_cache.get("markets", []))
    merged_cache["new_unique_intervals_added"] = max(0, len(merged_markets) - existing_count)
    return merged_cache, existing_count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build BTC Up/Down 5m token cache for now..T+2 days"
    )
    parser.add_argument(
        "--output",
        default="btc_updown_5m_tokens.json",
        help="Path for JSON cache output",
    )
    parser.add_argument("--horizon-days", type=int, default=2)
    parser.add_argument("--max-pages", type=int, default=60)
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--timeout-seconds", type=int, default=15)
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace output file instead of merging with existing markets",
    )
    args = parser.parse_args()

    session = requests.Session()
    all_markets = fetch_btc_5m_markets(
        session=session,
        max_pages=args.max_pages,
        page_size=args.page_size,
        timeout_seconds=args.timeout_seconds,
    )
    cache = build_cache(all_markets=all_markets, horizon_days=args.horizon_days)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    existing_count = 0
    if not args.replace:
        cache, existing_count = merge_with_existing_cache(output_path=output_path, new_cache=cache)
    output_path.write_text(json.dumps(cache, indent=2), encoding="utf-8")

    if args.replace:
        print(
            f"Wrote {cache['kept_intervals']} intervals to {output_path} (replace mode). "
            f"Scanned {len(all_markets)} BTC 5m markets."
        )
    else:
        print(
            f"Wrote {cache['kept_intervals']} merged intervals to {output_path}. "
            f"Scanned {len(all_markets)} BTC 5m markets. "
            f"Existing before merge: {existing_count}."
        )


if __name__ == "__main__":
    main()
