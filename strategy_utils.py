from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


@dataclass
class QuoteRow:
    ts: datetime
    market_slug: str
    up_bid: float | None
    up_ask: float | None
    down_bid: float | None
    down_ask: float | None


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load_quotes(csv_path: Path) -> dict[str, list[QuoteRow]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Quotes CSV not found: {csv_path}")

    grouped: dict[str, list[QuoteRow]] = {}
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            slug = row.get("market_slug")
            ts_raw = row.get("ts_utc")
            if not slug or not ts_raw:
                continue
            try:
                ts = parse_ts(ts_raw)
            except Exception:
                continue

            q = QuoteRow(
                ts=ts,
                market_slug=slug,
                up_bid=to_float(row.get("up_best_bid")),
                up_ask=to_float(row.get("up_best_ask")),
                down_bid=to_float(row.get("down_best_bid")),
                down_ask=to_float(row.get("down_best_ask")),
            )
            grouped.setdefault(slug, []).append(q)

    for slug in grouped:
        grouped[slug].sort(key=lambda r: r.ts)
    return grouped


def first_entry_row(rows: list[QuoteRow]) -> QuoteRow | None:
    for r in rows:
        if r.up_ask is not None and r.down_ask is not None:
            return r
    return None


def last_valid_row(rows: list[QuoteRow]) -> QuoteRow | None:
    for r in reversed(rows):
        if (r.up_bid is not None or r.up_ask is not None) and (r.down_bid is not None or r.down_ask is not None):
            return r
    return None


def infer_winner(last_row: QuoteRow) -> str:
    if last_row.up_bid is not None and last_row.down_bid is not None:
        return "up" if last_row.up_bid >= last_row.down_bid else "down"
    if last_row.up_ask is not None and last_row.down_ask is not None:
        return "up" if last_row.up_ask >= last_row.down_ask else "down"
    if last_row.up_bid is not None or last_row.up_ask is not None:
        return "up"
    return "down"


def is_full_5m_event(rows: list[QuoteRow], min_coverage_seconds: int) -> bool:
    if not rows:
        return False
    slug = rows[0].market_slug
    try:
        start_ts = int(slug.rsplit("-", 1)[1])
    except Exception:
        return False

    start_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    end_dt = start_dt + timedelta(minutes=5)
    first_ts = rows[0].ts
    last_ts = rows[-1].ts
    coverage_seconds = (last_ts - first_ts).total_seconds()
    return first_ts <= end_dt and last_ts >= start_dt and coverage_seconds >= min_coverage_seconds
