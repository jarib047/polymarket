from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt


@dataclass
class QuoteRow:
    ts: datetime
    market_slug: str
    up_bid: float | None
    down_bid: float | None


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load_quotes(csv_path: Path) -> list[QuoteRow]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Quotes CSV not found: {csv_path}")

    rows: list[QuoteRow] = []
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            slug = r.get("market_slug")
            ts_raw = r.get("ts_utc")
            if not slug or not ts_raw:
                continue
            try:
                ts = parse_iso(ts_raw)
            except Exception:
                continue

            rows.append(
                QuoteRow(
                    ts=ts,
                    market_slug=slug,
                    up_bid=to_float(r.get("up_best_bid")),
                    down_bid=to_float(r.get("down_best_bid")),
                )
            )

    rows.sort(key=lambda x: x.ts)
    return rows


def load_event_slugs_from_json(events_json_path: Path) -> list[str]:
    if not events_json_path.exists():
        raise FileNotFoundError(f"Events JSON not found: {events_json_path}")
    data = json.loads(events_json_path.read_text(encoding="utf-8"))
    markets = data.get("markets", [])
    slugs: list[str] = []
    for m in markets:
        slug = m.get("market_slug")
        if isinstance(slug, str) and slug:
            slugs.append(slug)
    # preserve order, remove duplicates
    seen = set()
    ordered = []
    for s in slugs:
        if s not in seen:
            seen.add(s)
            ordered.append(s)
    return ordered


def sign(value: float | None, threshold: float = 0.5) -> int:
    if value is None:
        return 0
    if value > threshold:
        return 1
    if value < threshold:
        return -1
    return 0


def count_crossings(series: list[float | None], threshold: float = 0.5) -> int:
    crossings = 0
    prev_non_zero: int | None = None

    for v in series:
        s = sign(v, threshold)
        if s == 0:
            continue
        if prev_non_zero is None:
            prev_non_zero = s
            continue
        if s != prev_non_zero:
            crossings += 1
            prev_non_zero = s

    return crossings


def analyze_event(rows: list[QuoteRow], market_slug: str, threshold: float = 0.5) -> dict[str, Any]:
    event_rows = [r for r in rows if r.market_slug == market_slug]
    event_rows.sort(key=lambda x: x.ts)

    up_series = [r.up_bid for r in event_rows]
    down_series = [r.down_bid for r in event_rows]

    up_cross = count_crossings(up_series, threshold=threshold)
    down_cross = count_crossings(down_series, threshold=threshold)

    either_cross = 0
    prev_up: int | None = None
    prev_down: int | None = None
    for r in event_rows:
        up_s = sign(r.up_bid, threshold)
        down_s = sign(r.down_bid, threshold)
        up_evt = False
        down_evt = False

        if up_s != 0:
            if prev_up is not None and up_s != prev_up:
                up_evt = True
            prev_up = up_s

        if down_s != 0:
            if prev_down is not None and down_s != prev_down:
                down_evt = True
            prev_down = down_s

        if up_evt or down_evt:
            either_cross += 1

    return {
        "market_slug": market_slug,
        "rows": len(event_rows),
        "first_ts_utc": event_rows[0].ts.isoformat() if event_rows else None,
        "last_ts_utc": event_rows[-1].ts.isoformat() if event_rows else None,
        "up_bid_crossings_0_5": up_cross,
        "down_bid_crossings_0_5": down_cross,
        "either_bid_crossings_0_5": either_cross,
    }


def plot_event(rows: list[QuoteRow], market_slug: str, output_png: Path) -> bool:
    event_rows = [r for r in rows if r.market_slug == market_slug]
    event_rows.sort(key=lambda x: x.ts)
    if not event_rows:
        return False

    x = [r.ts for r in event_rows]
    up = [r.up_bid for r in event_rows]
    down = [r.down_bid for r in event_rows]

    output_png.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(10, 4.5))
    plt.plot(x, up, label="UP bid", color="#1f77b4", linewidth=1.2)
    plt.plot(x, down, label="DOWN bid", color="#d62728", linewidth=1.2)
    plt.axhline(0.5, color="#666666", linestyle="--", linewidth=1.0, label="0.50")
    plt.title(f"{market_slug} | UP/DOWN Bid vs Time")
    plt.ylim(0, 1)
    plt.xlabel("Time")
    plt.ylabel("Bid Price")
    plt.grid(alpha=0.25)
    plt.legend(loc="best")
    plt.tight_layout()
    plt.savefig(output_png, dpi=160)
    plt.close()
    return True


def write_summary_json(path: Path, results: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "events_analyzed": len(results),
        "results": results,
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_summary_csv(path: Path, results: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not results:
        headers = [
            "market_slug",
            "rows",
            "first_ts_utc",
            "last_ts_utc",
            "up_bid_crossings_0_5",
            "down_bid_crossings_0_5",
            "either_bid_crossings_0_5",
        ]
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
        return

    headers = list(results[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(results)


def sanitize_filename(slug: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in slug)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Analyze UP/DOWN bid 0.5 crossings per 5m event and generate per-event bid plots"
    )
    parser.add_argument("--quotes-csv", default="live_btc_5m_quotes.csv")
    parser.add_argument("--events-json", default="btc_updown_5m_tokens.json")
    parser.add_argument("--market-slug", default=None, help="Analyze only one event slug")
    parser.add_argument("--all-events", action="store_true", help="Analyze all market_slug values from events JSON")
    parser.add_argument("--output-dir", default="outputs/event_bid_plots")
    parser.add_argument("--summary-json", default="outputs/event_bid_crossings_summary.json")
    parser.add_argument("--summary-csv", default="outputs/event_bid_crossings_summary.csv")
    parser.add_argument("--threshold", type=float, default=0.5)
    args = parser.parse_args()

    if not args.market_slug and not args.all_events:
        raise ValueError("Provide either --market-slug <slug> or --all-events")

    quotes = load_quotes(Path(args.quotes_csv))

    if args.market_slug:
        target_slugs = [args.market_slug]
    else:
        target_slugs = load_event_slugs_from_json(Path(args.events_json))

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, Any]] = []
    for slug in target_slugs:
        result = analyze_event(quotes, slug, threshold=args.threshold)
        if result["rows"] == 0:
            continue
        plot_path = output_dir / f"{sanitize_filename(slug)}.png"
        plotted = plot_event(quotes, slug, plot_path)
        result["plot_file"] = str(plot_path)
        result["plot_created"] = bool(plotted)
        results.append(result)

    write_summary_json(Path(args.summary_json), results)
    write_summary_csv(Path(args.summary_csv), results)

    print(
        f"Analyzed {len(results)} events. "
        f"Summary JSON: {args.summary_json} | Summary CSV: {args.summary_csv} | Plots dir: {args.output_dir}"
    )


if __name__ == "__main__":
    main()
