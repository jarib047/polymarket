from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean
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


def bid_delta(r: QuoteRow) -> float | None:
    if r.up_bid is None or r.down_bid is None:
        return None
    return abs(r.up_bid - r.down_bid)


def leader_token(r: QuoteRow) -> str | None:
    if r.up_bid is None or r.down_bid is None:
        return None
    if r.up_bid > r.down_bid:
        return "up"
    if r.down_bid > r.up_bid:
        return "down"
    return "up"


def generate_thresholds(start: float, end: float, step: float) -> list[float]:
    if step <= 0:
        raise ValueError("step must be > 0")
    if end < start:
        raise ValueError("end must be >= start")
    vals: list[float] = []
    x = start
    while x <= end + 1e-12:
        vals.append(round(x, 6))
        x += step
    return vals


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


def simulate_event(
    rows: list[QuoteRow],
    delta_sell_threshold: float,
    delta_rebuy_threshold: float,
    final_exit_ask_threshold: float,
    stake_per_token_usd: float,
) -> dict[str, Any] | None:
    if not rows:
        return None

    entry = first_entry_row(rows)
    last = last_valid_row(rows)
    if entry is None or last is None:
        return None

    cash = 0.0
    up_units = stake_per_token_usd / float(entry.up_ask)
    down_units = stake_per_token_usd / float(entry.down_ask)

    initial_cost = (up_units * float(entry.up_ask)) + (down_units * float(entry.down_ask))
    cash -= initial_cost

    # Stage1 is intentionally disabled in stage2-only mode.
    sold_leader_token: str | None = None
    sold_leader_time: datetime | None = None
    sold_leader_bid: float | None = None
    rebuy_time: datetime | None = None
    rebuy_ask: float | None = None
    stage1_done = False

    final_exit_token: str | None = None
    final_exit_time: datetime | None = None
    final_exit_bid: float | None = None
    stage2_done = False

    for r in rows:
        if r.ts < entry.ts:
            continue

        # Stage2-only execution.
        if not stage2_done and final_exit_token is None:
            up_hit = up_units > 0 and r.up_ask is not None and r.up_ask <= final_exit_ask_threshold
            down_hit = down_units > 0 and r.down_ask is not None and r.down_ask <= final_exit_ask_threshold

            chosen: str | None = None
            if up_hit and down_hit:
                chosen = "up" if float(r.up_ask) <= float(r.down_ask) else "down"
            elif up_hit:
                chosen = "up"
            elif down_hit:
                chosen = "down"

            if chosen == "up" and r.up_bid is not None:
                cash += up_units * float(r.up_bid)
                up_units = 0.0
                final_exit_token = "up"
                final_exit_time = r.ts
                final_exit_bid = float(r.up_bid)
                stage2_done = True
            elif chosen == "down" and r.down_bid is not None:
                cash += down_units * float(r.down_bid)
                down_units = 0.0
                final_exit_token = "down"
                final_exit_time = r.ts
                final_exit_bid = float(r.down_bid)
                stage2_done = True


    winner = infer_winner(last)
    resolution_payoff = 0.0
    if up_units > 0 and winner == "up":
        resolution_payoff += up_units
    if down_units > 0 and winner == "down":
        resolution_payoff += down_units

    total_payoff = cash + resolution_payoff

    return {
        "market_slug": rows[0].market_slug,
        "entry_time_utc": entry.ts.isoformat(),
        "last_ts_utc": last.ts.isoformat(),
        "stake_per_token_usd": stake_per_token_usd,
        "initial_total_cost_usd": initial_cost,
        "final_exit_ask_threshold": final_exit_ask_threshold,
        "final_exit_token": final_exit_token,
        "final_exit_time_utc": final_exit_time.isoformat() if final_exit_time else None,
        "final_exit_bid": final_exit_bid,
        "winner_token_inferred": winner,
        "remaining_up_units": up_units,
        "remaining_down_units": down_units,
        "resolution_payoff_usd": resolution_payoff,
        "total_payoff_usd": total_payoff,
        "pnl_usd": total_payoff,
        "roi": (total_payoff / initial_cost) if initial_cost > 0 else 0.0,
        "rows": len(rows),
    }


def summarize(event_results: list[dict[str, Any]]) -> dict[str, Any]:
    pnls = [r["pnl_usd"] for r in event_results]
    final_exits = [r["final_exit_token"] is not None for r in event_results]
    rois = [r["roi"] for r in event_results]
    return {
        "events_simulated": len(event_results),
        "final_exit_events": sum(1 for x in final_exits if x),
        "final_exit_rate": (sum(1 for x in final_exits if x) / len(event_results)) if event_results else 0.0,
        "total_pnl_usd": sum(pnls) if pnls else 0.0,
        "avg_pnl_usd": mean(pnls) if pnls else 0.0,
        "avg_roi": mean(rois) if rois else 0.0,
        "win_rate": (sum(1 for p in pnls if p > 0) / len(pnls)) if pnls else 0.0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Stage2-only sweep: buy both at open, sell first token whose ask<=threshold, hold the other to resolution"
        )
    )
    parser.add_argument("--quotes-csv", default="live_btc_5m_quotes.csv")
    parser.add_argument("--min-rows", type=int, default=3)
    parser.add_argument("--stage2-threshold-start", type=float, default=0.20)
    parser.add_argument("--stage2-threshold-end", type=float, default=0.20)
    parser.add_argument("--stage2-threshold-step", type=float, default=0.01)
    parser.add_argument("--stake-per-token-usd", type=float, default=10.0)
    parser.add_argument("--only-full-events", action="store_true")
    parser.add_argument("--min-coverage-seconds", type=int, default=295)
    parser.add_argument("--output-sweep-csv", default="outputs/strategy_delta_reentry_sweep.csv")
    parser.add_argument("--output-sweep-json", default="outputs/strategy_delta_reentry_sweep.json")
    parser.add_argument("--output-best-events-csv", default="outputs/strategy_delta_reentry_best_events.csv")
    args = parser.parse_args()

    grouped = load_quotes(Path(args.quotes_csv))
    thresholds = generate_thresholds(args.stage2_threshold_start, args.stage2_threshold_end, args.stage2_threshold_step)

    sweep_rows: list[dict[str, Any]] = []
    best_events: list[dict[str, Any]] = []
    best_total = None

    for stage2_thr in thresholds:
        events: list[dict[str, Any]] = []
        for _, rows in grouped.items():
            if len(rows) < args.min_rows:
                continue
            if args.only_full_events and not is_full_5m_event(rows, min_coverage_seconds=args.min_coverage_seconds):
                continue
            res = simulate_event(
                rows=rows,
                delta_sell_threshold=0.0,
                delta_rebuy_threshold=0.0,
                final_exit_ask_threshold=stage2_thr,
                stake_per_token_usd=args.stake_per_token_usd,
            )
            if res is not None:
                events.append(res)

        stats = summarize(events)
        row = {
            "stage2_sell_ask_threshold": stage2_thr,
            "stake_per_token_usd": args.stake_per_token_usd,
            "only_full_events": args.only_full_events,
            "min_coverage_seconds": args.min_coverage_seconds,
            **stats,
        }
        sweep_rows.append(row)

        if best_total is None or row["total_pnl_usd"] > best_total:
            best_total = row["total_pnl_usd"]
            best_events = events

    sweep_rows.sort(key=lambda x: x["stage2_sell_ask_threshold"])

    best_by_total = max(sweep_rows, key=lambda x: x["total_pnl_usd"]) if sweep_rows else None
    best_by_avg = max(sweep_rows, key=lambda x: x["avg_pnl_usd"]) if sweep_rows else None

    sweep_csv = Path(args.output_sweep_csv)
    sweep_csv.parent.mkdir(parents=True, exist_ok=True)
    with sweep_csv.open("w", newline="", encoding="utf-8") as f:
        headers = list(sweep_rows[0].keys()) if sweep_rows else ["stage2_sell_ask_threshold"]
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(sweep_rows)

    best_events_csv = Path(args.output_best_events_csv)
    best_events_csv.parent.mkdir(parents=True, exist_ok=True)
    with best_events_csv.open("w", newline="", encoding="utf-8") as f:
        headers = list(best_events[0].keys()) if best_events else ["market_slug"]
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(best_events)

    out = {
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "params": {
            "quotes_csv": args.quotes_csv,
            "min_rows": args.min_rows,
            "stage2_threshold_start": args.stage2_threshold_start,
            "stage2_threshold_end": args.stage2_threshold_end,
            "stage2_threshold_step": args.stage2_threshold_step,
            "stake_per_token_usd": args.stake_per_token_usd,
            "only_full_events": args.only_full_events,
            "min_coverage_seconds": args.min_coverage_seconds,
        },
        "rows": sweep_rows,
        "best_by_total_pnl": best_by_total,
        "best_by_avg_pnl": best_by_avg,
        "best_events_csv": args.output_best_events_csv,
    }

    sweep_json = Path(args.output_sweep_json)
    sweep_json.parent.mkdir(parents=True, exist_ok=True)
    sweep_json.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(json.dumps(out, indent=2))
    print(f"Wrote sweep CSV: {sweep_csv}")
    print(f"Wrote sweep JSON: {sweep_json}")
    print(f"Wrote best-threshold event details: {best_events_csv}")


if __name__ == "__main__":
    main()
