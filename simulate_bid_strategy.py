from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime
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


def infer_winner_from_last_row(last_row: QuoteRow) -> str:
    # Use best available proxy for terminal state: higher final bid if both present, else ask.
    if last_row.up_bid is not None and last_row.down_bid is not None:
        return "up" if last_row.up_bid >= last_row.down_bid else "down"

    if last_row.up_ask is not None and last_row.down_ask is not None:
        return "up" if last_row.up_ask >= last_row.down_ask else "down"

    if last_row.up_bid is not None or last_row.up_ask is not None:
        return "up"
    return "down"


def simulate_event(rows: list[QuoteRow], threshold_ask: float) -> dict[str, Any] | None:
    if not rows:
        return None

    entry = first_entry_row(rows)
    last = last_valid_row(rows)
    if entry is None or last is None:
        return None

    entry_up = float(entry.up_ask)
    entry_down = float(entry.down_ask)
    total_cost = entry_up + entry_down

    exit_token: str | None = None
    exit_time: datetime | None = None
    exit_ask: float | None = None
    exit_bid: float | None = None
    tie_break = False

    for r in rows:
        if r.ts < entry.ts:
            continue

        up_hit = r.up_ask is not None and r.up_ask <= threshold_ask
        down_hit = r.down_ask is not None and r.down_ask <= threshold_ask

        if not up_hit and not down_hit:
            continue

        if up_hit and down_hit:
            # deterministic tie-break: lower ask is considered to have "fallen" first.
            up_ask = float(r.up_ask)
            down_ask = float(r.down_ask)
            if up_ask < down_ask:
                exit_token = "up"
            elif down_ask < up_ask:
                exit_token = "down"
            else:
                exit_token = "up"
                tie_break = True
        elif up_hit:
            exit_token = "up"
        else:
            exit_token = "down"

        exit_time = r.ts
        if exit_token == "up":
            exit_ask = float(r.up_ask) if r.up_ask is not None else None
            exit_bid = float(r.up_bid) if r.up_bid is not None else None
        else:
            exit_ask = float(r.down_ask) if r.down_ask is not None else None
            exit_bid = float(r.down_bid) if r.down_bid is not None else None
        break

    winner = infer_winner_from_last_row(last)

    if exit_token is None:
        # No threshold hit; holding both resolves to payoff 1 in binary market.
        cash_from_exit = 0.0
        remaining_token = "both"
        resolution_payoff = 1.0
        trigger_hit = False
    else:
        cash_from_exit = exit_bid if exit_bid is not None else 0.0
        remaining_token = "down" if exit_token == "up" else "up"
        resolution_payoff = 1.0 if remaining_token == winner else 0.0
        trigger_hit = True

    total_payoff = cash_from_exit + resolution_payoff
    pnl = total_payoff - total_cost
    roi = pnl / total_cost if total_cost > 0 else 0.0

    return {
        "market_slug": rows[0].market_slug,
        "entry_time_utc": entry.ts.isoformat(),
        "exit_time_utc": exit_time.isoformat() if exit_time else None,
        "entry_up_ask": entry_up,
        "entry_down_ask": entry_down,
        "total_entry_cost": total_cost,
        "threshold_ask": threshold_ask,
        "trigger_hit": trigger_hit,
        "exit_token": exit_token,
        "exit_ask": exit_ask,
        "exit_bid": exit_bid,
        "tie_break_used": tie_break,
        "remaining_token": remaining_token,
        "winner_token_inferred": winner,
        "resolution_payoff": resolution_payoff,
        "cash_from_exit": cash_from_exit,
        "total_payoff": total_payoff,
        "pnl": pnl,
        "roi": roi,
        "rows": len(rows),
        "last_ts_utc": last.ts.isoformat(),
    }


def write_event_csv(path: Path, events: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not events:
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["market_slug", "pnl"])
        return

    headers = list(events[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(events)


def build_summary(
    event_results: list[dict[str, Any]],
    threshold_ask: float,
    min_rows: int,
    quotes_csv: str,
    events_csv_path: str,
) -> dict[str, Any]:
    pnls = [r["pnl"] for r in event_results]
    rois = [r["roi"] for r in event_results]
    triggers = [r["trigger_hit"] for r in event_results]
    return {
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "strategy": {
            "entry": "buy both tokens at first observed quote in each market",
            "exit": f"sell first token whose ask <= {threshold_ask} at that row's best bid",
            "hold": "hold remaining token to resolution (winner inferred from final quote)",
        },
        "params": {
            "threshold_ask": threshold_ask,
            "min_rows": min_rows,
            "quotes_csv": quotes_csv,
        },
        "events_simulated": len(event_results),
        "trigger_hit_events": sum(1 for t in triggers if t),
        "trigger_hit_rate": (sum(1 for t in triggers if t) / len(event_results)) if event_results else 0.0,
        "total_pnl": sum(pnls) if pnls else 0.0,
        "avg_pnl": mean(pnls) if pnls else 0.0,
        "avg_roi": mean(rois) if rois else 0.0,
        "win_rate": (sum(1 for p in pnls if p > 0) / len(pnls)) if pnls else 0.0,
        "events_csv": events_csv_path,
    }


def generate_thresholds(start: float, end: float, step: float) -> list[float]:
    if step <= 0:
        raise ValueError("--sweep-step must be > 0")
    if end < start:
        raise ValueError("--sweep-end must be >= --sweep-start")
    values: list[float] = []
    v = start
    while v <= end + 1e-12:
        values.append(round(v, 6))
        v += step
    return values


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Simulate: buy UP+DOWN at event start, sell first token whose ask <= threshold, hold other to resolution"
    )
    parser.add_argument("--quotes-csv", default="live_btc_5m_quotes.csv")
    parser.add_argument("--threshold-ask", type=float, default=0.30)
    parser.add_argument("--min-rows", type=int, default=3)
    parser.add_argument("--output-events-csv", default="outputs/strategy_events.csv")
    parser.add_argument("--output-summary-json", default="outputs/strategy_summary.json")
    parser.add_argument("--sweep-start", type=float, default=None)
    parser.add_argument("--sweep-end", type=float, default=None)
    parser.add_argument("--sweep-step", type=float, default=0.01)
    parser.add_argument("--output-sweep-csv", default="outputs/strategy_sweep.csv")
    parser.add_argument("--output-sweep-json", default="outputs/strategy_sweep.json")
    args = parser.parse_args()

    grouped = load_quotes(Path(args.quotes_csv))
    run_sweep = args.sweep_start is not None or args.sweep_end is not None
    if run_sweep:
        if args.sweep_start is None or args.sweep_end is None:
            raise ValueError("Provide both --sweep-start and --sweep-end for sweep mode")

        thresholds = generate_thresholds(args.sweep_start, args.sweep_end, args.sweep_step)
        sweep_rows: list[dict[str, Any]] = []
        for threshold in thresholds:
            event_results: list[dict[str, Any]] = []
            for _, rows in grouped.items():
                if len(rows) < args.min_rows:
                    continue
                res = simulate_event(rows, threshold_ask=threshold)
                if res is None:
                    continue
                event_results.append(res)

            summary = build_summary(
                event_results=event_results,
                threshold_ask=threshold,
                min_rows=args.min_rows,
                quotes_csv=args.quotes_csv,
                events_csv_path="",
            )
            sweep_rows.append(
                {
                    "threshold_ask": threshold,
                    "events_simulated": summary["events_simulated"],
                    "trigger_hit_events": summary["trigger_hit_events"],
                    "trigger_hit_rate": summary["trigger_hit_rate"],
                    "total_pnl": summary["total_pnl"],
                    "avg_pnl": summary["avg_pnl"],
                    "avg_roi": summary["avg_roi"],
                    "win_rate": summary["win_rate"],
                }
            )

        sweep_rows.sort(key=lambda x: x["threshold_ask"])
        best_by_total_pnl = max(sweep_rows, key=lambda x: x["total_pnl"]) if sweep_rows else None
        best_by_avg_roi = max(sweep_rows, key=lambda x: x["avg_roi"]) if sweep_rows else None

        sweep_csv_path = Path(args.output_sweep_csv)
        sweep_csv_path.parent.mkdir(parents=True, exist_ok=True)
        with sweep_csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(sweep_rows[0].keys()) if sweep_rows else ["threshold_ask"])
            writer.writeheader()
            writer.writerows(sweep_rows)

        sweep_json = {
            "generated_at_utc": datetime.utcnow().isoformat() + "Z",
            "params": {
                "sweep_start": args.sweep_start,
                "sweep_end": args.sweep_end,
                "sweep_step": args.sweep_step,
                "min_rows": args.min_rows,
                "quotes_csv": args.quotes_csv,
            },
            "rows": sweep_rows,
            "best_by_total_pnl": best_by_total_pnl,
            "best_by_avg_roi": best_by_avg_roi,
        }
        sweep_json_path = Path(args.output_sweep_json)
        sweep_json_path.parent.mkdir(parents=True, exist_ok=True)
        sweep_json_path.write_text(json.dumps(sweep_json, indent=2), encoding="utf-8")

        print(json.dumps(sweep_json, indent=2))
        print(f"Wrote sweep CSV: {sweep_csv_path}")
        print(f"Wrote sweep JSON: {sweep_json_path}")
        return

    event_results: list[dict[str, Any]] = []
    for _, rows in grouped.items():
        if len(rows) < args.min_rows:
            continue
        res = simulate_event(rows, threshold_ask=args.threshold_ask)
        if res is None:
            continue
        event_results.append(res)

    event_results.sort(key=lambda x: x["entry_time_utc"])
    summary = build_summary(
        event_results=event_results,
        threshold_ask=args.threshold_ask,
        min_rows=args.min_rows,
        quotes_csv=args.quotes_csv,
        events_csv_path=args.output_events_csv,
    )

    events_csv_path = Path(args.output_events_csv)
    summary_json_path = Path(args.output_summary_json)
    write_event_csv(events_csv_path, event_results)
    summary_json_path.parent.mkdir(parents=True, exist_ok=True)
    summary_json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(json.dumps(summary, indent=2))
    print(f"Wrote event-level results: {events_csv_path}")
    print(f"Wrote summary: {summary_json_path}")


if __name__ == "__main__":
    main()
