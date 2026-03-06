from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any

from strategy_utils import (
    QuoteRow,
    first_entry_row,
    infer_winner,
    is_full_5m_event,
    last_valid_row,
    load_quotes,
)


def simulate_event_bid_trigger(
    rows: list[QuoteRow],
    stage2_sell_bid_threshold: float,
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

    final_exit_token: str | None = None
    final_exit_time = None
    final_exit_bid: float | None = None

    for r in rows:
        if r.ts < entry.ts:
            continue

        if final_exit_token is None:
            up_hit = up_units > 0 and r.up_bid is not None and r.up_bid <= stage2_sell_bid_threshold
            down_hit = down_units > 0 and r.down_bid is not None and r.down_bid <= stage2_sell_bid_threshold

            chosen: str | None = None
            if up_hit and down_hit:
                chosen = "up" if float(r.up_bid) <= float(r.down_bid) else "down"
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
            elif chosen == "down" and r.down_bid is not None:
                cash += down_units * float(r.down_bid)
                down_units = 0.0
                final_exit_token = "down"
                final_exit_time = r.ts
                final_exit_bid = float(r.down_bid)

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
        "stage2_sell_bid_threshold": stage2_sell_bid_threshold,
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stage2-only sweep using bid-threshold trigger: sell first token whose bid<=threshold"
    )
    parser.add_argument("--quotes-csv", default="live_btc_5m_quotes.csv")
    parser.add_argument("--min-rows", type=int, default=3)
    parser.add_argument("--stage2-threshold-start", type=float, default=0.05)
    parser.add_argument("--stage2-threshold-end", type=float, default=0.95)
    parser.add_argument("--stage2-threshold-step", type=float, default=0.05)
    parser.add_argument("--stake-per-token-usd", type=float, default=10.0)
    parser.add_argument("--only-full-events", action="store_true")
    parser.add_argument("--min-coverage-seconds", type=int, default=295)
    parser.add_argument("--output-sweep-csv", default="outputs/strategy_stage2_bid_trigger_sweep.csv")
    parser.add_argument("--output-sweep-json", default="outputs/strategy_stage2_bid_trigger_sweep.json")
    parser.add_argument("--output-best-events-csv", default="outputs/strategy_stage2_bid_trigger_best_events.csv")
    args = parser.parse_args()

    grouped = load_quotes(Path(args.quotes_csv))

    thresholds: list[float] = []
    x = args.stage2_threshold_start
    while x <= args.stage2_threshold_end + 1e-12:
        thresholds.append(round(x, 6))
        x += args.stage2_threshold_step

    sweep_rows: list[dict[str, Any]] = []
    best_events: list[dict[str, Any]] = []
    best_total = None

    for thr in thresholds:
        events: list[dict[str, Any]] = []
        for _, rows in grouped.items():
            if len(rows) < args.min_rows:
                continue
            if args.only_full_events and not is_full_5m_event(rows, min_coverage_seconds=args.min_coverage_seconds):
                continue
            res = simulate_event_bid_trigger(
                rows=rows,
                stage2_sell_bid_threshold=thr,
                stake_per_token_usd=args.stake_per_token_usd,
            )
            if res is not None:
                events.append(res)

        pnls = [r["pnl_usd"] for r in events]
        rois = [r["roi"] for r in events]
        stage2_exits = [r["final_exit_token"] is not None for r in events]
        sold_winner_count = sum(
            1
            for r in events
            if r.get("final_exit_token") is not None and r.get("winner_token_inferred") == r.get("final_exit_token")
        )
        sold_loser_count = sum(
            1
            for r in events
            if r.get("final_exit_token") is not None and r.get("winner_token_inferred") != r.get("final_exit_token")
        )

        row = {
            "stage2_sell_bid_threshold": thr,
            "stake_per_token_usd": args.stake_per_token_usd,
            "only_full_events": args.only_full_events,
            "min_coverage_seconds": args.min_coverage_seconds,
            "events_simulated": len(events),
            "stage2_exit_events": sum(1 for x in stage2_exits if x),
            "stage2_exit_rate": (sum(1 for x in stage2_exits if x) / len(events)) if events else 0.0,
            "sold_winner_count": sold_winner_count,
            "sold_winner_rate": (sold_winner_count / len(events)) if events else 0.0,
            "sold_loser_count": sold_loser_count,
            "sold_loser_rate": (sold_loser_count / len(events)) if events else 0.0,
            "total_pnl_usd": sum(pnls) if pnls else 0.0,
            "avg_pnl_usd": mean(pnls) if pnls else 0.0,
            "avg_roi": mean(rois) if rois else 0.0,
            "win_rate": (sum(1 for p in pnls if p > 0) / len(pnls)) if pnls else 0.0,
        }
        sweep_rows.append(row)

        if best_total is None or row["total_pnl_usd"] > best_total:
            best_total = row["total_pnl_usd"]
            best_events = events

    sweep_rows.sort(key=lambda r: r["stage2_sell_bid_threshold"])

    sweep_csv = Path(args.output_sweep_csv)
    sweep_csv.parent.mkdir(parents=True, exist_ok=True)
    with sweep_csv.open("w", newline="", encoding="utf-8") as f:
        headers = list(sweep_rows[0].keys()) if sweep_rows else ["stage2_sell_bid_threshold"]
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

    best_by_total = max(sweep_rows, key=lambda x: x["total_pnl_usd"]) if sweep_rows else None
    out = {
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "params": {
            "quotes_csv": args.quotes_csv,
            "stage2_threshold_start": args.stage2_threshold_start,
            "stage2_threshold_end": args.stage2_threshold_end,
            "stage2_threshold_step": args.stage2_threshold_step,
            "stake_per_token_usd": args.stake_per_token_usd,
            "only_full_events": args.only_full_events,
            "min_coverage_seconds": args.min_coverage_seconds,
            "rule": "stage2-only bid-trigger exit strategy",
        },
        "rows": sweep_rows,
        "best_by_total_pnl": best_by_total,
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
