from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any

from strategy_utils import (
    first_entry_row,
    infer_winner,
    is_full_5m_event,
    last_valid_row,
    load_quotes,
)


def run_backtest(
    quotes_csv: Path,
    stake_per_event_usd: float,
    only_full_events: bool,
    min_coverage_seconds: int,
) -> dict[str, Any]:
    grouped = load_quotes(quotes_csv)
    events: list[dict[str, Any]] = []

    for slug, rows in grouped.items():
        if only_full_events and not is_full_5m_event(rows, min_coverage_seconds=min_coverage_seconds):
            continue

        entry = first_entry_row(rows)
        last = last_valid_row(rows)
        if entry is None or last is None:
            continue

        up_open = float(entry.up_ask)
        down_open = float(entry.down_ask)

        if up_open >= down_open:
            chosen_token = "up"
            open_price = up_open
        else:
            chosen_token = "down"
            open_price = down_open

        shares = stake_per_event_usd / open_price
        winner = infer_winner(last)
        payoff = shares if winner == chosen_token else 0.0
        pnl = payoff - stake_per_event_usd
        roi_pct = (pnl / stake_per_event_usd) * 100.0 if stake_per_event_usd > 0 else 0.0

        events.append(
            {
                "market_slug": slug,
                "entry_time_utc": entry.ts.isoformat(),
                "chosen_token": chosen_token,
                "open_price": round(open_price, 6),
                "shares_bought": round(shares, 6),
                "winner": winner,
                "payoff_usd": round(payoff, 6),
                "pnl_usd": round(pnl, 6),
                "roi_pct": round(roi_pct, 6),
            }
        )

    pnls = [e["pnl_usd"] for e in events]
    total_events = len(events)
    total_stake = total_events * stake_per_event_usd
    total_pnl = sum(pnls) if pnls else 0.0

    summary = {
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "params": {
            "quotes_csv": str(quotes_csv),
            "stake_per_event_usd": stake_per_event_usd,
            "only_full_events": only_full_events,
            "min_coverage_seconds": min_coverage_seconds,
            "rule": "buy token with higher opening ask, hold to resolution",
        },
        "events_simulated": total_events,
        "picked_up_count": sum(1 for e in events if e["chosen_token"] == "up"),
        "picked_down_count": sum(1 for e in events if e["chosen_token"] == "down"),
        "win_count": sum(1 for e in events if e["pnl_usd"] > 0),
        "win_rate": (sum(1 for e in events if e["pnl_usd"] > 0) / total_events) if total_events else 0.0,
        "total_stake_usd": round(total_stake, 6),
        "total_pnl_usd": round(total_pnl, 6),
        "avg_pnl_usd": round(mean(pnls), 6) if pnls else 0.0,
        "total_roi_pct": round((total_pnl / total_stake) * 100.0, 6) if total_stake > 0 else 0.0,
    }

    return {"summary": summary, "events": events}


def main() -> None:
    p = argparse.ArgumentParser(description="Backtest: buy expensive token at open and hold to resolution.")
    p.add_argument("--quotes-csv", default="live_btc_5m_quotes.csv")
    p.add_argument("--stake-per-event-usd", type=float, default=10.0)
    p.add_argument("--only-full-events", action="store_true")
    p.add_argument("--min-coverage-seconds", type=int, default=295)
    p.add_argument("--output-summary-json", default="outputs/strategy_expensive_open_summary.json")
    p.add_argument("--output-events-json", default="outputs/strategy_expensive_open_events.json")
    args = p.parse_args()

    result = run_backtest(
        quotes_csv=Path(args.quotes_csv),
        stake_per_event_usd=args.stake_per_event_usd,
        only_full_events=args.only_full_events,
        min_coverage_seconds=args.min_coverage_seconds,
    )

    out_summary = Path(args.output_summary_json)
    out_events = Path(args.output_events_json)
    out_summary.parent.mkdir(parents=True, exist_ok=True)
    out_events.parent.mkdir(parents=True, exist_ok=True)

    out_summary.write_text(json.dumps(result["summary"], indent=2), encoding="utf-8")
    out_events.write_text(json.dumps({"events": result["events"]}, indent=2), encoding="utf-8")

    print(json.dumps(result["summary"], indent=2))
    print(f"Wrote summary: {out_summary}")
    print(f"Wrote events: {out_events}")


if __name__ == "__main__":
    main()
