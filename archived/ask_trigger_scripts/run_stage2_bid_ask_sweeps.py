from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any

from simulate_delta_reentry_strategy import (
    QuoteRow,
    first_entry_row,
    infer_winner,
    is_full_5m_event,
    last_valid_row,
    load_quotes,
    simulate_event,
)


def fmt_elapsed(seconds: float | None) -> str | None:
    if seconds is None:
        return None
    s = int(round(seconds))
    if s < 0:
        s = 0
    mins = s // 60
    secs = s % 60
    return f"{mins} mins {secs} secs"


def avg_elapsed_label(seconds_list: list[float]) -> str | None:
    if not seconds_list:
        return None
    return fmt_elapsed(mean(seconds_list))


def extract_exit_seconds(res: dict[str, Any]) -> float | None:
    raw = res.get("stage2_exit_seconds_from_start")
    if isinstance(raw, (int, float)):
        return float(raw)

    entry_ts = res.get("entry_time_utc")
    exit_ts = res.get("final_exit_time_utc")
    if isinstance(entry_ts, str) and isinstance(exit_ts, str):
        try:
            entry = datetime.fromisoformat(entry_ts)
            exit_t = datetime.fromisoformat(exit_ts)
            return (exit_t - entry).total_seconds()
        except Exception:
            return None
    return None


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
    elapsed = (final_exit_time - entry.ts).total_seconds() if final_exit_time is not None else None

    return {
        "market_slug": rows[0].market_slug,
        "entry_time_utc": entry.ts.isoformat(),
        "last_ts_utc": last.ts.isoformat(),
        "stake_per_token_usd": stake_per_token_usd,
        "initial_total_cost_usd": initial_cost,
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
        "stage2_exit_seconds_from_start": elapsed,
        "stage2_exit_time_from_start": fmt_elapsed(elapsed),
    }


def build_complete_events_json(
    grouped: dict[str, list[QuoteRow]],
    title_by_slug: dict[str, str],
    mode: str,
    threshold: float,
    stake_per_token_usd: float,
    min_coverage_seconds: int,
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    exit_seconds: list[float] = []

    for slug, rows in grouped.items():
        if not is_full_5m_event(rows, min_coverage_seconds=min_coverage_seconds):
            continue
        entry = first_entry_row(rows)
        if entry is None:
            continue

        if mode == "ask":
            res = simulate_event(
                rows=rows,
                delta_sell_threshold=0.0,
                delta_rebuy_threshold=0.0,
                final_exit_ask_threshold=threshold,
                stake_per_token_usd=stake_per_token_usd,
            )
        else:
            res = simulate_event_bid_trigger(
                rows=rows,
                stage2_sell_bid_threshold=threshold,
                stake_per_token_usd=stake_per_token_usd,
            )

        if res is None:
            continue

        open_up = float(entry.up_ask)
        open_down = float(entry.down_ask)
        up_shares = stake_per_token_usd / open_up
        down_shares = stake_per_token_usd / open_down

        stage2_sold = res.get("final_exit_token")
        stage2_sell_price = res.get("final_exit_bid")
        stage2_pnl = 0.0
        if stage2_sold == "up" and stage2_sell_price is not None:
            stage2_pnl = (float(stage2_sell_price) - open_up) * up_shares
        elif stage2_sold == "down" and stage2_sell_price is not None:
            stage2_pnl = (float(stage2_sell_price) - open_down) * down_shares

        winner = res.get("winner_token_inferred")
        remaining_up = float(res.get("remaining_up_units", 0.0) or 0.0)
        remaining_down = float(res.get("remaining_down_units", 0.0) or 0.0)
        resolution = (winner == "up" and remaining_up > 0) or (winner == "down" and remaining_down > 0)

        elapsed = extract_exit_seconds(res)
        if elapsed is not None:
            exit_seconds.append(float(elapsed))

        pnl = float(res.get("pnl_usd", 0.0) or 0.0)
        roi_pct = (pnl / (2.0 * stake_per_token_usd)) * 100.0 if stake_per_token_usd > 0 else 0.0

        items.append(
            {
                "market_title": title_by_slug.get(slug, slug),
                "shares_bought_open_up": round(up_shares, 3),
                "open_up_price": round(open_up, 3),
                "shares_bought_open_down": round(down_shares, 3),
                "open_down_price": round(open_down, 3),
                "winner": winner,
                "stage2_sold": stage2_sold,
                "stage2_sell_price": round(float(stage2_sell_price), 3) if stage2_sell_price is not None else None,
                "stage2_pnl": round(stage2_pnl, 3),
                "stage2_exit_time_from_start": fmt_elapsed(float(elapsed)) if elapsed is not None else None,
                "resolution": bool(resolution),
                "resolution_token": winner if resolution else None,
                "total_payoff": round(pnl, 3),
                "pnl": round(pnl, 3),
                "roi": round(roi_pct, 3),
            }
        )

    count = len(items)
    total_pnl = sum(x["pnl"] for x in items)
    avg_pnl = (total_pnl / count) if count else 0.0
    avg_roi = (sum(x["roi"] for x in items) / count) if count else 0.0
    total_roi = ((total_pnl / (count * 2.0 * stake_per_token_usd)) * 100.0) if count else 0.0

    return {
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "params": {
            "trigger_mode": mode,
            "stage2_threshold": threshold,
            "stake_per_token_usd": stake_per_token_usd,
            "only_full_events": True,
            "min_coverage_seconds": min_coverage_seconds,
            "rule": f"stage2-only {'ask' if mode == 'ask' else 'bid'} trigger",
            "average_exit_time_from_start": avg_elapsed_label(exit_seconds),
            "average_pnl": round(avg_pnl, 3),
            "average_roi": round(avg_roi, 3),
            "total_pnl": round(total_pnl, 3),
            "total_roi": round(total_roi, 3),
        },
        "events_analyzed": count,
        "events": items,
    }


def run_sweep(
    grouped: dict[str, list[QuoteRow]],
    mode: str,
    threshold_values: list[float],
    stake_per_token_usd: float,
    min_coverage_seconds: int,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    best_events: list[dict[str, Any]] = []
    best_total = None

    for thr in threshold_values:
        events: list[dict[str, Any]] = []
        for _, ev_rows in grouped.items():
            if not is_full_5m_event(ev_rows, min_coverage_seconds=min_coverage_seconds):
                continue
            if mode == "ask":
                res = simulate_event(
                    rows=ev_rows,
                    delta_sell_threshold=0.0,
                    delta_rebuy_threshold=0.0,
                    final_exit_ask_threshold=thr,
                    stake_per_token_usd=stake_per_token_usd,
                )
            else:
                res = simulate_event_bid_trigger(
                    rows=ev_rows,
                    stage2_sell_bid_threshold=thr,
                    stake_per_token_usd=stake_per_token_usd,
                )
            if res is not None:
                events.append(res)

        pnls = [r["pnl_usd"] for r in events]
        rois = [r["roi"] for r in events]
        exit_seconds = [float(ex) for ex in (extract_exit_seconds(r) for r in events) if ex is not None]
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
            "stage2_threshold": thr,
            "trigger_mode": mode,
            "stake_per_token_usd": stake_per_token_usd,
            "only_full_events": True,
            "min_coverage_seconds": min_coverage_seconds,
            "events_simulated": len(events),
            "stage2_exit_events": sum(1 for x in stage2_exits if x),
            "stage2_exit_rate": (sum(1 for x in stage2_exits if x) / len(events)) if events else 0.0,
            "sold_winner_count": sold_winner_count,
            "sold_winner_rate": (sold_winner_count / len(events)) if events else 0.0,
            "sold_loser_count": sold_loser_count,
            "sold_loser_rate": (sold_loser_count / len(events)) if events else 0.0,
            "average_exit_time_from_start": avg_elapsed_label(exit_seconds),
            "total_pnl_usd": sum(pnls) if pnls else 0.0,
            "avg_pnl_usd": mean(pnls) if pnls else 0.0,
            "avg_roi": mean(rois) if rois else 0.0,
            "win_rate": (sum(1 for p in pnls if p > 0) / len(pnls)) if pnls else 0.0,
        }
        rows.append(row)

        if best_total is None or row["total_pnl_usd"] > best_total:
            best_total = row["total_pnl_usd"]
            best_events = events

    rows.sort(key=lambda r: r["stage2_threshold"])
    best = max(rows, key=lambda r: r["total_pnl_usd"]) if rows else None
    return {"rows": rows, "best": best, "best_events": best_events}


def main() -> None:
    p = argparse.ArgumentParser(description="Run stage2 sweeps for both ask-trigger and bid-trigger and emit best-case complete events JSONs.")
    p.add_argument("--quotes-csv", default="live_btc_5m_quotes.csv")
    p.add_argument("--stake-per-token-usd", type=float, default=10.0)
    p.add_argument("--threshold-start", type=float, default=0.05)
    p.add_argument("--threshold-end", type=float, default=0.50)
    p.add_argument("--threshold-step", type=float, default=0.05)
    p.add_argument("--min-coverage-seconds", type=int, default=295)
    args = p.parse_args()

    quotes_csv = Path(args.quotes_csv)
    grouped = load_quotes(quotes_csv)

    title_by_slug: dict[str, str] = {}
    with quotes_csv.open("r", encoding="utf-8", newline="") as f:
        import csv

        reader = csv.DictReader(f)
        for row in reader:
            slug = row.get("market_slug")
            title = row.get("title")
            if slug and title and slug not in title_by_slug:
                title_by_slug[slug] = title

    values: list[float] = []
    x = args.threshold_start
    while x <= args.threshold_end + 1e-12:
        values.append(round(x, 6))
        x += args.threshold_step

    ask = run_sweep(
        grouped=grouped,
        mode="ask",
        threshold_values=values,
        stake_per_token_usd=args.stake_per_token_usd,
        min_coverage_seconds=args.min_coverage_seconds,
    )
    bid = run_sweep(
        grouped=grouped,
        mode="bid",
        threshold_values=values,
        stake_per_token_usd=args.stake_per_token_usd,
        min_coverage_seconds=args.min_coverage_seconds,
    )

    ask_sweep_json = {
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "params": {
            "quotes_csv": args.quotes_csv,
            "trigger_mode": "ask",
            "threshold_range": [args.threshold_start, args.threshold_end, args.threshold_step],
            "stake_per_token_usd": args.stake_per_token_usd,
            "only_full_events": True,
            "min_coverage_seconds": args.min_coverage_seconds,
        },
        "rows": ask["rows"],
        "best_by_total_pnl": ask["best"],
    }
    bid_sweep_json = {
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "params": {
            "quotes_csv": args.quotes_csv,
            "trigger_mode": "bid",
            "threshold_range": [args.threshold_start, args.threshold_end, args.threshold_step],
            "stake_per_token_usd": args.stake_per_token_usd,
            "only_full_events": True,
            "min_coverage_seconds": args.min_coverage_seconds,
        },
        "rows": bid["rows"],
        "best_by_total_pnl": bid["best"],
    }

    ask_best_thr = float(ask["best"]["stage2_threshold"]) if ask["best"] else args.threshold_start
    bid_best_thr = float(bid["best"]["stage2_threshold"]) if bid["best"] else args.threshold_start

    ask_complete = build_complete_events_json(
        grouped=grouped,
        title_by_slug=title_by_slug,
        mode="ask",
        threshold=ask_best_thr,
        stake_per_token_usd=args.stake_per_token_usd,
        min_coverage_seconds=args.min_coverage_seconds,
    )
    bid_complete = build_complete_events_json(
        grouped=grouped,
        title_by_slug=title_by_slug,
        mode="bid",
        threshold=bid_best_thr,
        stake_per_token_usd=args.stake_per_token_usd,
        min_coverage_seconds=args.min_coverage_seconds,
    )

    out_ask_sweep = Path("outputs/strategy_stage2_ask_trigger_sweep.json")
    out_bid_sweep = Path("outputs/strategy_stage2_bid_trigger_sweep.json")
    out_ask_complete = Path("outputs/strategy_stage2_ask_trigger_complete_event_pnl.json")
    out_bid_complete = Path("outputs/strategy_stage2_bid_trigger_complete_event_pnl.json")

    out_ask_sweep.write_text(json.dumps(ask_sweep_json, indent=2), encoding="utf-8")
    out_bid_sweep.write_text(json.dumps(bid_sweep_json, indent=2), encoding="utf-8")
    out_ask_complete.write_text(json.dumps(ask_complete, indent=2), encoding="utf-8")
    out_bid_complete.write_text(json.dumps(bid_complete, indent=2), encoding="utf-8")

    print(f"Wrote {out_ask_sweep}")
    print(f"Wrote {out_bid_sweep}")
    print(f"Wrote {out_ask_complete}")
    print(f"Wrote {out_bid_complete}")
    if ask["best"]:
        print(f"Ask best threshold: {ask_best_thr} total_pnl={ask['best']['total_pnl_usd']}")
    if bid["best"]:
        print(f"Bid best threshold: {bid_best_thr} total_pnl={bid['best']['total_pnl_usd']}")


if __name__ == "__main__":
    main()
