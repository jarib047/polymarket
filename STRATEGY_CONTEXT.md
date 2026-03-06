# Polymarket BTC 5m Strategy Context

This file captures the strategy work completed in this project so future sessions can continue quickly.

## Scope

- Market: Polymarket Bitcoin Up/Down 5-minute markets.
- Data source used for backtests: `live_btc_5m_quotes.csv`.
- Event completeness filter used in most analyses: full 5-minute coverage (`min_coverage_seconds = 295`).

## Data/Monitoring Work Completed

- Built token cache workflows for rotating Up/Down token IDs.
- Built live quote monitoring for best bid/ask over time.
- Built event crossing analysis and plotting utilities (some later archived).
- Standardized strategy runs through script-based reproducible workflows.

## Strategy Variants Implemented

### 1) Stage2 Ask-Trigger Exit (Stage2-only)

Rule:
- Buy both tokens at event start (`$10` each in most runs).
- Sell the first token whose ask falls below threshold (sell at bid).
- Hold the remaining token to resolution.

Sweep:
- Threshold range tested: typically `0.05` to `0.95`, and later `0.05` to `0.50`.

Key result on complete events:
- Best threshold found: `0.50`.
- Approx total P/L around `+103.06` on 66 complete events.

### 2) Stage2 Bid-Trigger Exit (alternate)

Rule:
- Buy both tokens at event start.
- Sell the first token whose bid falls below threshold (sell at bid).
- Hold the remaining token to resolution.

Sweep:
- Tested across threshold ranges, including `0.05` to `0.50`.

Key result on complete events:
- Best threshold found: `0.50`.
- Approx total P/L around `+123.65` on 66 complete events.

### 3) Expensive-at-Open Hold-to-Resolution

Rule:
- At event open, buy only the token with the higher opening ask.
- Hold to resolution.

Result snapshot (`$10` stake/event, complete events):
- Events: 66
- Total P/L: `+159.248486`
- Avg P/L: `+2.412856`
- Win rate: `65.15%`
- Total ROI: `24.128558%`

Script:
- `simulate_expensive_open_strategy.py`

## Experiments Tried and Later Reverted/Archived

### Stage1 + Stage2 Logic

We tested multi-stage logic including:
- Stage1: delta-based sell/rebuy.
- Stage2: threshold exit.
- Variants where Stage1 and Stage2 precedence were adjusted.

Status:
- Later removed from active workflow to simplify and avoid logic confusion.

### Stage2 "Second Exit" Variant

We tested allowing a second Stage2 exit (dump remaining token on leader flip).

Outcome:
- This generally reduced performance in tested sample.
- Archived from active workflow.

## Archiving Decisions

Ask-trigger combined sweep runner and old mixed strategy code were archived:
- `archived/ask_trigger_scripts/run_stage2_bid_ask_sweeps.py`
- `archived/ask_trigger_scripts/simulate_delta_reentry_strategy.py`
- `archived/ask_trigger_scripts/README.txt`

Stage1-related artifacts were archived as part of cleanup.

## Active Scripts (Current)

- `main_improved.py`
  Live market utilities/monitoring workflow.

- `strategy_utils.py`
  Shared quote/event parsing and event-completeness helpers.

- `simulate_stage2_bid_trigger_sweep.py`
  Stage2 bid-trigger sweep with winner/loser sold stats.

- `simulate_expensive_open_strategy.py`
  Buy expensive token at open and hold to resolution.

## Practical Notes

- Bid-trigger often outperformed ask-trigger in this sample, likely because trigger behavior differs under spread/microstructure.
- Results are sample-dependent and sensitive to quote timing/granularity.
- Always re-run on updated data before making conclusions.
- Portability note: avoid depending on ignored folders/files when sharing or moving environments.

## Repro Quick Commands

- Stage2 bid-trigger sweep:
```bash
python simulate_stage2_bid_trigger_sweep.py --only-full-events
```

- Expensive-at-open strategy:
```bash
python simulate_expensive_open_strategy.py --only-full-events
```
