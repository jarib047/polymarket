# Polymarket BTC 5m Monitor

This repo tracks Polymarket **Bitcoin Up/Down 5-minute markets**, stores live best bid/ask quotes, and computes/plots 0.5 crossing statistics.

## What This Project Does
- Fetches active BTC 5m token IDs (`up` / `down`) and keeps them in a token cache JSON.
- Monitors the current 5m market in real time and appends best bid/ask quotes to CSV.
- Computes crossing metrics (how many times bids cross `0.5`) per 5m event.
- Generates per-event bid-vs-time plots and summary CSV/JSON outputs.

## Main Files
- `build_btc_5m_token_cache.py`
  - Builds/updates `btc_updown_5m_tokens.json`.
  - Merges newly fetched markets into the same existing file by `market_slug`.
- `monitor_live_btc_5m_quotes.py`
  - Reads current active market from token cache.
  - Fetches best bid/ask for up/down tokens.
  - Appends to `live_btc_5m_quotes.csv` (or creates if missing).
- `analyze_event_bid_crossings.py`
  - Reads `live_btc_5m_quotes.csv`.
  - For one event or all events in token JSON:
  - Counts 0.5 bid crossings (`up`, `down`, `either`).
  - Writes summary CSV/JSON and per-event PNG plots.
- `main_improved.py`
  - Utility script to fetch and print active order-book snapshots from token cache.
- `redacted/plot_live_btc_5m_quotes.py`
  - Optional static plotting helper.
- `redacted/animate_live_btc_5m_quotes.py`
  - Optional animation helper.

## Data / Output Locations
- Inputs / live logs:
  - `btc_updown_5m_tokens.json`
  - `live_btc_5m_quotes.csv`
- Generated analysis outputs:
  - `outputs/*.json`
  - `outputs/*.csv`
  - `outputs/event_bid_plots/*.png`
  - `outputs/images/.../*.png`

## Setup
Use Python 3.10+.

Install dependencies:

```bash
pip install requests matplotlib py-clob-client
```

## Reproduce Outputs

### 1) Refresh token cache (single canonical file)

```bash
python build_btc_5m_token_cache.py --output btc_updown_5m_tokens.json --horizon-days 3
```

### 2) Record live quotes (append mode)

```bash
python monitor_live_btc_5m_quotes.py --output live_btc_5m_quotes.csv --poll-seconds 0.01 --refresh-seconds 30
```

Useful options:
- `--overwrite` to start a fresh CSV
- `--request-retries 2 --retry-sleep-seconds 0.1` for transient timeout resilience

### 3) Generate crossings + plots for **all events** in token cache

```bash
python analyze_event_bid_crossings.py --quotes-csv live_btc_5m_quotes.csv --events-json btc_updown_5m_tokens.json --all-events
```

Default outputs created:
- `outputs/event_bid_crossings_summary.json`
- `outputs/event_bid_crossings_summary.csv`
- `outputs/event_bid_plots/*.png`

### 4) Generate crossings + plot for a **single event**

```bash
python analyze_event_bid_crossings.py --quotes-csv live_btc_5m_quotes.csv --market-slug btc-updown-5m-<timestamp>
```

## Notes
- `best bid` is computed as max bid price and `best ask` as min ask price (order-independent).
- Network/API read timeouts can occur; monitor script retries and skips failed ticks instead of crashing.
- This repo is currently focused on **live monitored crossings** (historical trade-level approach removed).
