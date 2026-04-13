# DTCC FX Daily ROC Pipeline

Daily pipeline for `EURUSD` and `GBPUSD` that:
- ingests DTCC public FX options data,
- ingests Dukascopy 1-minute bars,
- recalculates gamma proxy files,
- generates daily buy/sell limit report rows for both:
  - default gamma-weighted signal
  - alternate `gamma_nearest_top1_md50_dec1` signal

## Stack
- Java 17+
- Gradle

## Run
From repo root:

```bash
./scripts/run_daily_roc_pipeline.sh --report-date 2026-04-11
```

Default `--report-date` is yesterday UTC.

## Important Paths
- SQLite DB: `data/market-bars-5y.db` (default)
- Hourly bars output:
  - `tick_data/eurusd_1h.csv`
  - `tick_data/gbpusd_1h.csv`
- Gamma outputs:
  - `data/eurusd_gamma_proxy_daily_call_put.csv`
  - `data/eurusd_gamma_proxy_by_strike_call_put.csv`
  - `data/gbpusd_gamma_proxy_daily_call_put.csv`
  - `data/gbpusd_gamma_proxy_by_strike_call_put.csv`
- Daily report:
  - `data/reports/fx_limit_order_report_<YYYY-MM-DD>.csv`
  - columns include both default and alternate levels:
    - `default_buy_limit`
    - `default_sell_limit`
    - `alt_buy_limit`
    - `alt_sell_limit`

## SQLite Bootstrap Behavior
No DB file or tables need to exist in git.
On first run the pipeline creates DB parent directories, the DB file, and tables automatically.

Created tables:
- `fx_bars`
- `dtcc_option_trades`
- `dtcc_ingested_files`
- `gamma_limit_reports`

## Key Flags
- `--project-root` (default: `.`)
- `--data-dir` (default: `data`)
- `--tick-dir` (default: `tick_data`)
- `--db` (default: `data/market-bars-5y.db`)
- `--report-date` (YYYY-MM-DD)
- `--dtcc-bootstrap-start` (default: `2025-04-10`)
- `--market-bootstrap-start` (default: `report-date - 5y`)
- `--dtcc-regime` (default: `CFTC`)
- `--dtcc-asset` (default: `FX`)
- `--vol-assumption` (default: `0.10`)
- `--topn` (default: `10`)

## Notes
- DTCC ingest is incremental by file (`dtcc_ingested_files`).
- Dukascopy ingest is incremental by latest stored bar date per pair.
- Daily runtime is pure Java: DTCC ingest, hourly export, gamma recomputation and limit generation do not shell out to Python.
- The report persists the default signal into legacy `buy_limit` / `sell_limit` / `notes` columns and also stores alternate signal columns `alt_buy_limit` / `alt_sell_limit` / `alt_notes`.
- Python scripts remain in the repo for research and backtesting only.
