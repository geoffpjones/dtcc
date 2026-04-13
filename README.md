# DTCC FX Daily ROC Pipeline

Daily pipeline for the configured FX pair universe, currently
`EURUSD,GBPUSD,AUDUSD,USDCAD,USDJPY`, that:
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

To generate report rows only from existing hourly/gamma files:

```bash
./scripts/run_daily_roc_pipeline.sh --report-date 2026-04-13 --report-only true
```

To repair `dtcc_option_trades` from a local DTCC CSV extract:

```bash
gradle -q run -PmainClass=com.markov.fx.pipeline.DtccOptionsCsvBackfillMain --args="--options-input-csv data/options_data_full.csv --pairs AUDUSD,USDCAD,USDJPY"
```

Default `--report-date` is yesterday UTC.

## Important Paths
- SQLite DB: `data/market-bars-5y.db` (default)
- Hourly bars output:
  - `tick_data/eurusd_1h.csv`
  - `tick_data/gbpusd_1h.csv`
  - `tick_data/audusd_1h.csv`
  - `tick_data/usdcad_1h.csv`
  - `tick_data/usdjpy_1h.csv`
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
    - plus pair-selected production fields:
      - `selected_signal`
      - `selected_buy_limit`
      - `selected_sell_limit`

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
- `--pairs` (comma-separated FX symbols; default: `EURUSD,GBPUSD,AUDUSD,USDCAD,USDJPY`)
- `--options-input-csv` (optional CSV override for gamma input; if supplied, skip DB export and use this file directly)
- `--signal-selection` (optional CSV with columns `pair,selected_signal`; default is `config/signal_selection.csv` when present)
- `--report-only` (`true|false`, default: `false`) to skip ingest/recalc and only write report rows from existing files
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
- Pair selection is driven by checked-in config when available.
- Pair universe is configurable with `--pairs`; the default run covers the five pairs in the checked-in signal-selection file.
- By default the pipeline loads [config/signal_selection.csv](/home/geoffpjones/projects/dtcc/config/signal_selection.csv) if it exists.
- The supporting backtest snapshot used to create that config is stored in [signal_selection_backtest_snapshot_2026-04-13.csv](/home/geoffpjones/projects/dtcc/config/signal_selection_backtest_snapshot_2026-04-13.csv).
- If no selection file is present and none is supplied, the selected production signal defaults to `default_gamma_weighted` for all pairs.
- Python scripts remain in the repo for research and backtesting only.
