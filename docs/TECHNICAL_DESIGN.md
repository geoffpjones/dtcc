# DTCC FX Pipeline Technical Design

## 1. Purpose
This project builds a daily research/trading signal dataset for a configurable FX pair
universe, currently `EURUSD,GBPUSD,AUDUSD,USDCAD,USDJPY`, by combining:
- DTCC public FX options trade repository data,
- Dukascopy minute spot bars,
- gamma-proxy calculations by strike,
- daily limit-order signal generation.

The pipeline is intentionally deterministic and incremental so it can run as a daily batch job.

## 2. End-to-End Flow
Primary entrypoint:
- `scripts/run_daily_roc_pipeline.sh`
- Java main: `com.markov.fx.pipeline.DailyRocPipelineMain`

Daily stages:
1. DTCC ingest:
   - list cumulative files from DTCC API,
   - download new files in date range,
   - parse CSV rows from zip,
   - filter to option-like rows and target pairs,
   - persist into SQLite.
2. Dukascopy ingest:
   - fetch minute bars for missing days only,
   - upsert to SQLite market bars table.
3. Hourly export:
   - aggregate minute bars to hourly OHLCV csv per pair.
4. Gamma recomputation:
   - run native Java gamma builder,
   - write daily and strike-level call/put gamma csv outputs.
5. Limit report:
   - run native Java walk-forward limit calculator for report date,
   - calculate both the default and alternate signal variants,
   - persist report rows to SQLite and csv.

For operator workflows that only need fresh daily levels from already prepared data,
the pipeline also supports a report-only mode.
- `--report-only true` skips DTCC ingest, Dukascopy ingest, hourly export and gamma recomputation
- it reads the existing hourly and strike-gamma files already present under `tick_data/` and `data/`

## 3. Storage Model (SQLite)
Default DB: `data/market-bars-5y.db`

Tables:
- `fx_bars`
  - minute bars from Dukascopy
  - primary key: `(symbol, ts_utc)`
- `dtcc_option_trades`
  - parsed DTCC option rows for target pairs
  - unique row identity: `row_hash`
- `dtcc_ingested_files`
  - DTCC ingestion watermark and file-level stats
  - prevents duplicate file processing
- `gamma_limit_reports`
  - per-day, per-pair final limit report rows
  - stores both default and alternate signal levels

Bootstrap behavior:
- DB directories are auto-created.
- DB file is auto-created if missing.
- tables are created with `CREATE TABLE IF NOT EXISTS`.

## 4. Core Quant Assumptions

### 4.1 Notional proxy
Gamma uses a simplified notional proxy:
- option notional is approximated from DTCC leg notionals in pair currencies,
- this is a practical heuristic, not a full premium-adjusted exposure model.

### 4.1a Vanilla instrument filter
The default vanilla gamma proxy intentionally includes only DTCC rows whose `UPI FISN`
starts with `NA/O Van`.
- this keeps the signal focused on plain vanilla OTC options,
- it excludes digitals, NDOs and other exotic/non-standard structures,
- those products are omitted because their greeks and hedging behaviour are not
  comparable to the plain-vanilla gamma proxy used by the limit logic.

### 4.2 Volatility assumption
Gamma uses a fixed sigma (`--vol-assumption`, default `0.10`) for Black-Scholes gamma.
- no smile/surface calibration,
- this is a relative signal-weighting model, not a pricing model.

### 4.3 Alignment / lookahead control
Gamma inventory is aligned so day `T` reflects positions known before trading day `T`.
- roll-offs are applied end-of-day,
- new adds are applied for next day state.
- daily output still reports same-day `added_*` and `expiring_*` flows, but `active_*` and gamma are
  based on the start-of-day book only.

### 4.4 Gamma outputs
The Java gamma builder writes two files per pair:
- `*_gamma_proxy_daily_call_put.csv`
  - daily total active notionals and aggregate call/put gamma
- `*_gamma_proxy_by_strike_call_put.csv`
  - strike-level active notionals and call/put gamma

These outputs are consumed directly by the native Java limit calculator.

### 4.5 Limit derivation
Daily buy/sell levels are weighted averages of top gamma strikes:
- buy side from call-gamma strikes below previous close,
- sell side from put-gamma strikes above previous close.

Current daily report contains two signal variants:
- `default_gamma_weighted`
  - gamma-weighted average of top strikes on each side
- `gamma_nearest_top1_md50_dec1`
  - gamma-weighted ranking,
  - nearest selected strike,
  - top 1 candidate,
  - max 50 pip distance from prior close,
  - distance decay power 1

The production report also exposes a pair-selected signal view based on the latest
external signal-selection configuration.
- the pipeline does not infer “best” from repo contents
- selection is supplied via an external CSV with columns `pair,selected_signal`
- the repo currently carries a checked-in selection snapshot at `config/signal_selection.csv`
- that snapshot is backed by `config/signal_selection_backtest_snapshot_2026-04-13.csv`
- when no selection file is supplied, the selected production signal defaults to `default_gamma_weighted`

### 4.6 Execution simulation
Backtests use hourly bars with intrabar assumptions:
- fills are assumed when limit lies within `[low, high]`,
- if stop and target are both touched in same bar, policy decides precedence,
- this is bar-based simulation and cannot recover tick-level ordering.

## 5. Operational Assumptions
- Pipeline runs once daily after DTCC/market data for prior day is available.
- Daily runtime is pure Java.
- Python is only required for offline research/backtest scripts.
- Network access is required for DTCC and Dukascopy pulls.

## 6. Failure Modes and Handling
- Stage failures raise runtime errors and fail pipeline.
- DTCC and Dukascopy ingestion are incremental; reruns are idempotent on existing data.

## 7. Known Limitations
- DTCC product classification relies on field heuristics and UPI text matching.
- Gamma sizing still relies on heuristic notional proxy rather than contract-normalized Greeks.
- Strategy performance metrics depend on hourly bar assumptions.
- Pair scope is controlled by the `--pairs` flag.

## 8. Engineering Plan

### Near-term hardening
1. Add structured logging format and run-id correlation.
2. Add integration tests with a small fixture DB + fixture DTCC zip.
3. Add data-quality checks:
   - missing-day detection,
   - monotonic timestamp checks,
   - duplicate row diagnostics.
4. Add alternate signal modes to the Java limit calculator without regressing the default path.

### Mid-term improvements
1. Replace heuristic notional proxy with pair-consistent base-currency normalization.
2. Add optional implied-vol input or regime-dependent sigma.
3. Implement incremental hourly export (only new hours).
4. Add report/audit table for stage-level run metadata and statuses.

### Longer-term
1. Promote python analytics modules into package structure with tests.
2. Add CLI/API service around report generation.
3. Add scenario/backfill tooling for reproducible historical reruns.
