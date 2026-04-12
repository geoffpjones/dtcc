# DTCC FX Pipeline Technical Design

## 1. Purpose
This project builds a daily research/trading signal dataset for `EURUSD` and `GBPUSD` by combining:
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
   - call python `scripts/build_gamma_for_pairs.py`.
5. Limit report:
   - call python `scripts/backtest_walkforward_gamma_limits.py` for report date,
   - persist report rows to SQLite and csv.

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

Bootstrap behavior:
- DB directories are auto-created.
- DB file is auto-created if missing.
- tables are created with `CREATE TABLE IF NOT EXISTS`.

## 4. Core Quant Assumptions

### 4.1 Notional proxy
Gamma scripts use a simplified notional proxy:
- option notional is approximated from DTCC leg notionals in pair currencies,
- this is a practical heuristic, not a full premium-adjusted exposure model.

### 4.2 Volatility assumption
Gamma uses a fixed sigma (`--vol-assumption`, default `0.10`) for BS gamma.
- no smile/surface calibration,
- this is a relative signal-weighting model, not a pricing model.

### 4.3 Alignment / lookahead control
Gamma inventory is aligned so day `T` reflects positions known before trading day `T`.
- roll-offs are applied end-of-day,
- new adds are applied for next day state.

### 4.4 Limit derivation
Daily buy/sell levels are weighted averages of top gamma strikes:
- buy side from call-gamma strikes below previous close,
- sell side from put-gamma strikes above previous close.

### 4.5 Execution simulation
Backtests use hourly bars with intrabar assumptions:
- fills are assumed when limit lies within `[low, high]`,
- if stop and target are both touched in same bar, policy decides precedence,
- this is bar-based simulation and cannot recover tick-level ordering.

## 5. Operational Assumptions
- Pipeline runs once daily after DTCC/market data for prior day is available.
- Python scripts are available in environment and callable by configured interpreter.
- Network access is required for DTCC and Dukascopy pulls.

## 6. Failure Modes and Handling
- Stage failures raise runtime errors and fail pipeline.
- external commands have timeout (`--command-timeout-sec`).
- DTCC and Dukascopy ingestion are incremental; reruns are idempotent on existing data.

## 7. Known Limitations
- DTCC product classification relies on field heuristics and UPI text matching.
- Strategy performance metrics depend on hourly bar assumptions.
- Current pair scope in Java orchestrator is fixed to `EURUSD,GBPUSD`.

## 8. Engineering Plan

### Near-term hardening
1. Move pair universe to config flag and validate symbols.
2. Add structured logging format and run-id correlation.
3. Add integration tests with a small fixture DB + fixture DTCC zip.
4. Add data-quality checks:
   - missing-day detection,
   - monotonic timestamp checks,
   - duplicate row diagnostics.

### Mid-term improvements
1. Replace heuristic notional proxy with pair-consistent base-currency normalization.
2. Add optional implied-vol input or regime-dependent sigma.
3. Implement incremental hourly export (only new hours).
4. Add report/audit table for stage-level run metadata and statuses.

### Longer-term
1. Promote python analytics modules into package structure with tests.
2. Add CLI/API service around report generation.
3. Add scenario/backfill tooling for reproducible historical reruns.
