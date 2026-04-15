# Research Next

This folder is the isolated workspace for the next research cycle.

Boundary:
- do not modify production Java pipeline code from here,
- do not modify the existing `scripts/` backtest path from here,
- read existing outputs from `data/` and `tick_data/`,
- write new research outputs under `research_next/output/`.

Current goal:
- build a trade-level feature table,
- run bucketed diagnostics for momentum, volatility, ATR-adjusted distance and MA distance,
- use those diagnostics to define simple walk-forward filters later.
- separately, prototype a research-only Garman-Kohlhagen IV and gamma path from raw DTCC rows.

## Workflow

### 1. Build feature tables
```bash
python3 research_next/build_feature_table.py \
  --reopt-dir data/reopt_2026-04-13 \
  --tick-dir tick_data \
  --data-dir data \
  --output-dir research_next/output
```

### 2. Run bucketed diagnostics
```bash
python3 research_next/run_bucketed_diagnostics.py \
  --features-csv research_next/output/all_pairs_trade_features.csv \
  --output-dir research_next/output/diagnostics
```

### 3. Test simple walk-forward feature filters
```bash
python3 research_next/test_walkforward_feature_filters.py \
  --features-csv research_next/output/all_pairs_trade_features.csv \
  --output-dir research_next/output/walkforward_filters
```

### 4. Audit DTCC IV inputs
```bash
python3 research_next/audit_dtcc_iv_inputs.py \
  --input-csv data/options_data_full.csv \
  --output-dir research_next/output/iv_audit
```

### 5. Run EURUSD GK IV pilot
```bash
python3 research_next/build_gk_iv_eurusd_pilot.py \
  --input-csv data/options_data_full.csv \
  --tick-csv tick_data/eurusd_1h.csv \
  --output-csv research_next/output/gk_iv/eurusd_gk_iv_sample.csv \
  --rates-csv research_next/templates/fx_rates_tenor_daily_template.csv \
  --limit 1000
```

### 6. Assemble pair rates from normalized currency curves
```bash
python3 research_next/assemble_pair_rates_csv.py \
  --input-dir research_next/input_rates \
  --output-dir research_next/output/rates
```

### 7. Aggregate EURUSD GK trade rows into daily and by-strike gamma outputs
```bash
python3 research_next/aggregate_gk_gamma_eurusd.py \
  --gk-trades-csv research_next/output/gk_iv/eurusd_gk_iv_sample.csv \
  --tick-csv tick_data/eurusd_1h.csv \
  --output-daily-csv research_next/output/gk_gamma/eurusd_gk_gamma_daily.csv \
  --output-strike-csv research_next/output/gk_gamma/eurusd_gk_gamma_by_strike.csv
```

### 8. Run the EURUSD GK path end to end
```bash
./research_next/run_gk_eurusd_pipeline.sh \
  --input-rates-dir research_next/input_rates \
  --output-dir research_next/output/gk_run \
  --limit 1000
```

## Outputs
- `research_next/output/all_pairs_trade_features.csv`
- `research_next/output/<pair>_trade_features.csv`
- `research_next/output/diagnostics/<pair>_bucketed_feature_summary.csv`
- `research_next/output/diagnostics/<pair>_top_feature_edges.csv`
- `research_next/output/walkforward_filters/<pair>_walkforward_filter_results.csv`
- `research_next/output/walkforward_filters/all_pairs_top_walkforward_filters.csv`
- `research_next/output/iv_audit/*.csv`
- `research_next/output/gk_iv/eurusd_gk_iv_sample.csv`
- `research_next/output/gk_gamma/eurusd_gk_gamma_daily.csv`
- `research_next/output/gk_gamma/eurusd_gk_gamma_by_strike.csv`
- `research_next/output/gk_run/eurusd_gk_iv.csv`
- `research_next/output/gk_run/eurusd_gk_gamma_daily.csv`
- `research_next/output/gk_run/eurusd_gk_gamma_by_strike.csv`
- `research_next/templates/fx_rates_tenor_daily_template.csv`
- `research_next/templates/currency_curve_daily_template.csv`
- `research_next/output/rates/<pair>_rates_daily.csv`

## Notes
- This workspace starts from filled trades in the current best-trailing trade files.
- It does not yet score non-filled signal days.
- Expiry concentration near selected strike is not included yet because the current gamma csv outputs
  do not carry expiry-level breakdown. That can be added later from the DTCC trade table if needed.
- The GK pilot uses timestamp-aligned hourly spot and a tenor-aware rates csv with schema:
- Normalized per-currency rate inputs use schema:
  - `currency,date,tenor_days,rate,source`
- Pair-level rates are assembled from normalized currency curves using:
  - domestic = quote currency
  - foreign = base currency
- The assembler writes pair-level schema:
  - `pair,date,tenor_days,domestic_rate,foreign_rate,source`
- If a trade tenor falls between two tenor points on the same date, the pilot linearly interpolates.
- If the exact trade date is missing, it uses the latest prior date in the rates file.
- If no rates file can cover the trade, it falls back to the flat-rate arguments.
