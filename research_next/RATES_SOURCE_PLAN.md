# Rates Source Plan

## Objective
Provide a maintainable historical rates input for research-only Garman-Kohlhagen implied-vol and gamma calculations.

The immediate target pairs are:
- `EURUSD`
- `GBPUSD`
- `AUDUSD`
- `USDCAD`
- `USDJPY`

## Quant Requirement
The Garman-Kohlhagen pilot needs:
- trade date
- tenor in days
- domestic rate
- foreign rate

Current research code accepts:
- pair-level daily tenor curves in:
  - `pair,date,tenor_days,domestic_rate,foreign_rate,source`

## Recommended Data Hierarchy

### Tier 1: Official risk-free overnight benchmarks plus short tenor interpolation
This is the best low-friction starting point.

Use official benchmark sources:
- `USD`: SOFR
- `EUR`: €STR
- `GBP`: SONIA
- `CAD`: CORRA
- `AUD`: RBA cash rate / AONIA proxy
- `JPY`: BOJ uncollateralized overnight call rate / TONA proxy

Why:
- official and public
- daily history available
- stable to maintain
- enough to replace the current flat-rate assumption cleanly

Limitation:
- overnight benchmarks alone do not give a full tenor curve
- term structure still needs approximation

### Tier 2: Official short tenor money-market yields / bills by currency
Preferred next upgrade after overnight-only.

Goal:
- derive a daily tenor curve at points such as:
  - `7`
  - `30`
  - `90`
  - `180`
  - `365`

Examples:
- `USD`: SOFR plus Treasury bill yields
- `EUR`: €STR plus EUR money-market / OIS proxy if available
- `GBP`: SONIA plus short sterling curve proxy
- `CAD`: CORRA plus Canadian T-bill yields
- `AUD`: cash rate plus short bank bill / government bill proxy
- `JPY`: overnight call rate plus short JPY bill proxy

This is the best target for research quality before building a full production curve framework.

### Tier 3: OIS / swap curves
Best quality, but higher engineering cost.

Use if:
- the IV-based gamma materially improves signal quality,
- and the simpler Tier 1 / Tier 2 curve approximation proves insufficient.

## Current Source Options

### USD
- `SOFR`
  - official source: Federal Reserve Bank of New York / FRED mirror
  - example:
    - https://fred.stlouisfed.org/series/SOFR

### EUR
- `€STR`
  - official source: ECB Data Portal
  - examples:
    - https://www.ecb.europa.eu/stats/euro-short-term-rates/html/index.en.html
    - https://data.ecb.europa.eu/data/data-categories/financial-markets-and-interest-rates/euro-money-market/euro-short-term-rate

### GBP
- `SONIA`
  - official source: Bank of England
  - examples:
    - https://www.bankofengland.co.uk/markets/sonia-benchmark
    - https://www.bankofengland.co.uk/markets/sonia-benchmark/sonia-key-features-and-policies

### CAD
- `CORRA`
  - official source: Bank of Canada
  - examples:
    - https://www.bankofcanada.ca/rates/interest-rates/corra/
    - https://www.bankofcanada.ca/rates/interest-rates/money-market-yields/

### AUD
- `Cash Rate / AONIA proxy`
  - official source: RBA
  - examples:
    - https://www.rba.gov.au/statistics/cash-rate/
    - https://www.rba.gov.au/cash-rate-target-overview.html

### JPY
- `Uncollateralized Overnight Call Rate / TONA proxy`
  - official source: Bank of Japan
  - examples:
    - https://www.boj.or.jp/en/statistics/market/short/mutan/d_release/md/2026/index.htm
    - https://www.boj.or.jp/en/statistics/market/short/mutan/index.htm

## Recommended Starting Design

### Step 1
Collect normalized per-currency daily tenor curves into:
- `research_next/input_rates/<CCY>_curve_daily.csv`

Normalized schema:
- `currency`
- `date`
- `tenor_days`
- `rate`
- `source`

### Step 2
Assemble pair-level curves using:
- domestic currency = quote currency
- foreign currency = base currency

For example:
- `EURUSD`
  - domestic = `USD`
  - foreign = `EUR`

### Step 3
Write assembled pair curves into:
- `research_next/output/rates/<pair>_rates_daily.csv`

Schema:
- `pair`
- `date`
- `tenor_days`
- `domestic_rate`
- `foreign_rate`
- `source`

### Step 4
Use the assembled file as input to:
- `research_next/build_gk_iv_eurusd_pilot.py`

## Practical Recommendation

### Immediate implementation
Use a research-only curve built from:
- official overnight benchmarks
- simple tenor replication or interpolation

This is already better than the current fixed-rate assumption and keeps engineering manageable.

### Next upgrade
Replace overnight-only approximations with:
- official short tenor yields
- per-currency daily tenor points

## Engineering Boundary
This work remains isolated to `research_next/` until:
- IV inversion is stable
- IV-based gamma can be aggregated cleanly
- backtests show improvement versus the existing gamma proxy
