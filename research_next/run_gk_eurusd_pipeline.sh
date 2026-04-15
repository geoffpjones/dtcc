#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

INPUT_CSV="$ROOT/data/options_data_full.csv"
TICK_CSV="$ROOT/tick_data/eurusd_1h.csv"
INPUT_RATES_DIR="$ROOT/research_next/input_rates"
PAIR_RATES_CSV=""
OUTPUT_DIR="$ROOT/research_next/output/gk_run"
LIMIT=""
FLAT_DOMESTIC_RATE="0.04"
FLAT_FOREIGN_RATE="0.025"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --input-csv)
      INPUT_CSV="$2"
      shift 2
      ;;
    --tick-csv)
      TICK_CSV="$2"
      shift 2
      ;;
    --input-rates-dir)
      INPUT_RATES_DIR="$2"
      shift 2
      ;;
    --pair-rates-csv)
      PAIR_RATES_CSV="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --limit)
      LIMIT="$2"
      shift 2
      ;;
    --flat-domestic-rate)
      FLAT_DOMESTIC_RATE="$2"
      shift 2
      ;;
    --flat-foreign-rate)
      FLAT_FOREIGN_RATE="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

mkdir -p "$OUTPUT_DIR"

if [[ -z "$PAIR_RATES_CSV" ]]; then
  if [[ -f "$INPUT_RATES_DIR/USD_curve_daily.csv" && -f "$INPUT_RATES_DIR/EUR_curve_daily.csv" ]]; then
    python3 "$ROOT/research_next/assemble_pair_rates_csv.py" \
      --input-dir "$INPUT_RATES_DIR" \
      --output-dir "$OUTPUT_DIR/rates" \
      --pairs EURUSD
    PAIR_RATES_CSV="$OUTPUT_DIR/rates/eurusd_rates_daily.csv"
  fi
fi

GK_TRADES_CSV="$OUTPUT_DIR/eurusd_gk_iv.csv"
GK_DAILY_CSV="$OUTPUT_DIR/eurusd_gk_gamma_daily.csv"
GK_STRIKE_CSV="$OUTPUT_DIR/eurusd_gk_gamma_by_strike.csv"

BUILD_ARGS=(
  python3 "$ROOT/research_next/build_gk_iv_eurusd_pilot.py"
  --input-csv "$INPUT_CSV"
  --tick-csv "$TICK_CSV"
  --output-csv "$GK_TRADES_CSV"
  --flat-domestic-rate "$FLAT_DOMESTIC_RATE"
  --flat-foreign-rate "$FLAT_FOREIGN_RATE"
)

if [[ -n "$PAIR_RATES_CSV" && -f "$PAIR_RATES_CSV" ]]; then
  BUILD_ARGS+=(--rates-csv "$PAIR_RATES_CSV")
fi

if [[ -n "$LIMIT" ]]; then
  BUILD_ARGS+=(--limit "$LIMIT")
fi

"${BUILD_ARGS[@]}"

python3 "$ROOT/research_next/aggregate_gk_gamma_eurusd.py" \
  --gk-trades-csv "$GK_TRADES_CSV" \
  --tick-csv "$TICK_CSV" \
  --output-daily-csv "$GK_DAILY_CSV" \
  --output-strike-csv "$GK_STRIKE_CSV"

printf '%s\n' "$GK_TRADES_CSV"
printf '%s\n' "$GK_DAILY_CSV"
printf '%s\n' "$GK_STRIKE_CSV"
