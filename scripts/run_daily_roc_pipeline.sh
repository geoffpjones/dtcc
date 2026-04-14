#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

resolve_report_date() {
  local explicit_date="$1"
  if [[ -n "$explicit_date" ]]; then
    printf '%s\n' "$explicit_date"
    return 0
  fi
  python3 - <<'PY'
from datetime import datetime, timedelta, timezone
print((datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat())
PY
}

resolve_reopt_dir() {
  local explicit_dir="${ROC_REOPT_DIR:-}"
  if [[ -n "$explicit_dir" ]]; then
    printf '%s\n' "$explicit_dir"
    return 0
  fi
  local latest_dir
  latest_dir="$(find "$ROOT/data" -maxdepth 1 -type d -name 'reopt_*' | sort | tail -n 1 || true)"
  if [[ -z "$latest_dir" ]]; then
    echo "Unable to locate reoptimization directory under data/reopt_*; set ROC_REOPT_DIR." >&2
    return 1
  fi
  printf '%s\n' "$latest_dir"
}

REPORT_DATE=""
ARGS=("$@")
while [[ $# -gt 0 ]]; do
  case "$1" in
    --report-date)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for --report-date" >&2
        exit 1
      fi
      REPORT_DATE="$2"
      shift 2
      ;;
    --report-date=*)
      REPORT_DATE="${1#*=}"
      shift
      ;;
    *)
      shift
      ;;
  esac
done

REPORT_DATE="$(resolve_report_date "$REPORT_DATE")"
REOPT_DIR="$(resolve_reopt_dir)"
REPORT_CSV="$ROOT/data/reports/fx_limit_order_report_${REPORT_DATE}.csv"
REPORT_PDF="$ROOT/data/reports/fx_daily_report_${REPORT_DATE}.pdf"

if [[ -x "$ROOT/gradlew" ]]; then
  "$ROOT/gradlew" -q run --args="${ARGS[*]}"
else
  gradle -q run --args="${ARGS[*]}"
fi

if [[ ! -f "$REPORT_CSV" ]]; then
  echo "Expected report CSV was not produced: $REPORT_CSV" >&2
  exit 1
fi

python3 "$ROOT/scripts/generate_daily_pdf_report.py" \
  --report-csv "$REPORT_CSV" \
  --reopt-dir "$REOPT_DIR" \
  --report-date "$REPORT_DATE" \
  --output-pdf "$REPORT_PDF"
