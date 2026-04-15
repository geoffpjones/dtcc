#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from pathlib import Path


PAIRS = ["EURUSD", "GBPUSD", "AUDUSD", "USDCAD", "USDJPY"]
PAIR_ALIAS = {
    "EURUSD": "EURUSD",
    "GBPUSD": "GBPUSD",
    "AUDUSD": "AUDUSD",
    "CADUSD": "USDCAD",
    "USDCAD": "USDCAD",
    "JPYUSD": "USDJPY",
    "USDJPY": "USDJPY",
}
AUDIT_FIELDS = [
    "Execution Timestamp",
    "Event timestamp",
    "Effective Date",
    "Expiration Date",
    "First exercise date",
    "Call amount",
    "Call currency",
    "Put amount",
    "Put currency",
    "Option Premium Amount",
    "Option Premium Currency",
    "Strike Price",
    "Strike price currency/currency pair",
    "Exchange rate",
    "Exchange rate basis",
    "UPI FISN",
    "UPI Underlier Name",
    "Action type",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Audit DTCC vanilla FX option fields needed for GK IV inversion.")
    p.add_argument("--input-csv", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--pairs", default=",".join(PAIRS))
    return p.parse_args()


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        if not rows:
            f.write("")
            return
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def normalize_pair(value: str) -> str:
    return PAIR_ALIAS.get("".join((value or "").split()).upper(), "")


def main() -> int:
    args = parse_args()
    pairs = {pair.strip().upper() for pair in args.pairs.split(",") if pair.strip()}
    output_dir = Path(args.output_dir)

    total_rows = Counter()
    field_counts = {pair: Counter() for pair in pairs}
    fisn_counts = Counter()
    premium_ccy_counts = Counter()
    strike_notation_counts = Counter()
    exchange_basis_counts = Counter()
    missing_combo_counts = defaultdict(Counter)

    with Path(args.input_csv).open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=1):
            if i % 1_000_000 == 0:
                print(f"scanned_rows={i}")

            fisn = (row.get("UPI FISN") or "").strip()
            if not fisn.startswith("NA/O Van"):
                continue

            pair = normalize_pair(row.get("UPI Underlier Name", ""))
            if pair not in pairs:
                continue

            total_rows[pair] += 1
            fisn_counts[(pair, fisn)] += 1
            premium_ccy_counts[(pair, (row.get("Option Premium Currency") or "").strip())] += 1
            strike_notation_counts[(pair, (row.get("Strike price currency/currency pair") or "").strip())] += 1
            exchange_basis_counts[(pair, (row.get("Exchange rate basis") or "").strip())] += 1

            for field in AUDIT_FIELDS:
                if (row.get(field) or "").strip() != "":
                    field_counts[pair][field] += 1

            key_fields = [
                "Execution Timestamp",
                "Expiration Date",
                "Option Premium Amount",
                "Option Premium Currency",
                "Strike Price",
                "Call amount",
                "Put amount",
                "Exchange rate",
            ]
            for field in key_fields:
                if (row.get(field) or "").strip() == "":
                    missing_combo_counts[pair][field] += 1

    coverage_rows: list[dict[str, object]] = []
    for pair in sorted(pairs):
        for field in AUDIT_FIELDS:
            count = field_counts[pair][field]
            total = total_rows[pair]
            coverage_rows.append(
                {
                    "pair": pair,
                    "total_vanilla_rows": total,
                    "field": field,
                    "non_blank_count": count,
                    "coverage_pct": round((100.0 * count / total), 4) if total else 0.0,
                }
            )

    write_csv(output_dir / "iv_input_field_coverage.csv", coverage_rows)
    write_csv(
        output_dir / "iv_input_upi_fisn_counts.csv",
        [
            {"pair": pair, "upi_fisn": fisn, "rows": count}
            for (pair, fisn), count in fisn_counts.most_common()
        ],
    )
    write_csv(
        output_dir / "iv_input_premium_currency_counts.csv",
        [
            {"pair": pair, "option_premium_currency": ccy, "rows": count}
            for (pair, ccy), count in premium_ccy_counts.most_common()
        ],
    )
    write_csv(
        output_dir / "iv_input_strike_pair_counts.csv",
        [
            {"pair": pair, "strike_pair": strike_pair, "rows": count}
            for (pair, strike_pair), count in strike_notation_counts.most_common()
        ],
    )
    write_csv(
        output_dir / "iv_input_exchange_basis_counts.csv",
        [
            {"pair": pair, "exchange_rate_basis": basis, "rows": count}
            for (pair, basis), count in exchange_basis_counts.most_common()
        ],
    )
    write_csv(
        output_dir / "iv_input_missing_key_field_counts.csv",
        [
            {"pair": pair, "field": field, "missing_rows": count, "total_vanilla_rows": total_rows[pair]}
            for pair in sorted(pairs)
            for field, count in missing_combo_counts[pair].items()
        ],
    )

    print(output_dir / "iv_input_field_coverage.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
