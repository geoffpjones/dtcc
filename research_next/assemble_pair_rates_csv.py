#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path


PAIR_TO_CCY = {
    "EURUSD": ("USD", "EUR"),
    "GBPUSD": ("USD", "GBP"),
    "AUDUSD": ("USD", "AUD"),
    "USDCAD": ("CAD", "USD"),
    "USDJPY": ("JPY", "USD"),
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Assemble pair-level daily tenor curves from normalized currency curves.")
    p.add_argument("--input-dir", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--pairs", default="EURUSD,GBPUSD,AUDUSD,USDCAD,USDJPY")
    return p.parse_args()


def load_currency_curve(path: Path) -> dict[tuple[str, int], dict[str, str]]:
    out: dict[tuple[str, int], dict[str, str]] = {}
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            out[(row["date"], int(row["tenor_days"]))] = row
    return out


def main() -> int:
    args = parse_args()
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    pairs = [pair.strip().upper() for pair in args.pairs.split(",") if pair.strip()]

    cache: dict[str, dict[tuple[str, int], dict[str, str]]] = {}
    for pair in pairs:
        if pair not in PAIR_TO_CCY:
            raise ValueError(f"Unsupported pair: {pair}")
        domestic_ccy, foreign_ccy = PAIR_TO_CCY[pair]
        for ccy in {domestic_ccy, foreign_ccy}:
            if ccy not in cache:
                cache[ccy] = load_currency_curve(input_dir / f"{ccy}_curve_daily.csv")

        domestic_curve = cache[domestic_ccy]
        foreign_curve = cache[foreign_ccy]
        common_keys = sorted(set(domestic_curve).intersection(foreign_curve))
        rows: list[dict[str, object]] = []
        for key in common_keys:
            d = domestic_curve[key]
            f = foreign_curve[key]
            rows.append(
                {
                    "pair": pair,
                    "date": key[0],
                    "tenor_days": key[1],
                    "domestic_rate": d["rate"],
                    "foreign_rate": f["rate"],
                    "source": f"{domestic_ccy}:{d.get('source','')}|{foreign_ccy}:{f.get('source','')}",
                }
            )

        out_path = output_dir / f"{pair.lower()}_rates_daily.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", newline="", encoding="utf-8") as out_f:
            if rows:
                w = csv.DictWriter(out_f, fieldnames=list(rows[0].keys()))
                w.writeheader()
                w.writerows(rows)
            else:
                out_f.write("")
        print(out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
