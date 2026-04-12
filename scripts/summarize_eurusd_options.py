#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
import re

PAIR_RE = re.compile(r"\bEUR USD\b", re.IGNORECASE)
CALL_RE = re.compile(r"\bCall\b", re.IGNORECASE)
PUT_RE = re.compile(r"\bPut\b", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Create EURUSD option daily summary and strike map.")
    p.add_argument("--input", default="/home/geoffpjones/projects/dtcc/data/options_data_full.csv")
    p.add_argument(
        "--daily-output",
        default="/home/geoffpjones/projects/dtcc/data/eurusd_options_daily_summary.csv",
    )
    p.add_argument(
        "--strike-output",
        default="/home/geoffpjones/projects/dtcc/data/eurusd_options_strike_map.csv",
    )
    return p.parse_args()


def parse_decimal(value: str) -> Decimal:
    v = (value or "").strip().replace(",", "")
    if not v:
        return Decimal("0")
    try:
        return Decimal(v)
    except InvalidOperation:
        return Decimal("0")


def eur_notional(row: dict[str, str]) -> Decimal:
    total = Decimal("0")
    if (row.get("Notional currency-Leg 1") or "").strip().upper() == "EUR":
        total += parse_decimal(row.get("Notional amount-Leg 1") or "")
    if (row.get("Notional currency-Leg 2") or "").strip().upper() == "EUR":
        total += parse_decimal(row.get("Notional amount-Leg 2") or "")
    return total


def fmt_decimal(value: Decimal) -> str:
    s = format(value, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s or "0"


def normalize_strike(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return "(blank)"
    d = parse_decimal(raw)
    return fmt_decimal(d)


def option_side(fisn: str) -> str | None:
    if CALL_RE.search(fisn):
        return "Call"
    if PUT_RE.search(fisn):
        return "Put"
    return None


def row_date(row: dict[str, str]) -> str:
    d = (row.get("source_date") or "").strip()
    if d:
        return d
    ts = (row.get("Event timestamp") or "").strip()
    if len(ts) >= 10:
        return ts[:10]
    return ""


def main() -> int:
    args = parse_args()
    in_path = Path(args.input)
    daily_path = Path(args.daily_output)
    strike_path = Path(args.strike_output)
    daily_path.parent.mkdir(parents=True, exist_ok=True)
    strike_path.parent.mkdir(parents=True, exist_ok=True)

    daily = defaultdict(
        lambda: {
            "Call": {"trade_count": 0, "notional_eur": Decimal("0"), "strikes": set()},
            "Put": {"trade_count": 0, "notional_eur": Decimal("0"), "strikes": set()},
        }
    )
    strike_map = defaultdict(lambda: {"trade_count": 0, "notional_eur": Decimal("0")})

    scanned = 0
    matched = 0

    with in_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            scanned += 1
            fisn = (row.get("UPI FISN") or "").strip()
            if not fisn or not PAIR_RE.search(fisn):
                continue

            side = option_side(fisn)
            if side is None:
                continue

            d = row_date(row)
            if not d:
                continue

            strike = normalize_strike(row.get("Strike Price") or "")
            notional = eur_notional(row)
            matched += 1

            daily[d][side]["trade_count"] += 1
            daily[d][side]["notional_eur"] += notional
            daily[d][side]["strikes"].add(strike)

            key = (d, side, strike)
            strike_map[key]["trade_count"] += 1
            strike_map[key]["notional_eur"] += notional

    with daily_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "date",
                "call_trade_count",
                "put_trade_count",
                "total_trade_count",
                "call_notional_eur",
                "put_notional_eur",
                "total_notional_eur",
                "call_distinct_strikes",
                "put_distinct_strikes",
                "total_distinct_strikes",
            ]
        )
        for d in sorted(daily.keys()):
            call = daily[d]["Call"]
            put = daily[d]["Put"]
            total_strikes = call["strikes"] | put["strikes"]
            w.writerow(
                [
                    d,
                    call["trade_count"],
                    put["trade_count"],
                    call["trade_count"] + put["trade_count"],
                    fmt_decimal(call["notional_eur"]),
                    fmt_decimal(put["notional_eur"]),
                    fmt_decimal(call["notional_eur"] + put["notional_eur"]),
                    len(call["strikes"]),
                    len(put["strikes"]),
                    len(total_strikes),
                ]
            )

    with strike_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "option_side", "strike_price", "trade_count", "notional_eur"])
        for d, side, strike in sorted(strike_map.keys()):
            row = strike_map[(d, side, strike)]
            w.writerow([d, side, strike, row["trade_count"], fmt_decimal(row["notional_eur"])])

    print(f"Scanned rows: {scanned}")
    print(f"Matched EURUSD call/put option rows: {matched}")
    print(f"Daily summary: {daily_path}")
    print(f"Strike map: {strike_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
