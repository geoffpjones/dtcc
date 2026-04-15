#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from common import build_daily_bars, load_hourly_csv
from gk import gk_gamma


@dataclass
class TradeRow:
    effective_date: date
    expiration_date: date
    strike: float
    is_call_base: bool
    base_notional: float
    domestic_rate: float
    foreign_rate: float
    implied_vol: float


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Aggregate research-only EURUSD GK trade rows into daily and by-strike gamma files.")
    p.add_argument("--gk-trades-csv", required=True)
    p.add_argument("--tick-csv", required=True)
    p.add_argument("--output-daily-csv", required=True)
    p.add_argument("--output-strike-csv", required=True)
    return p.parse_args()


def load_gk_trades(path: Path) -> list[TradeRow]:
    out: list[TradeRow] = []
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            out.append(
                TradeRow(
                    effective_date=date.fromisoformat(row["effective_date"]),
                    expiration_date=date.fromisoformat(row["expiration_date"]),
                    strike=float(row["strike"]),
                    is_call_base=row["is_call_base"] == "1",
                    base_notional=float(row["base_notional"]),
                    domestic_rate=float(row["domestic_rate"]),
                    foreign_rate=float(row["foreign_rate"]),
                    implied_vol=float(row["implied_vol"]),
                )
            )
    return out


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        if rows:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        else:
            f.write("")


def main() -> int:
    args = parse_args()
    trades = load_gk_trades(Path(args.gk_trades_csv))
    daily_bars = build_daily_bars(load_hourly_csv(Path(args.tick_csv)))

    daily_rows: list[dict[str, object]] = []
    strike_rows: list[dict[str, object]] = []

    for bar in daily_bars:
        current_day = bar.day
        spot = bar.close

        added_call_notional = sum(t.base_notional for t in trades if t.effective_date == current_day and t.is_call_base)
        added_put_notional = sum(t.base_notional for t in trades if t.effective_date == current_day and not t.is_call_base)
        expiring_call_notional = sum(t.base_notional for t in trades if t.expiration_date == current_day and t.is_call_base)
        expiring_put_notional = sum(t.base_notional for t in trades if t.expiration_date == current_day and not t.is_call_base)

        # Match the no-lookahead rule used elsewhere: trades reported on day T enter the active book on T+1,
        # while expiries remain active through their expiration date and roll off after that day.
        active = [t for t in trades if t.effective_date < current_day <= t.expiration_date]

        strike_bucket: dict[float, dict[str, float]] = defaultdict(lambda: {"call_notional": 0.0, "put_notional": 0.0, "call_gamma": 0.0, "put_gamma": 0.0})
        total_call_notional = 0.0
        total_put_notional = 0.0
        total_call_gamma = 0.0
        total_put_gamma = 0.0

        for trade in active:
            remaining_days = (trade.expiration_date - current_day).days
            expiry_years = max(remaining_days, 1) / 365.0
            gamma_per_unit = gk_gamma(
                spot=spot,
                strike=trade.strike,
                expiry_years=expiry_years,
                domestic_rate=trade.domestic_rate,
                foreign_rate=trade.foreign_rate,
                sigma=trade.implied_vol,
            )
            gamma_scaled = gamma_per_unit * trade.base_notional
            bucket = strike_bucket[trade.strike]
            if trade.is_call_base:
                bucket["call_notional"] += trade.base_notional
                bucket["call_gamma"] += gamma_scaled
                total_call_notional += trade.base_notional
                total_call_gamma += gamma_scaled
            else:
                bucket["put_notional"] += trade.base_notional
                bucket["put_gamma"] += gamma_scaled
                total_put_notional += trade.base_notional
                total_put_gamma += gamma_scaled

        for strike, bucket in sorted(strike_bucket.items()):
            total_gamma = bucket["call_gamma"] + bucket["put_gamma"]
            dist = abs(spot - strike)
            strike_rows.append(
                {
                    "date": current_day.isoformat(),
                    "spot_close": round(spot, 6),
                    "strike": f"{strike:.6f}",
                    "active_call_notional": round(bucket["call_notional"], 2),
                    "active_put_notional": round(bucket["put_notional"], 2),
                    "call_gamma_abs_per_usd": round(bucket["call_gamma"], 6),
                    "put_gamma_abs_per_usd": round(bucket["put_gamma"], 6),
                    "total_gamma_abs_per_usd": round(total_gamma, 6),
                    "dist_spot_to_strike": round(dist, 6),
                    "dist_spot_to_strike_pct": round((dist / spot) if spot else 0.0, 6),
                }
            )

        daily_rows.append(
            {
                "date": current_day.isoformat(),
                "spot_close": round(spot, 6),
                "added_call_notional": round(added_call_notional, 2),
                "added_put_notional": round(added_put_notional, 2),
                "expiring_call_notional": round(expiring_call_notional, 2),
                "expiring_put_notional": round(expiring_put_notional, 2),
                "active_call_notional": round(total_call_notional, 2),
                "active_put_notional": round(total_put_notional, 2),
                "active_total_notional": round(total_call_notional + total_put_notional, 2),
                "call_gamma_abs_per_usd": round(total_call_gamma, 6),
                "put_gamma_abs_per_usd": round(total_put_gamma, 6),
                "total_gamma_abs_per_usd": round(total_call_gamma + total_put_gamma, 6),
            }
        )

    write_csv(Path(args.output_daily_csv), daily_rows)
    write_csv(Path(args.output_strike_csv), strike_rows)
    print(args.output_daily_csv)
    print(args.output_strike_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
