#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

from common import parse_ts_utc
from gk import gk_gamma, solve_gk_implied_vol
from rates import load_tenor_rates, resolve_rates_for_trade


PAIR = "EURUSD"
BASE = "EUR"
QUOTE = "USD"


@dataclass
class HourBar:
    ts: datetime
    close: float


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Research-only GK IV + gamma pilot for EURUSD DTCC vanilla options.")
    p.add_argument("--input-csv", required=True)
    p.add_argument("--tick-csv", required=True)
    p.add_argument("--output-csv", required=True)
    p.add_argument("--rates-csv")
    p.add_argument("--flat-domestic-rate", type=float, default=0.04)
    p.add_argument("--flat-foreign-rate", type=float, default=0.025)
    p.add_argument("--limit", type=int)
    return p.parse_args()


def parse_number(value: str) -> float | None:
    text = (value or "").replace(",", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def load_hourly(path: Path) -> list[HourBar]:
    rows: list[HourBar] = []
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(HourBar(ts=parse_ts_utc(row["timestamp_utc"]), close=float(row["close"])))
    rows.sort(key=lambda x: x.ts)
    return rows


def spot_at_or_before(ts: datetime, rows: list[HourBar]) -> float | None:
    best: float | None = None
    for row in rows:
        if row.ts <= ts:
            best = row.close
        else:
            break
    return best


def infer_option_side(row: dict[str, str]) -> tuple[bool, float] | None:
    call_ccy = (row.get("Call currency") or "").strip().upper()
    put_ccy = (row.get("Put currency") or "").strip().upper()
    call_amt = parse_number(row.get("Call amount", ""))
    put_amt = parse_number(row.get("Put amount", ""))
    if call_ccy == BASE and put_ccy == QUOTE and call_amt:
        return True, call_amt
    if put_ccy == BASE and call_ccy == QUOTE and put_amt:
        return False, put_amt
    return None


def premium_per_base_unit(
    premium_amt: float,
    premium_ccy: str,
    base_notional: float,
    spot: float,
) -> float | None:
    if premium_amt <= 0.0 or base_notional <= 0.0 or spot <= 0.0:
        return None
    premium_ccy = premium_ccy.upper()
    if premium_ccy == QUOTE:
        return premium_amt / base_notional
    if premium_ccy == BASE:
        return (premium_amt * spot) / base_notional
    return None


def main() -> int:
    args = parse_args()
    hourly = load_hourly(Path(args.tick_csv))
    rates = load_tenor_rates(Path(args.rates_csv), PAIR) if args.rates_csv else {}

    out_rows: list[dict[str, object]] = []
    with Path(args.input_csv).open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            fisn = (row.get("UPI FISN") or "").strip()
            if not fisn.startswith("NA/O Van"):
                continue
            pair = "".join((row.get("UPI Underlier Name") or "").split()).upper()
            if pair != PAIR:
                continue

            exec_ts_text = (row.get("Execution Timestamp") or "").strip()
            expiry_text = (row.get("Expiration Date") or "").strip()
            premium_amt = parse_number(row.get("Option Premium Amount", ""))
            premium_ccy = (row.get("Option Premium Currency") or "").strip()
            strike = parse_number(row.get("Strike Price", ""))
            inferred = infer_option_side(row)
            if not exec_ts_text or not expiry_text or premium_amt is None or strike is None or inferred is None:
                continue

            exec_ts = parse_ts_utc(exec_ts_text)
            expiry = date.fromisoformat(expiry_text)
            tenor_days = (expiry - exec_ts.date()).days
            expiry_years = tenor_days / 365.0
            if expiry_years <= 0.0:
                continue

            is_call, base_notional = inferred
            spot = spot_at_or_before(exec_ts, hourly)
            if spot is None:
                continue

            resolved_rates = resolve_rates_for_trade(rates, exec_ts.date(), tenor_days) if rates else None
            if resolved_rates is None:
                rd, rf, rate_source = args.flat_domestic_rate, args.flat_foreign_rate, "flat_fallback"
            else:
                rd, rf, rate_source = resolved_rates
            premium_unit = premium_per_base_unit(premium_amt, premium_ccy, base_notional, spot)
            if premium_unit is None:
                continue

            iv = solve_gk_implied_vol(
                target_price=premium_unit,
                spot=spot,
                strike=strike,
                expiry_years=expiry_years,
                domestic_rate=rd,
                foreign_rate=rf,
                is_call=is_call,
            )
            if iv is None:
                continue

            gamma_unit = gk_gamma(
                spot=spot,
                strike=strike,
                expiry_years=expiry_years,
                domestic_rate=rd,
                foreign_rate=rf,
                sigma=iv,
            )
            # Gamma is per unit of base currency. Scale it by base notional to get a trade-level proxy.
            gamma_notional = gamma_unit * base_notional

            out_rows.append(
                {
                    "execution_ts_utc": exec_ts_text,
                    "effective_date": row.get("Effective Date", ""),
                    "expiration_date": expiry_text,
                    "upi_fisn": fisn,
                    "is_call_base": int(is_call),
                    "spot_used": round(spot, 6),
                    "strike": strike,
                    "tenor_days": tenor_days,
                    "expiry_years": round(expiry_years, 8),
                    "premium_amount": premium_amt,
                    "premium_currency": premium_ccy,
                    "premium_per_base_unit_quote_ccy": round(premium_unit, 10),
                    "base_notional": base_notional,
                    "domestic_rate": rd,
                    "foreign_rate": rf,
                    "implied_vol": round(iv, 10),
                    "gamma_per_base_unit": round(gamma_unit, 12),
                    "gamma_times_base_notional": round(gamma_notional, 6),
                    "exchange_rate_field": row.get("Exchange rate", ""),
                    "exchange_rate_basis": row.get("Exchange rate basis", ""),
                    "strike_pair": row.get("Strike price currency/currency pair", ""),
                    "option_premium_currency": premium_ccy,
                    "rate_source": rate_source,
                }
            )
            if args.limit and len(out_rows) >= args.limit:
                break

    output = Path(args.output_csv)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as f:
        if out_rows:
            w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
            w.writeheader()
            w.writerows(out_rows)
        else:
            f.write("")
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
