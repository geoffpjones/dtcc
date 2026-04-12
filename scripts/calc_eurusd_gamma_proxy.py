#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Estimate EURUSD daily gamma-hedging proxy by strike and roll-off.')
    p.add_argument('--options-input', default='/home/geoffpjones/projects/dtcc/data/options_data_full.csv')
    p.add_argument('--spot-input', default='/home/geoffpjones/projects/dtcc/tick_data/eurusd_1h.csv')
    p.add_argument('--daily-output', default='/home/geoffpjones/projects/dtcc/data/eurusd_gamma_proxy_daily.csv')
    p.add_argument('--strike-output', default='/home/geoffpjones/projects/dtcc/data/eurusd_gamma_proxy_by_strike.csv')
    p.add_argument('--vol-assumption', type=float, default=0.10, help='Flat implied vol assumption, e.g. 0.10')
    p.add_argument('--include-exotics', action='store_true', help='Include non-vanilla options')
    return p.parse_args()


def f(v: str) -> float:
    t = (v or '').strip().replace(',', '')
    if not t:
        return 0.0
    try:
        return float(t)
    except Exception:
        return 0.0


def parse_day(v: str) -> date | None:
    t = (v or '').strip()
    if not t:
        return None
    try:
        return date.fromisoformat(t[:10])
    except Exception:
        return None


def parse_spot_daily(path: Path) -> dict[date, float]:
    grouped: dict[date, tuple[datetime, float]] = {}
    with path.open(newline='', encoding='utf-8') as fh:
        r = csv.DictReader(fh)
        for row in r:
            ts = datetime.fromisoformat(row['timestamp_utc'])
            d = ts.date()
            c = f(row['close'])
            prev = grouped.get(d)
            if prev is None or ts > prev[0]:
                grouped[d] = (ts, c)
    return {d: v[1] for d, v in grouped.items()}


def eur_notional(row: dict[str, str]) -> float:
    total = 0.0
    if (row.get('Notional currency-Leg 1') or '').strip().upper() == 'EUR':
        total += f(row.get('Notional amount-Leg 1') or '')
    if (row.get('Notional currency-Leg 2') or '').strip().upper() == 'EUR':
        total += f(row.get('Notional amount-Leg 2') or '')
    return total


@dataclass(frozen=True)
class Key:
    strike: float
    expiry: date


def norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def bs_gamma(s: float, k: float, t_years: float, sigma: float) -> float:
    if s <= 0 or k <= 0 or t_years <= 0 or sigma <= 0:
        return 0.0
    vol_sqrt_t = sigma * math.sqrt(t_years)
    if vol_sqrt_t <= 0:
        return 0.0
    d1 = (math.log(s / k) + 0.5 * sigma * sigma * t_years) / vol_sqrt_t
    return norm_pdf(d1) / (s * vol_sqrt_t)


def main() -> int:
    args = parse_args()

    opt_path = Path(args.options_input)
    spot_path = Path(args.spot_input)
    daily_out = Path(args.daily_output)
    strike_out = Path(args.strike_output)
    daily_out.parent.mkdir(parents=True, exist_ok=True)
    strike_out.parent.mkdir(parents=True, exist_ok=True)

    spot_close = parse_spot_daily(spot_path)

    adds_by_day: dict[date, dict[Key, float]] = defaultdict(lambda: defaultdict(float))
    roll_by_day: dict[date, dict[Key, float]] = defaultdict(lambda: defaultdict(float))

    rows_scanned = 0
    rows_used = 0

    with opt_path.open(newline='', encoding='utf-8') as fh:
        r = csv.DictReader(fh)
        for row in r:
            rows_scanned += 1
            upi = (row.get('UPI FISN') or '').strip()
            if 'EUR USD' not in upi or ('Call' not in upi and 'Put' not in upi):
                continue
            if not args.include_exotics and ' Van ' not in upi:
                continue
            if (row.get('Action type') or '').strip() != 'NEWT':
                continue

            strike = f(row.get('Strike Price') or '')
            expiry = parse_day(row.get('Expiration Date') or '')
            start = parse_day(row.get('source_date') or '')
            if start is None:
                start = parse_day(row.get('Event timestamp') or '')

            if strike <= 0 or expiry is None or start is None:
                continue
            if expiry < start:
                continue

            notional = eur_notional(row)
            if notional <= 0:
                continue

            key = Key(strike=round(strike, 6), expiry=expiry)
            adds_by_day[start][key] += notional
            roll_by_day[expiry][key] += notional
            rows_used += 1

    if not adds_by_day:
        print('No qualifying option rows found.')
        return 1

    start_day = min(adds_by_day)
    end_day = max(max(roll_by_day), max(spot_close))

    all_days: list[date] = []
    cur = start_day
    while cur <= end_day:
        all_days.append(cur)
        cur = date.fromordinal(cur.toordinal() + 1)

    active: dict[Key, float] = defaultdict(float)

    with daily_out.open('w', newline='', encoding='utf-8') as fd, strike_out.open('w', newline='', encoding='utf-8') as fs:
        wd = csv.writer(fd)
        ws = csv.writer(fs)

        wd.writerow([
            'date', 'spot_close',
            'added_notional_eur', 'expiring_notional_eur', 'active_notional_eur_startofday',
            'active_strike_expiry_nodes', 'active_strikes',
            'gamma_exposure_abs_per_usd', 'gamma_exposure_signed_assume_dealer_short_per_usd',
            'hedge_eur_for_1pip_move_abs', 'hedge_eur_for_1pct_move_abs',
        ])
        ws.writerow([
            'date', 'spot_close', 'strike',
            'active_notional_eur', 'active_nodes_at_strike',
            'gamma_exposure_abs_per_usd', 'hedge_eur_for_1pip_move_abs',
            'dist_spot_to_strike', 'dist_spot_to_strike_pct',
        ])

        for d in all_days:
            s = spot_close.get(d)
            added = sum(adds_by_day.get(d, {}).values())
            expiring = sum(roll_by_day.get(d, {}).values())

            if s is not None and s > 0:
                g_abs_total = 0.0
                by_strike_notional: dict[float, float] = defaultdict(float)
                by_strike_nodes: dict[float, int] = defaultdict(int)
                by_strike_gabs: dict[float, float] = defaultdict(float)

                for k, notion in active.items():
                    if notion == 0:
                        continue
                    tau_days = (k.expiry - d).days
                    if tau_days < 0:
                        continue
                    t = max(tau_days / 365.0, 1.0 / 365.0)
                    g = bs_gamma(s=s, k=k.strike, t_years=t, sigma=args.vol_assumption)
                    gabs = abs(notion * g)
                    g_abs_total += gabs

                    by_strike_notional[k.strike] += notion
                    by_strike_nodes[k.strike] += 1
                    by_strike_gabs[k.strike] += gabs

                active_notional = sum(active.values())
                hedge_1pip = g_abs_total * 0.0001
                hedge_1pct = g_abs_total * (0.01 * s)

                wd.writerow([
                    d.isoformat(), f'{s:.6f}',
                    f'{added:.2f}', f'{expiring:.2f}', f'{active_notional:.2f}',
                    len(active), len(by_strike_notional),
                    f'{g_abs_total:.6f}', f'{-g_abs_total:.6f}',
                    f'{hedge_1pip:.2f}', f'{hedge_1pct:.2f}',
                ])

                for strike in sorted(by_strike_notional):
                    dist = abs(s - strike)
                    dist_pct = dist / s if s else 0.0
                    ws.writerow([
                        d.isoformat(), f'{s:.6f}', f'{strike:.6f}',
                        f'{by_strike_notional[strike]:.2f}', by_strike_nodes[strike],
                        f'{by_strike_gabs[strike]:.6f}', f'{by_strike_gabs[strike] * 0.0001:.2f}',
                        f'{dist:.6f}', f'{dist_pct:.6f}',
                    ])

            for k, v in roll_by_day.get(d, {}).items():
                active[k] -= v
                if abs(active[k]) < 1e-9:
                    del active[k]
            for k, v in adds_by_day.get(d, {}).items():
                active[k] += v

    print(f'Rows scanned: {rows_scanned}')
    print(f'Rows used (EURUSD vanilla NEWT): {rows_used}')
    print(f'Wrote {daily_out}')
    print(f'Wrote {strike_out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
