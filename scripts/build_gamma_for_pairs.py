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
    p = argparse.ArgumentParser(description='Build aligned strike gamma proxy files for multiple FX pairs.')
    p.add_argument('--pairs', default='AUDUSD,GBPUSD,USDCAD,USDJPY')
    p.add_argument('--options-input', default='/home/geoffpjones/projects/dtcc/data/options_data_full.csv')
    p.add_argument('--tick-dir', default='/home/geoffpjones/projects/dtcc/tick_data')
    p.add_argument('--out-dir', default='/home/geoffpjones/projects/dtcc/data')
    p.add_argument('--vol-assumption', type=float, default=0.10)
    p.add_argument('--include-exotics', action='store_true')
    return p.parse_args()


def to_float(v: str) -> float:
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


def option_side(upi: str) -> str | None:
    if 'Call' in upi:
        return 'Call'
    if 'Put' in upi:
        return 'Put'
    return None


def is_supported_vanilla_upi(upi: str) -> bool:
    return upi.upper().startswith('NA/O VAN ')


def eur_like_notional(row: dict[str, str], ccy_a: str, ccy_b: str) -> float:
    # Exposure proxy assumption:
    # We approximate option size as the sum of leg notionals denominated in either pair currency.
    # This is a practical signal proxy, not a full valuation-normalized Greeks engine.
    total = 0.0
    c1 = (row.get('Notional currency-Leg 1') or '').strip().upper()
    c2 = (row.get('Notional currency-Leg 2') or '').strip().upper()
    if c1 in (ccy_a, ccy_b):
        total += to_float(row.get('Notional amount-Leg 1') or '')
    if c2 in (ccy_a, ccy_b):
        total += to_float(row.get('Notional amount-Leg 2') or '')
    return total


def norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def bs_gamma(s: float, k: float, t_years: float, sigma: float) -> float:
    if s <= 0 or k <= 0 or t_years <= 0 or sigma <= 0:
        return 0.0
    vol_sqrt_t = sigma * math.sqrt(t_years)
    if vol_sqrt_t <= 0:
        return 0.0
    # Standard Black-Scholes gamma with r=0 assumption.
    d1 = (math.log(s / k) + 0.5 * sigma * sigma * t_years) / vol_sqrt_t
    return norm_pdf(d1) / (s * vol_sqrt_t)


def parse_spot_daily(path: Path) -> dict[date, float]:
    out = {}
    with path.open(newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        last = {}
        for row in r:
            ts = datetime.fromisoformat(row['timestamp_utc'])
            d = ts.date()
            c = float(row['close'])
            prev = last.get(d)
            if prev is None or ts > prev[0]:
                last[d] = (ts, c)
    for d, (_, c) in last.items():
        out[d] = c
    return out


@dataclass(frozen=True)
class Key:
    side: str
    strike: float
    expiry: date


def main() -> int:
    args = parse_args()

    pairs = [p.strip().upper() for p in args.pairs.split(',') if p.strip()]
    pair_tokens = {p: [f'{p[:3]} {p[3:]}', f'{p[3:]} {p[:3]}'] for p in pairs}

    tick_dir = Path(args.tick_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    spot_daily = {}
    for p in pairs:
        hp = tick_dir / f'{p.lower()}_1h.csv'
        if not hp.exists():
            print(f'skip {p}: missing hourly {hp}')
            continue
        spot_daily[p] = parse_spot_daily(hp)

    live_pairs = sorted(spot_daily.keys())
    if not live_pairs:
        print('No pairs with hourly data found.')
        return 1

    adds_by_day = {p: defaultdict(lambda: defaultdict(float)) for p in live_pairs}
    roll_by_day = {p: defaultdict(lambda: defaultdict(float)) for p in live_pairs}

    scanned = 0
    used = {p: 0 for p in live_pairs}

    with Path(args.options_input).open(newline='', encoding='utf-8') as fh:
        r = csv.DictReader(fh)
        for row in r:
            scanned += 1
            upi = (row.get('UPI FISN') or '').strip()
            if (row.get('Action type') or '').strip() != 'NEWT':
                continue
            # Vanilla gamma is intentionally restricted to the NA/O Van* UPI family.
            # Digitals and other exotic structures need different greek treatment and
            # should not be mixed into the plain-vanilla strike gamma proxy.
            if not args.include_exotics and not is_supported_vanilla_upi(upi):
                continue

            side = option_side(upi)
            if side is None:
                continue

            match_pair = None
            for p in live_pairs:
                if any(tok in upi for tok in pair_tokens[p]):
                    match_pair = p
                    break
            if match_pair is None:
                continue

            strike = to_float(row.get('Strike Price') or '')
            expiry = parse_day(row.get('Expiration Date') or '')
            start = parse_day(row.get('source_date') or '') or parse_day(row.get('Event timestamp') or '')
            if strike <= 0 or expiry is None or start is None or expiry < start:
                continue

            ccy_a, ccy_b = match_pair[:3], match_pair[3:]
            notion = eur_like_notional(row, ccy_a, ccy_b)
            if notion <= 0:
                continue

            k = Key(side=side, strike=round(strike, 6), expiry=expiry)
            adds_by_day[match_pair][start][k] += notion
            roll_by_day[match_pair][expiry][k] += notion
            used[match_pair] += 1

    print('rows_scanned', scanned)
    for p in live_pairs:
        print('rows_used', p, used[p])

    for p in live_pairs:
        sp = spot_daily[p]
        if not sp:
            continue
        start_day = min(min(adds_by_day[p]) if adds_by_day[p] else min(sp), min(sp))
        end_day = max(max(roll_by_day[p]) if roll_by_day[p] else max(sp), max(sp))

        all_days = []
        d = start_day
        while d <= end_day:
            all_days.append(d)
            d = date.fromordinal(d.toordinal() + 1)

        active = defaultdict(float)

        out_daily = out_dir / f'{p.lower()}_gamma_proxy_daily_call_put.csv'
        out_strike = out_dir / f'{p.lower()}_gamma_proxy_by_strike_call_put.csv'

        with out_daily.open('w', newline='', encoding='utf-8') as fd, out_strike.open('w', newline='', encoding='utf-8') as fs:
            wd = csv.writer(fd)
            ws = csv.writer(fs)

            wd.writerow([
                'date', 'spot_close',
                'added_call_notional', 'added_put_notional',
                'expiring_call_notional', 'expiring_put_notional',
                'active_call_notional', 'active_put_notional', 'active_total_notional',
                'call_gamma_abs_per_usd', 'put_gamma_abs_per_usd', 'total_gamma_abs_per_usd',
            ])
            ws.writerow([
                'date', 'spot_close', 'strike',
                'active_call_notional', 'active_put_notional',
                'call_gamma_abs_per_usd', 'put_gamma_abs_per_usd', 'total_gamma_abs_per_usd',
                'dist_spot_to_strike', 'dist_spot_to_strike_pct',
            ])

            for d in all_days:
                s = sp.get(d)
                add_call = sum(v for k, v in adds_by_day[p].get(d, {}).items() if k.side == 'Call')
                add_put = sum(v for k, v in adds_by_day[p].get(d, {}).items() if k.side == 'Put')
                exp_call = sum(v for k, v in roll_by_day[p].get(d, {}).items() if k.side == 'Call')
                exp_put = sum(v for k, v in roll_by_day[p].get(d, {}).items() if k.side == 'Put')

                if s is not None and s > 0:
                    call_gamma = 0.0
                    put_gamma = 0.0
                    by_call_n = defaultdict(float)
                    by_put_n = defaultdict(float)
                    by_call_g = defaultdict(float)
                    by_put_g = defaultdict(float)

                    for k, notion in active.items():
                        if notion == 0:
                            continue
                        tau_days = (k.expiry - d).days
                        if tau_days < 0:
                            continue
                        # Minimum tenor floor avoids exploding gamma exactly at expiry.
                        t = max(tau_days / 365.0, 1.0 / 365.0)
                        g = abs(notion * bs_gamma(s=s, k=k.strike, t_years=t, sigma=args.vol_assumption))
                        if k.side == 'Call':
                            call_gamma += g
                            by_call_n[k.strike] += notion
                            by_call_g[k.strike] += g
                        else:
                            put_gamma += g
                            by_put_n[k.strike] += notion
                            by_put_g[k.strike] += g

                    act_call = sum(v for k, v in active.items() if k.side == 'Call')
                    act_put = sum(v for k, v in active.items() if k.side == 'Put')

                    wd.writerow([
                        d.isoformat(), f'{s:.6f}',
                        f'{add_call:.2f}', f'{add_put:.2f}',
                        f'{exp_call:.2f}', f'{exp_put:.2f}',
                        f'{act_call:.2f}', f'{act_put:.2f}', f'{act_call + act_put:.2f}',
                        f'{call_gamma:.6f}', f'{put_gamma:.6f}', f'{call_gamma + put_gamma:.6f}',
                    ])

                    strikes = sorted(set(by_call_n.keys()) | set(by_put_n.keys()))
                    for ks in strikes:
                        cg = by_call_g.get(ks, 0.0)
                        pg = by_put_g.get(ks, 0.0)
                        dist = abs(s - ks)
                        ws.writerow([
                            d.isoformat(), f'{s:.6f}', f'{ks:.6f}',
                            f'{by_call_n.get(ks, 0.0):.2f}', f'{by_put_n.get(ks, 0.0):.2f}',
                            f'{cg:.6f}', f'{pg:.6f}', f'{cg + pg:.6f}',
                            f'{dist:.6f}', f'{(dist / s):.6f}',
                        ])

                # Lookahead control:
                # day-T output uses start-of-day inventory; expiries and new trades are applied
                # after computation so they impact T+1.
                for k, v in roll_by_day[p].get(d, {}).items():
                    active[k] -= v
                    if abs(active[k]) < 1e-9:
                        del active[k]
                for k, v in adds_by_day[p].get(d, {}).items():
                    active[k] += v

        print('wrote', out_daily)
        print('wrote', out_strike)

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
