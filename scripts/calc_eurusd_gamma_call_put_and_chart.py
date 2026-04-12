#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='EURUSD gamma proxy split by call/put with Feb-2026 chart.')
    p.add_argument('--options-input', default='/home/geoffpjones/projects/dtcc/data/options_data_full.csv')
    p.add_argument('--spot-input', default='/home/geoffpjones/projects/dtcc/tick_data/eurusd_1h.csv')
    p.add_argument('--vol-assumption', type=float, default=0.10)
    p.add_argument('--include-exotics', action='store_true')
    p.add_argument('--daily-output', default='/home/geoffpjones/projects/dtcc/data/eurusd_gamma_proxy_daily_call_put.csv')
    p.add_argument('--strike-output', default='/home/geoffpjones/projects/dtcc/data/eurusd_gamma_proxy_by_strike_call_put.csv')
    p.add_argument('--levels-output', default='/home/geoffpjones/projects/dtcc/data/eurusd_feb2026_gamma_call_put_levels.csv')
    p.add_argument('--chart-output', default='/home/geoffpjones/projects/dtcc/data/eurusd_feb2026_hourly_vs_gamma_call_put_levels.png')
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


def load_spot_hourly_feb(path: Path) -> tuple[list[datetime], list[float]]:
    s = datetime.fromisoformat('2026-02-01T00:00:00+00:00')
    e = datetime.fromisoformat('2026-03-01T00:00:00+00:00')
    ts, cl = [], []
    with path.open(newline='', encoding='utf-8') as fh:
        r = csv.DictReader(fh)
        for row in r:
            t = datetime.fromisoformat(row['timestamp_utc'])
            if s <= t < e:
                ts.append(t)
                cl.append(f(row['close']))
    return ts, cl


def eur_notional(row: dict[str, str]) -> float:
    total = 0.0
    if (row.get('Notional currency-Leg 1') or '').strip().upper() == 'EUR':
        total += f(row.get('Notional amount-Leg 1') or '')
    if (row.get('Notional currency-Leg 2') or '').strip().upper() == 'EUR':
        total += f(row.get('Notional amount-Leg 2') or '')
    return total


def option_side(upi: str) -> str | None:
    if 'Call' in upi:
        return 'Call'
    if 'Put' in upi:
        return 'Put'
    return None


@dataclass(frozen=True)
class Key:
    side: str
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


def top_wavg(strikes_gamma: list[tuple[float, float]], topn: int = 10) -> float:
    if not strikes_gamma:
        return float('nan')
    sg = sorted(strikes_gamma, key=lambda x: x[1], reverse=True)[:topn]
    w = sum(g for _, g in sg)
    if w <= 0:
        return float('nan')
    return sum(k * g for k, g in sg) / w


def main() -> int:
    args = parse_args()

    options_input = Path(args.options_input)
    spot_input = Path(args.spot_input)
    daily_out = Path(args.daily_output)
    strike_out = Path(args.strike_output)
    levels_out = Path(args.levels_output)
    chart_out = Path(args.chart_output)

    daily_out.parent.mkdir(parents=True, exist_ok=True)
    strike_out.parent.mkdir(parents=True, exist_ok=True)

    spot_daily = parse_spot_daily(spot_input)

    adds_by_day: dict[date, dict[Key, float]] = defaultdict(lambda: defaultdict(float))
    roll_by_day: dict[date, dict[Key, float]] = defaultdict(lambda: defaultdict(float))

    scanned = 0
    used = 0

    with options_input.open(newline='', encoding='utf-8') as fh:
        r = csv.DictReader(fh)
        for row in r:
            scanned += 1
            upi = (row.get('UPI FISN') or '').strip()
            if 'EUR USD' not in upi:
                continue
            side = option_side(upi)
            if side is None:
                continue
            if not args.include_exotics and ' Van ' not in upi:
                continue
            if (row.get('Action type') or '').strip() != 'NEWT':
                continue

            strike = f(row.get('Strike Price') or '')
            expiry = parse_day(row.get('Expiration Date') or '')
            start = parse_day(row.get('source_date') or '') or parse_day(row.get('Event timestamp') or '')
            if strike <= 0 or expiry is None or start is None or expiry < start:
                continue

            notional = eur_notional(row)
            if notional <= 0:
                continue

            key = Key(side=side, strike=round(strike, 6), expiry=expiry)
            adds_by_day[start][key] += notional
            roll_by_day[expiry][key] += notional
            used += 1

    if not adds_by_day:
        print('No qualifying rows found.')
        return 1

    start_day = min(adds_by_day)
    end_day = max(max(roll_by_day), max(spot_daily))

    all_days = []
    d = start_day
    while d <= end_day:
        all_days.append(d)
        d = date.fromordinal(d.toordinal() + 1)

    active: dict[Key, float] = defaultdict(float)

    # for feb chart levels
    feb_levels: list[dict] = []

    with daily_out.open('w', newline='', encoding='utf-8') as fd, strike_out.open('w', newline='', encoding='utf-8') as fs:
        wd = csv.writer(fd)
        ws = csv.writer(fs)

        wd.writerow([
            'date', 'spot_close',
            'added_call_notional_eur', 'added_put_notional_eur',
            'expiring_call_notional_eur', 'expiring_put_notional_eur',
            'active_call_notional_eur', 'active_put_notional_eur', 'active_total_notional_eur',
            'call_gamma_abs_per_usd', 'put_gamma_abs_per_usd', 'total_gamma_abs_per_usd',
            'hedge_eur_for_1pip_call_abs', 'hedge_eur_for_1pip_put_abs', 'hedge_eur_for_1pip_total_abs',
        ])

        ws.writerow([
            'date', 'spot_close', 'strike',
            'active_call_notional_eur', 'active_put_notional_eur',
            'call_gamma_abs_per_usd', 'put_gamma_abs_per_usd', 'total_gamma_abs_per_usd',
            'dist_spot_to_strike', 'dist_spot_to_strike_pct',
        ])

        for d in all_days:
            s = spot_daily.get(d)

            added_call = sum(v for k, v in adds_by_day.get(d, {}).items() if k.side == 'Call')
            added_put = sum(v for k, v in adds_by_day.get(d, {}).items() if k.side == 'Put')
            exp_call = sum(v for k, v in roll_by_day.get(d, {}).items() if k.side == 'Call')
            exp_put = sum(v for k, v in roll_by_day.get(d, {}).items() if k.side == 'Put')

            if s is not None and s > 0:
                call_gamma = 0.0
                put_gamma = 0.0
                by_strike_call_n = defaultdict(float)
                by_strike_put_n = defaultdict(float)
                by_strike_call_g = defaultdict(float)
                by_strike_put_g = defaultdict(float)

                for k, notion in active.items():
                    if notion == 0:
                        continue
                    tau_days = (k.expiry - d).days
                    if tau_days < 0:
                        continue
                    t = max(tau_days / 365.0, 1.0 / 365.0)
                    g = abs(notion * bs_gamma(s=s, k=k.strike, t_years=t, sigma=args.vol_assumption))
                    if k.side == 'Call':
                        call_gamma += g
                        by_strike_call_n[k.strike] += notion
                        by_strike_call_g[k.strike] += g
                    else:
                        put_gamma += g
                        by_strike_put_n[k.strike] += notion
                        by_strike_put_g[k.strike] += g

                active_call = sum(v for k, v in active.items() if k.side == 'Call')
                active_put = sum(v for k, v in active.items() if k.side == 'Put')
                total_gamma = call_gamma + put_gamma

                wd.writerow([
                    d.isoformat(), f'{s:.6f}',
                    f'{added_call:.2f}', f'{added_put:.2f}',
                    f'{exp_call:.2f}', f'{exp_put:.2f}',
                    f'{active_call:.2f}', f'{active_put:.2f}', f'{active_call + active_put:.2f}',
                    f'{call_gamma:.6f}', f'{put_gamma:.6f}', f'{total_gamma:.6f}',
                    f'{call_gamma * 0.0001:.2f}', f'{put_gamma * 0.0001:.2f}', f'{total_gamma * 0.0001:.2f}',
                ])

                strikes = sorted(set(by_strike_call_n.keys()) | set(by_strike_put_n.keys()))
                for kstrike in strikes:
                    cg = by_strike_call_g.get(kstrike, 0.0)
                    pg = by_strike_put_g.get(kstrike, 0.0)
                    dist = abs(s - kstrike)
                    ws.writerow([
                        d.isoformat(), f'{s:.6f}', f'{kstrike:.6f}',
                        f'{by_strike_call_n.get(kstrike, 0.0):.2f}', f'{by_strike_put_n.get(kstrike, 0.0):.2f}',
                        f'{cg:.6f}', f'{pg:.6f}', f'{cg + pg:.6f}',
                        f'{dist:.6f}', f'{(dist / s):.6f}',
                    ])

                if date(2026, 2, 1) <= d <= date(2026, 2, 28):
                    call_below = [(k, g) for k, g in by_strike_call_g.items() if k < s]
                    call_above = [(k, g) for k, g in by_strike_call_g.items() if k >= s]
                    put_below = [(k, g) for k, g in by_strike_put_g.items() if k < s]
                    put_above = [(k, g) for k, g in by_strike_put_g.items() if k >= s]

                    feb_levels.append({
                        'date': d.isoformat(),
                        'spot_close': s,
                        'call_below_wavg_top10': top_wavg(call_below, 10),
                        'call_above_wavg_top10': top_wavg(call_above, 10),
                        'put_below_wavg_top10': top_wavg(put_below, 10),
                        'put_above_wavg_top10': top_wavg(put_above, 10),
                    })

            for k, v in roll_by_day.get(d, {}).items():
                active[k] -= v
                if abs(active[k]) < 1e-9:
                    del active[k]
            for k, v in adds_by_day.get(d, {}).items():
                active[k] += v

    # write feb levels
    with levels_out.open('w', newline='', encoding='utf-8') as flev:
        fields = ['date', 'spot_close', 'call_below_wavg_top10', 'call_above_wavg_top10', 'put_below_wavg_top10', 'put_above_wavg_top10']
        w = csv.DictWriter(flev, fieldnames=fields)
        w.writeheader()
        for r in feb_levels:
            w.writerow(r)

    # plot feb chart
    h_ts, h_close = load_spot_hourly_feb(spot_input)
    x = [datetime.fromisoformat(r['date'] + 'T00:00:00+00:00') for r in feb_levels]
    cb = [r['call_below_wavg_top10'] for r in feb_levels]
    ca = [r['call_above_wavg_top10'] for r in feb_levels]
    pb = [r['put_below_wavg_top10'] for r in feb_levels]
    pa = [r['put_above_wavg_top10'] for r in feb_levels]

    fig, ax = plt.subplots(figsize=(15, 7), constrained_layout=True)
    ax.plot(h_ts, h_close, color='#1f77b4', lw=1.1, label='EURUSD Hourly Close')
    ax.step(x, cb, where='post', color='#2ca02c', lw=2.0, label='Call-Below Level (top10 wavg)')
    ax.step(x, ca, where='post', color='#d62728', lw=2.0, label='Call-Above Level (top10 wavg)')
    ax.step(x, pb, where='post', color='#17becf', lw=2.0, label='Put-Below Level (top10 wavg)')
    ax.step(x, pa, where='post', color='#ff7f0e', lw=2.0, label='Put-Above Level (top10 wavg)')

    ax.set_title('EURUSD Hourly Spot vs Call/Put Gamma Levels - Feb 2026 (UTC)')
    ax.set_ylabel('EURUSD')
    ax.set_xlim(datetime.fromisoformat('2026-02-01T00:00:00+00:00'), datetime.fromisoformat('2026-03-01T00:00:00+00:00'))
    ax.grid(True, alpha=0.25)
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.setp(ax.get_xticklabels(), rotation=45, ha='right')
    ax.legend(loc='upper left', ncol=2)
    fig.savefig(chart_out, dpi=170)

    print(f'Rows scanned: {scanned}')
    print(f'Rows used (EURUSD NEWT, side-split): {used}')
    print(f'Wrote {daily_out}')
    print(f'Wrote {strike_out}')
    print(f'Wrote {levels_out}')
    print(f'Wrote {chart_out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
