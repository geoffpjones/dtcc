#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path


@dataclass
class HourBar:
    ts: datetime
    o: float
    h: float
    l: float
    c: float


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Walk-forward EURUSD gamma-limit strategy (no lookahead).')
    p.add_argument('--strike-gamma', default='/home/geoffpjones/projects/dtcc/data/eurusd_gamma_proxy_by_strike_call_put.csv')
    p.add_argument('--hourly', default='/home/geoffpjones/projects/dtcc/tick_data/eurusd_1h.csv')
    p.add_argument('--start-date', default='2025-11-11')
    p.add_argument('--end-date', default='2026-03-31')
    p.add_argument('--topn', type=int, default=10)
    p.add_argument('--weight-mode', choices=['gamma', 'notional'], default='gamma')
    p.add_argument('--level-mode', choices=['weighted', 'nearest'], default='weighted')
    p.add_argument('--max-distance-pips', type=float, default=0.0, help='0 disables the filter')
    p.add_argument('--distance-decay-power', type=float, default=0.0, help='0 disables decay; otherwise weight /= distance^power')
    p.add_argument('--pip-size', type=float, default=0.0001, help='Price units per pip')
    p.add_argument('--output', default='/home/geoffpjones/projects/dtcc/data/eurusd_walkforward_gamma_limits_2025-11-11_to_2026-03-31.csv')
    return p.parse_args()


def f(x: str) -> float:
    t = (x or '').strip()
    if t == '':
        return float('nan')
    try:
        return float(t)
    except Exception:
        return float('nan')


def load_hourly(path: Path) -> dict[date, list[HourBar]]:
    grouped: dict[date, list[HourBar]] = defaultdict(list)
    with path.open(newline='', encoding='utf-8') as fh:
        r = csv.DictReader(fh)
        for row in r:
            ts = datetime.fromisoformat(row['timestamp_utc'])
            d = ts.date()
            grouped[d].append(HourBar(ts=ts, o=f(row['open']), h=f(row['high']), l=f(row['low']), c=f(row['close'])))
    for d in grouped:
        grouped[d].sort(key=lambda x: x.ts)
    return grouped


def load_strike_gamma(path: Path) -> dict[date, list[dict[str, float]]]:
    # date -> list[{strike, active_call_notional, active_put_notional, call_gamma_abs, put_gamma_abs}]
    out: dict[date, list[dict[str, float]]] = defaultdict(list)
    with path.open(newline='', encoding='utf-8') as fh:
        r = csv.DictReader(fh)
        for row in r:
            d = date.fromisoformat(row['date'])
            strike = f(row['strike'])
            acn = f(row['active_call_notional'])
            apn = f(row['active_put_notional'])
            cg = f(row['call_gamma_abs_per_usd'])
            pg = f(row['put_gamma_abs_per_usd'])
            if math.isnan(strike):
                continue
            out[d].append({
                'strike': strike,
                'active_call_notional': 0.0 if math.isnan(acn) else acn,
                'active_put_notional': 0.0 if math.isnan(apn) else apn,
                'call_gamma_abs_per_usd': 0.0 if math.isnan(cg) else cg,
                'put_gamma_abs_per_usd': 0.0 if math.isnan(pg) else pg,
            })
    return out


def select_weights(row: dict[str, float], weight_mode: str) -> tuple[float, float]:
    if weight_mode == 'notional':
        return row['active_call_notional'], row['active_put_notional']
    return row['call_gamma_abs_per_usd'], row['put_gamma_abs_per_usd']


def adjust_weight(raw_weight: float, distance_price: float, pip_size: float, decay_power: float) -> float:
    if raw_weight <= 0:
        return 0.0
    if decay_power <= 0:
        return raw_weight
    distance_pips = max(abs(distance_price) / pip_size, 1.0)
    return raw_weight / (distance_pips ** decay_power)


def select_level(levels: list[tuple[float, float, float]], topn: int, level_mode: str) -> float:
    # levels: [(price, weight)]
    clean = [(p, w, abs(d)) for p, w, d in levels if not (math.isnan(p) or math.isnan(w)) and w > 0]
    if not clean:
        return float('nan')
    top = sorted(clean, key=lambda x: x[1], reverse=True)[:topn]
    if level_mode == 'nearest':
        return min(top, key=lambda x: x[2])[0]
    wsum = sum(w for _, w, _ in top)
    if wsum <= 0:
        return float('nan')
    return sum(p * w for p, w, _ in top) / wsum


def fmt(x) -> str:
    if isinstance(x, float):
        if math.isnan(x):
            return ''
        return f'{x:.10f}'.rstrip('0').rstrip('.')
    return str(x)


def main() -> int:
    args = parse_args()
    hourly = load_hourly(Path(args.hourly))
    strike_gamma = load_strike_gamma(Path(args.strike_gamma))

    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)

    # Reference price assumption:
    # daily limits are anchored to prior session close to avoid same-day lookahead.
    days_sorted = sorted(hourly.keys())
    eod_close = {d: hourly[d][-1].c for d in days_sorted if hourly[d]}
    prev_close = {}
    prev = None
    for d in days_sorted:
        prev_close[d] = prev
        if d in eod_close:
            prev = eod_close[d]

    rows = []
    d = start
    while d <= end:
        bars = hourly.get(d, [])
        sg = strike_gamma.get(d, [])
        ref = prev_close.get(d)

        if not bars or not sg or ref is None or math.isnan(ref):
            rows.append({
                'date': d.isoformat(),
                'ref_price_prev_close': ref if ref is not None else float('nan'),
                'buy_limit': float('nan'),
                'sell_limit': float('nan'),
                'eod_close': float('nan'),
                'buy_filled': 0,
                'sell_filled': 0,
                'buy_fill_time_utc': '',
                'sell_fill_time_utc': '',
                'buy_pnl_pips': 0.0,
                'sell_pnl_pips': 0.0,
                'net_pnl_pips': 0.0,
                'notes': (
                    f'missing_hourly_or_strike_gamma_or_ref'
                    f'|weight_mode={args.weight_mode}'
                    f'|level_mode={args.level_mode}'
                    f'|max_distance_pips={args.max_distance_pips:g}'
                    f'|distance_decay_power={args.distance_decay_power:g}'
                ),
            })
            d = date.fromordinal(d.toordinal() + 1)
            continue

        # Signal mapping:
        # - call strikes below reference => buy support candidates
        # - put strikes above reference => sell resistance candidates
        # Weighting can use gamma magnitude or active notional concentration.
        # We optionally force levels to stay closer to the market by filtering distant
        # strikes and/or decaying their contribution by distance from the prior close.
        call_below = []
        put_above = []
        for row in sg:
            strike = row['strike']
            call_weight, put_weight = select_weights(row, args.weight_mode)
            signed_distance = strike - ref
            distance_pips = abs(signed_distance) / args.pip_size
            if args.max_distance_pips > 0 and distance_pips > args.max_distance_pips:
                continue
            if strike < ref and call_weight > 0:
                adj = adjust_weight(call_weight, signed_distance, args.pip_size, args.distance_decay_power)
                call_below.append((strike, adj, signed_distance))
            if strike > ref and put_weight > 0:
                adj = adjust_weight(put_weight, signed_distance, args.pip_size, args.distance_decay_power)
                put_above.append((strike, adj, signed_distance))

        buy = select_level(call_below, args.topn, args.level_mode)
        sell = select_level(put_above, args.topn, args.level_mode)

        eod = bars[-1].c

        buy_filled = 0
        sell_filled = 0
        buy_fill_ts = ''
        sell_fill_ts = ''

        for b in bars:
            # Fill model assumption: a limit is filled if price range crosses the level.
            if (not math.isnan(buy)) and (buy_filled == 0) and b.l <= buy <= b.h:
                buy_filled = 1
                buy_fill_ts = b.ts.isoformat()
            if (not math.isnan(sell)) and (sell_filled == 0) and b.l <= sell <= b.h:
                sell_filled = 1
                sell_fill_ts = b.ts.isoformat()

        buy_pnl = ((eod - buy) * 10000.0) if buy_filled else 0.0
        sell_pnl = ((sell - eod) * 10000.0) if sell_filled else 0.0
        net = buy_pnl + sell_pnl

        note = ''
        if buy_filled and sell_filled:
            note = 'both_filled_intraday_order_ambiguous_with_1h_bars'
        tag = (
            f'weight_mode={args.weight_mode}'
            f'|level_mode={args.level_mode}'
            f'|max_distance_pips={args.max_distance_pips:g}'
            f'|distance_decay_power={args.distance_decay_power:g}'
        )
        note = f'{note}|{tag}' if note else tag

        rows.append({
            'date': d.isoformat(),
            'ref_price_prev_close': ref,
            'buy_limit': buy,
            'sell_limit': sell,
            'eod_close': eod,
            'buy_filled': buy_filled,
            'sell_filled': sell_filled,
            'buy_fill_time_utc': buy_fill_ts,
            'sell_fill_time_utc': sell_fill_ts,
            'buy_pnl_pips': buy_pnl,
            'sell_pnl_pips': sell_pnl,
            'net_pnl_pips': net,
            'notes': note,
        })

        d = date.fromordinal(d.toordinal() + 1)

    # cum + month
    cum = 0.0
    monthly = defaultdict(float)
    for r in rows:
        cum += r['net_pnl_pips']
        r['cum_net_pnl_pips'] = cum
        ym = r['date'][:7]
        r['year_month'] = ym
        monthly[ym] += r['net_pnl_pips']

    out_path = Path(args.output)
    if out_path.parent != Path(''):
        out_path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        'date', 'year_month', 'ref_price_prev_close',
        'buy_limit', 'sell_limit', 'eod_close',
        'buy_filled', 'sell_filled', 'buy_fill_time_utc', 'sell_fill_time_utc',
        'buy_pnl_pips', 'sell_pnl_pips', 'net_pnl_pips', 'cum_net_pnl_pips', 'notes',
    ]
    with out_path.open('w', newline='', encoding='utf-8') as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: fmt(r[k]) for k in fields})

    month_path = out_path.with_name(f'{out_path.stem}_monthly_summary.csv')
    with month_path.open('w', newline='', encoding='utf-8') as fh:
        w = csv.writer(fh)
        w.writerow(['year_month', 'net_pnl_pips'])
        for ym in sorted(monthly):
            w.writerow([ym, f'{monthly[ym]:.6f}'])

    traded = [r for r in rows if r['buy_filled'] or r['sell_filled']]
    both = [r for r in rows if r['buy_filled'] and r['sell_filled']]
    buy_only = [r for r in rows if r['buy_filled'] and not r['sell_filled']]
    sell_only = [r for r in rows if r['sell_filled'] and not r['buy_filled']]

    total_buy = sum(r['buy_pnl_pips'] for r in rows)
    total_sell = sum(r['sell_pnl_pips'] for r in rows)
    total_net = sum(r['net_pnl_pips'] for r in rows)

    print(f'Wrote {out_path}')
    print(f'Wrote {month_path}')
    print(f'Days: {len(rows)}')
    print(f'Traded days: {len(traded)} | buy_only={len(buy_only)} sell_only={len(sell_only)} both={len(both)}')
    print(f'Total buy pnl (pips): {total_buy:.2f}')
    print(f'Total sell pnl (pips): {total_sell:.2f}')
    print(f'Total net pnl (pips): {total_net:.2f}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
