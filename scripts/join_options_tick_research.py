#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean

SPOT_1H = Path('/home/geoffpjones/projects/dtcc/tick_data/eurusd_1h.csv')
OPT_DAILY = Path('/home/geoffpjones/projects/dtcc/data/eurusd_options_daily_summary.csv')
OPT_STRIKE = Path('/home/geoffpjones/projects/dtcc/data/eurusd_options_strike_map.csv')

OUT_DAILY = Path('/home/geoffpjones/projects/dtcc/data/eurusd_options_spot_daily_joined.csv')
OUT_HOURLY = Path('/home/geoffpjones/projects/dtcc/data/eurusd_options_spot_hourly_strike_proximity.csv')
OUT_REPORT = Path('/home/geoffpjones/projects/dtcc/data/eurusd_options_spot_signal_report.txt')


def to_float(v: str) -> float:
    try:
        return float((v or '').strip())
    except Exception:
        return float('nan')


def parse_iso_dt(v: str) -> datetime:
    return datetime.fromisoformat(v)


def correlation(xs: list[float], ys: list[float]) -> float:
    pairs = [(x, y) for x, y in zip(xs, ys) if not (math.isnan(x) or math.isnan(y))]
    if len(pairs) < 3:
        return float('nan')
    xv = [p[0] for p in pairs]
    yv = [p[1] for p in pairs]
    mx, my = mean(xv), mean(yv)
    num = sum((x - mx) * (y - my) for x, y in pairs)
    denx = math.sqrt(sum((x - mx) ** 2 for x in xv))
    deny = math.sqrt(sum((y - my) ** 2 for y in yv))
    if denx == 0 or deny == 0:
        return float('nan')
    return num / (denx * deny)


def load_spot_daily() -> tuple[dict[str, dict], list[dict]]:
    by_date: dict[str, dict] = {}
    hourly_rows: list[dict] = []

    with SPOT_1H.open(newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        grouped: dict[str, list[dict]] = defaultdict(list)
        for row in r:
            ts = parse_iso_dt(row['timestamp_utc'])
            d = ts.date().isoformat()
            row['_date'] = d
            row['_ts'] = ts
            grouped[d].append(row)
            hourly_rows.append(row)

    for d, rows in grouped.items():
        rows.sort(key=lambda x: x['_ts'])
        opens = [to_float(x['open']) for x in rows]
        highs = [to_float(x['high']) for x in rows]
        lows = [to_float(x['low']) for x in rows]
        closes = [to_float(x['close']) for x in rows]
        by_date[d] = {
            'spot_open': opens[0],
            'spot_high': max(highs),
            'spot_low': min(lows),
            'spot_close': closes[-1],
            'spot_minutes': sum(int(float(x['minutes_in_hour'])) for x in rows),
            'spot_volume_sum': sum(to_float(x['volume_sum']) for x in rows),
        }

    dates = sorted(by_date.keys())
    prev_close = None
    for d in dates:
        c = by_date[d]['spot_close']
        by_date[d]['spot_return_1d'] = float('nan') if prev_close in (None, 0) else (c / prev_close - 1.0)
        prev_close = c

    for i, d in enumerate(dates[:-1]):
        nd = dates[i + 1]
        by_date[d]['spot_return_next_1d'] = by_date[nd]['spot_return_1d']
        by_date[d]['spot_abs_return_next_1d'] = abs(by_date[nd]['spot_return_1d']) if not math.isnan(by_date[nd]['spot_return_1d']) else float('nan')
    by_date[dates[-1]]['spot_return_next_1d'] = float('nan')
    by_date[dates[-1]]['spot_abs_return_next_1d'] = float('nan')

    return by_date, hourly_rows


def load_options_daily() -> dict[str, dict]:
    out: dict[str, dict] = {}
    with OPT_DAILY.open(newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        for row in r:
            d = row['date']
            call_notional = to_float(row['call_notional_eur'])
            put_notional = to_float(row['put_notional_eur'])
            total_notional = to_float(row['total_notional_eur'])
            out[d] = {
                'call_trade_count': int(float(row['call_trade_count'])),
                'put_trade_count': int(float(row['put_trade_count'])),
                'total_trade_count': int(float(row['total_trade_count'])),
                'call_notional_eur': call_notional,
                'put_notional_eur': put_notional,
                'total_notional_eur': total_notional,
                'put_call_notional_ratio': float('nan') if call_notional == 0 else put_notional / call_notional,
                'put_call_trade_ratio': float('nan') if int(float(row['call_trade_count'])) == 0 else int(float(row['put_trade_count'])) / int(float(row['call_trade_count'])),
                'total_distinct_strikes': int(float(row['total_distinct_strikes'])),
            }
    return out


def load_strike_features() -> dict[str, dict]:
    per_date_side: dict[str, dict[str, list[tuple[float, float, int]]]] = defaultdict(lambda: {'Call': [], 'Put': []})
    with OPT_STRIKE.open(newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        for row in r:
            d = row['date']
            side = row['option_side']
            strike_raw = row['strike_price']
            if strike_raw == '(blank)':
                continue
            strike = to_float(strike_raw)
            if math.isnan(strike):
                continue
            notional = to_float(row['notional_eur'])
            tc = int(float(row['trade_count']))
            per_date_side[d][side].append((strike, notional, tc))

    features: dict[str, dict] = {}
    for d, sides in per_date_side.items():
        calls = sides['Call']
        puts = sides['Put']
        all_rows = calls + puts
        if not all_rows:
            continue

        def top(rows: list[tuple[float, float, int]]):
            if not rows:
                return (float('nan'), 0.0, 0)
            s, n, t = max(rows, key=lambda x: x[1])
            return (s, n, t)

        c_s, c_n, c_t = top(calls)
        p_s, p_n, p_t = top(puts)
        a_s, a_n, a_t = top(all_rows)
        total_n = sum(x[1] for x in all_rows)

        wavg_num = sum(s * n for s, n, _ in all_rows)
        wavg = float('nan') if total_n == 0 else wavg_num / total_n

        features[d] = {
            'top_call_strike': c_s,
            'top_call_notional': c_n,
            'top_put_strike': p_s,
            'top_put_notional': p_n,
            'top_any_strike': a_s,
            'top_any_notional': a_n,
            'top_any_notional_share': float('nan') if total_n == 0 else a_n / total_n,
            'weighted_avg_strike': wavg,
            'strike_levels_count_nonblank': len(all_rows),
        }

    return features


def add_option_deltas(rows: list[dict]) -> None:
    rows.sort(key=lambda x: x['date'])
    prev = None
    for r in rows:
        if prev is None:
            r['delta_total_notional_eur'] = float('nan')
            r['delta_total_trade_count'] = float('nan')
        else:
            r['delta_total_notional_eur'] = r['total_notional_eur'] - prev['total_notional_eur']
            r['delta_total_trade_count'] = r['total_trade_count'] - prev['total_trade_count']
        prev = r


def fmt(v) -> str:
    if isinstance(v, float):
        if math.isnan(v):
            return ''
        return f'{v:.10f}'.rstrip('0').rstrip('.')
    return str(v)


def main() -> int:
    spot_daily, spot_hourly = load_spot_daily()
    opt_daily = load_options_daily()
    strike_feat = load_strike_features()

    joined: list[dict] = []
    for d in sorted(set(opt_daily.keys()) & set(spot_daily.keys())):
        r = {'date': d}
        r.update(opt_daily[d])
        r.update(spot_daily[d])
        r.update(strike_feat.get(d, {
            'top_call_strike': float('nan'),
            'top_call_notional': float('nan'),
            'top_put_strike': float('nan'),
            'top_put_notional': float('nan'),
            'top_any_strike': float('nan'),
            'top_any_notional': float('nan'),
            'top_any_notional_share': float('nan'),
            'weighted_avg_strike': float('nan'),
            'strike_levels_count_nonblank': 0,
        }))

        close = r['spot_close']
        r['dist_close_to_top_any_strike'] = float('nan') if math.isnan(r['top_any_strike']) else abs(close - r['top_any_strike'])
        r['dist_close_to_wavg_strike'] = float('nan') if math.isnan(r['weighted_avg_strike']) else abs(close - r['weighted_avg_strike'])

        joined.append(r)

    add_option_deltas(joined)

    OUT_DAILY.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        'date',
        'call_trade_count', 'put_trade_count', 'total_trade_count',
        'call_notional_eur', 'put_notional_eur', 'total_notional_eur',
        'put_call_notional_ratio', 'put_call_trade_ratio',
        'delta_total_notional_eur', 'delta_total_trade_count',
        'total_distinct_strikes', 'strike_levels_count_nonblank',
        'top_call_strike', 'top_call_notional', 'top_put_strike', 'top_put_notional',
        'top_any_strike', 'top_any_notional', 'top_any_notional_share', 'weighted_avg_strike',
        'spot_open', 'spot_high', 'spot_low', 'spot_close', 'spot_minutes', 'spot_volume_sum',
        'spot_return_1d', 'spot_return_next_1d', 'spot_abs_return_next_1d',
        'dist_close_to_top_any_strike', 'dist_close_to_wavg_strike',
    ]
    with OUT_DAILY.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in joined:
            w.writerow({k: fmt(r.get(k, '')) for k in fields})

    # Hourly proximity map to daily key strikes
    strike_lookup = {r['date']: r for r in joined}
    with OUT_HOURLY.open('w', newline='', encoding='utf-8') as f:
        fields_h = [
            'timestamp_utc', 'date', 'hour_close',
            'top_call_strike', 'top_put_strike', 'top_any_strike',
            'dist_to_top_call', 'dist_to_top_put', 'dist_to_top_any',
            'next_hour_return',
        ]
        w = csv.DictWriter(f, fieldnames=fields_h)
        w.writeheader()

        spot_hourly.sort(key=lambda x: x['_ts'])
        for i, row in enumerate(spot_hourly):
            d = row['_date']
            if d not in strike_lookup:
                continue
            cur_close = to_float(row['close'])
            nxt_ret = float('nan')
            if i + 1 < len(spot_hourly):
                nxt_close = to_float(spot_hourly[i + 1]['close'])
                if cur_close != 0:
                    nxt_ret = nxt_close / cur_close - 1.0

            sf = strike_lookup[d]
            top_call = sf['top_call_strike']
            top_put = sf['top_put_strike']
            top_any = sf['top_any_strike']
            out = {
                'timestamp_utc': row['timestamp_utc'],
                'date': d,
                'hour_close': cur_close,
                'top_call_strike': top_call,
                'top_put_strike': top_put,
                'top_any_strike': top_any,
                'dist_to_top_call': float('nan') if math.isnan(top_call) else abs(cur_close - top_call),
                'dist_to_top_put': float('nan') if math.isnan(top_put) else abs(cur_close - top_put),
                'dist_to_top_any': float('nan') if math.isnan(top_any) else abs(cur_close - top_any),
                'next_hour_return': nxt_ret,
            }
            w.writerow({k: fmt(out[k]) for k in fields_h})

    # Basic signal report
    xs1 = [r['delta_total_notional_eur'] for r in joined]
    ys_signed = [r['spot_return_next_1d'] for r in joined]
    ys_abs = [r['spot_abs_return_next_1d'] for r in joined]
    xs_ratio = [r['put_call_notional_ratio'] for r in joined]
    xs_share = [r['top_any_notional_share'] for r in joined]
    xs_dist = [r['dist_close_to_top_any_strike'] for r in joined]

    c1 = correlation(xs1, ys_signed)
    c2 = correlation(xs1, ys_abs)
    c3 = correlation(xs_ratio, ys_signed)
    c4 = correlation(xs_share, ys_abs)
    c5 = correlation(xs_dist, ys_abs)

    with OUT_REPORT.open('w', encoding='utf-8') as f:
        f.write('EURUSD Options + Spot Exploratory Signal Report\n')
        f.write('===========================================\n\n')
        f.write(f'Daily joined rows: {len(joined)}\n')
        f.write(f'Date range: {joined[0]["date"]} to {joined[-1]["date"]}\n\n')
        f.write('Hypothesis 1: changes in options interest predict spot move\n')
        f.write(f'corr(delta_total_notional_eur, next_day_return) = {c1:.6f}\n')
        f.write(f'corr(delta_total_notional_eur, abs(next_day_return)) = {c2:.6f}\n')
        f.write(f'corr(put_call_notional_ratio, next_day_return) = {c3:.6f}\n\n')
        f.write('Hypothesis 2: large strikes/expiries create predictable moves\n')
        f.write(f'corr(top_any_notional_share, abs(next_day_return)) = {c4:.6f}\n')
        f.write(f'corr(distance(close, top_any_strike), abs(next_day_return)) = {c5:.6f}\n')

    print(f'Wrote {OUT_DAILY}')
    print(f'Wrote {OUT_HOURLY}')
    print(f'Wrote {OUT_REPORT}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
