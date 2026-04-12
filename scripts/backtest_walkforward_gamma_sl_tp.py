#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

@dataclass
class Bar:
    ts: datetime
    high: float
    low: float
    close: float


@dataclass
class Trade:
    signal_date: str
    side: Literal['long', 'short']
    entry_ts: datetime
    entry_price: float


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Backtest SL/TP exits for walk-forward gamma-limit entries.')
    p.add_argument('--signals', default='/home/geoffpjones/projects/dtcc/data/eurusd_walkforward_gamma_limits_2025-11-11_to_2026-03-31.csv')
    p.add_argument('--hourly', default='/home/geoffpjones/projects/dtcc/tick_data/eurusd_1h.csv')
    p.add_argument('--tp-pips', type=float, default=20.0)
    p.add_argument('--sl-pips', type=float, default=20.0)
    p.add_argument('--pip-size', type=float, default=0.0001, help='Price units per pip (e.g., 0.0001, 0.01 for JPY pairs)')
    p.add_argument(
        '--tp-mode',
        choices=['fixed', 'trail_after_tp'],
        default='fixed',
        help='fixed: exit at TP; trail_after_tp: TP activates trailing stop mode',
    )
    p.add_argument(
        '--trail-pips',
        type=float,
        default=10.0,
        help='Trailing stop distance in pips once TP has been activated',
    )
    p.add_argument('--max-hold-hours', type=int, default=72, help='Hours after entry before forced close at bar close')
    p.add_argument(
        '--max-hold-mode',
        choices=['hours', 'eod_ny'],
        default='hours',
        help='hours: use --max-hold-hours; eod_ny: force close at next 5pm New York time',
    )
    p.add_argument('--intrabar-policy', choices=['stop', 'target'], default='stop', help='If TP and SL are both hit in same bar')
    p.add_argument('--output-trades', default='/home/geoffpjones/projects/dtcc/data/eurusd_walkforward_gamma_sl_tp_trades.csv')
    p.add_argument('--output-daily', default='/home/geoffpjones/projects/dtcc/data/eurusd_walkforward_gamma_sl_tp_daily_summary.csv')
    return p.parse_args()


def load_bars(path: Path) -> list[Bar]:
    bars: list[Bar] = []
    with path.open(newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        for row in r:
            bars.append(
                Bar(
                    ts=_parse_ts_utc(row['timestamp_utc']),
                    high=float(row['high']),
                    low=float(row['low']),
                    close=float(row['close']),
                )
            )
    bars.sort(key=lambda b: b.ts)
    return bars


def load_entries(path: Path) -> list[Trade]:
    trades: list[Trade] = []
    with path.open(newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        for row in r:
            signal_date = row['date']
            if row.get('buy_filled') == '1' and row.get('buy_fill_time_utc'):
                trades.append(
                    Trade(
                        signal_date=signal_date,
                        side='long',
                        entry_ts=_parse_ts_utc(row['buy_fill_time_utc']),
                        entry_price=float(row['buy_limit']),
                    )
                )
            if row.get('sell_filled') == '1' and row.get('sell_fill_time_utc'):
                trades.append(
                    Trade(
                        signal_date=signal_date,
                        side='short',
                        entry_ts=_parse_ts_utc(row['sell_fill_time_utc']),
                        entry_price=float(row['sell_limit']),
                    )
                )
    trades.sort(key=lambda t: t.entry_ts)
    return trades


def find_entry_bar_index(bars: list[Bar], ts: datetime) -> int:
    # Exact timestamp should exist; fallback to first bar >= ts.
    for i, b in enumerate(bars):
        if b.ts == ts:
            return i
    for i, b in enumerate(bars):
        if b.ts > ts:
            return i
    return -1


def _parse_ts_utc(v: str) -> datetime:
    ts = datetime.fromisoformat(v)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def pnl_pips(side: str, entry: float, exitp: float, pip_size: float) -> float:
    if side == 'long':
        return (exitp - entry) / pip_size
    return (entry - exitp) / pip_size


def run_trade(
    trade: Trade,
    bars: list[Bar],
    tp_pips: float,
    sl_pips: float,
    pip_size: float,
    tp_mode: str,
    trail_pips: float,
    max_hold_hours: int,
    max_hold_mode: str,
    intrabar_policy: str,
) -> dict:
    if not bars:
        return {
            'signal_date': trade.signal_date,
            'side': trade.side,
            'entry_ts_utc': trade.entry_ts.isoformat(),
            'entry_price': trade.entry_price,
            'tp_level': trade.entry_price,
            'sl_level': trade.entry_price,
            'tp_mode': tp_mode,
            'trail_pips': trail_pips,
            'trailing_activated': 0,
            'exit_ts_utc': trade.entry_ts.isoformat(),
            'exit_price': trade.entry_price,
            'exit_reason': 'no_hourly_data',
            'hold_hours': 0.0,
            'pnl_pips': 0.0,
        }

    idx = find_entry_bar_index(bars, trade.entry_ts)
    if idx < 0:
        return {
            'signal_date': trade.signal_date,
            'side': trade.side,
            'entry_ts_utc': trade.entry_ts.isoformat(),
            'entry_price': trade.entry_price,
            'tp_level': trade.entry_price,
            'sl_level': trade.entry_price,
            'tp_mode': tp_mode,
            'trail_pips': trail_pips,
            'trailing_activated': 0,
            'exit_ts_utc': trade.entry_ts.isoformat(),
            'exit_price': trade.entry_price,
            'exit_reason': 'entry_not_in_hourly_data',
            'hold_hours': 0.0,
            'pnl_pips': 0.0,
        }

    # Exit evaluation starts on the next bar to avoid ambiguity between entry and exit ordering
    # inside the same aggregated hourly candle.
    start_idx = min(idx + 1, len(bars) - 1)

    tp_dist = tp_pips * pip_size
    sl_dist = sl_pips * pip_size
    trail_dist = trail_pips * pip_size

    if trade.side == 'long':
        tp_level = trade.entry_price + tp_dist
        sl_level = trade.entry_price - sl_dist
    else:
        tp_level = trade.entry_price - tp_dist
        sl_level = trade.entry_price + sl_dist

    if max_hold_mode == 'eod_ny':
        # Session close assumption: forced close at next 5pm New York.
        ny_tz = ZoneInfo('America/New_York')
        entry_ny = trade.entry_ts.astimezone(ny_tz)
        cutoff_ny = entry_ny.replace(hour=17, minute=0, second=0, microsecond=0)
        if entry_ny >= cutoff_ny:
            cutoff_ny = cutoff_ny + timedelta(days=1)
        deadline = cutoff_ny.astimezone(timezone.utc)
    else:
        deadline = trade.entry_ts + timedelta(hours=max_hold_hours)

    exit_ts = bars[-1].ts
    exit_price = bars[-1].close
    exit_reason = 'end_of_data'
    trailing_active = False
    best_fav = None
    trailing_stop = None

    for i in range(start_idx, len(bars)):
        b = bars[i]

        if trailing_active:
            # Conservative trail logic: evaluate current stop before moving it deeper in profit.
            if trade.side == 'long':
                effective_stop = max(sl_level, trailing_stop if trailing_stop is not None else -1e99)
                if b.low <= effective_stop:
                    exit_ts, exit_price, exit_reason = b.ts, effective_stop, 'trailing_stop'
                    break
                best_fav = max(best_fav, b.high) if best_fav is not None else b.high
                trailing_stop = best_fav - trail_dist
            else:
                effective_stop = min(sl_level, trailing_stop if trailing_stop is not None else 1e99)
                if b.high >= effective_stop:
                    exit_ts, exit_price, exit_reason = b.ts, effective_stop, 'trailing_stop'
                    break
                best_fav = min(best_fav, b.low) if best_fav is not None else b.low
                trailing_stop = best_fav + trail_dist
        else:
            hit_tp = False
            hit_sl = False
            if trade.side == 'long':
                hit_tp = b.high >= tp_level
                hit_sl = b.low <= sl_level
            else:
                hit_tp = b.low <= tp_level
                hit_sl = b.high >= sl_level

            if hit_tp and hit_sl:
                # Intrabar ambiguity policy controls whether stop or target wins on same candle.
                if intrabar_policy == 'stop':
                    exit_ts, exit_price, exit_reason = b.ts, sl_level, 'stop_and_target_same_bar_stop'
                else:
                    if tp_mode == 'fixed':
                        exit_ts, exit_price, exit_reason = b.ts, tp_level, 'stop_and_target_same_bar_target'
                    else:
                        trailing_active = True
                        if trade.side == 'long':
                            best_fav = tp_level
                            trailing_stop = best_fav - trail_dist
                        else:
                            best_fav = tp_level
                            trailing_stop = best_fav + trail_dist
                        exit_reason = 'tp_activated_trailing_same_bar'
                if exit_reason.startswith('stop_and_target_same_bar'):
                    break
            elif hit_sl:
                exit_ts, exit_price, exit_reason = b.ts, sl_level, 'stop'
                break
            elif hit_tp:
                if tp_mode == 'fixed':
                    exit_ts, exit_price, exit_reason = b.ts, tp_level, 'target'
                    break
                trailing_active = True
                if trade.side == 'long':
                    best_fav = max(tp_level, b.high)
                    trailing_stop = best_fav - trail_dist
                else:
                    best_fav = min(tp_level, b.low)
                    trailing_stop = best_fav + trail_dist
                exit_reason = 'tp_activated_trailing'

        if b.ts >= deadline:
            exit_ts, exit_price, exit_reason = b.ts, b.close, 'max_hold_close'
            break

    pips = pnl_pips(trade.side, trade.entry_price, exit_price, pip_size)
    hold_hours = (exit_ts - trade.entry_ts).total_seconds() / 3600.0

    return {
        'signal_date': trade.signal_date,
        'side': trade.side,
        'entry_ts_utc': trade.entry_ts.isoformat(),
        'entry_price': trade.entry_price,
        'tp_level': tp_level,
        'sl_level': sl_level,
        'tp_mode': tp_mode,
        'trail_pips': trail_pips,
        'trailing_activated': int(trailing_active),
        'exit_ts_utc': exit_ts.isoformat(),
        'exit_price': exit_price,
        'exit_reason': exit_reason,
        'hold_hours': hold_hours,
        'pnl_pips': pips,
    }


def main() -> int:
    args = parse_args()

    bars = load_bars(Path(args.hourly))
    entries = load_entries(Path(args.signals))

    out = [
        run_trade(
            trade=t,
            bars=bars,
            tp_pips=args.tp_pips,
            sl_pips=args.sl_pips,
            pip_size=args.pip_size,
            tp_mode=args.tp_mode,
            trail_pips=args.trail_pips,
            max_hold_hours=args.max_hold_hours,
            max_hold_mode=args.max_hold_mode,
            intrabar_policy=args.intrabar_policy,
        )
        for t in entries
    ]

    out_trades = Path(args.output_trades)
    out_trades.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        'signal_date', 'side', 'entry_ts_utc', 'entry_price', 'tp_level', 'sl_level',
        'tp_mode', 'trail_pips', 'trailing_activated',
        'exit_ts_utc', 'exit_price', 'exit_reason', 'hold_hours', 'pnl_pips',
    ]
    with out_trades.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(out)

    daily = {}
    for r in out:
        d = r['signal_date']
        agg = daily.setdefault(d, {'trades': 0, 'long_trades': 0, 'short_trades': 0, 'pnl_pips': 0.0})
        agg['trades'] += 1
        if r['side'] == 'long':
            agg['long_trades'] += 1
        else:
            agg['short_trades'] += 1
        agg['pnl_pips'] += float(r['pnl_pips'])

    out_daily = Path(args.output_daily)
    with out_daily.open('w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['signal_date', 'trades', 'long_trades', 'short_trades', 'pnl_pips'])
        for d in sorted(daily):
            a = daily[d]
            w.writerow([d, a['trades'], a['long_trades'], a['short_trades'], f"{a['pnl_pips']:.6f}"])

    total_pnl = sum(float(r['pnl_pips']) for r in out)
    wins = sum(1 for r in out if float(r['pnl_pips']) > 0)
    losses = sum(1 for r in out if float(r['pnl_pips']) < 0)

    print(f'Entries: {len(out)}')
    print(f'Wins: {wins} | Losses: {losses} | Win rate: {(wins/len(out) if out else 0):.4f}')
    print(f'Total pnl (pips): {total_pnl:.2f}')
    print(f'Wrote {out_trades}')
    print(f'Wrote {out_daily}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
