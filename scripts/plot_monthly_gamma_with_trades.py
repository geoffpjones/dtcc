#!/usr/bin/env python3
from __future__ import annotations

import csv
from datetime import date, datetime, timezone
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

HOURLY = Path('/home/geoffpjones/projects/dtcc/tick_data/eurusd_1h.csv')
TRADES = Path('/home/geoffpjones/projects/dtcc/data/eurusd_walkforward_gamma_limits_2025-11-11_to_2026-03-31.csv')
OUTDIR = Path('/home/geoffpjones/projects/dtcc/data/charts_gamma_call_put_trades_walkforward_limits')

MONTHS = [(2025, 11), (2025, 12), (2026, 1), (2026, 2), (2026, 3)]


def load_hourly(path: Path):
    hourly = []
    close_by_ts = {}
    day_last = {}
    with path.open(newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        for row in r:
            ts = datetime.fromisoformat(row['timestamp_utc'])
            c = float(row['close'])
            h = float(row['high'])
            l = float(row['low'])
            hourly.append((ts, c, h, l))
            close_by_ts[ts] = c
            d = ts.date()
            prev = day_last.get(d)
            if prev is None or ts > prev[0]:
                day_last[d] = (ts, c)
    hourly.sort(key=lambda x: x[0])
    return hourly, close_by_ts, day_last


def load_trades(path: Path):
    out = {}
    with path.open(newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        for row in r:
            d = date.fromisoformat(row['date'])
            out[d] = {
                'buy': float(row['buy_limit']) if row['buy_limit'] else float('nan'),
                'sell': float(row['sell_limit']) if row['sell_limit'] else float('nan'),
                'buy_filled': row['buy_filled'] == '1',
                'sell_filled': row['sell_filled'] == '1',
                'buy_ts': datetime.fromisoformat(row['buy_fill_time_utc']) if row['buy_fill_time_utc'] else None,
                'sell_ts': datetime.fromisoformat(row['sell_fill_time_utc']) if row['sell_fill_time_utc'] else None,
                'net': float(row['net_pnl_pips']) if row['net_pnl_pips'] else 0.0,
            }
    return out


def monthly_bounds(y: int, m: int):
    start = datetime(y, m, 1, tzinfo=timezone.utc)
    if m == 12:
        end = datetime(y + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(y, m + 1, 1, tzinfo=timezone.utc)
    return start, end


def main() -> int:
    OUTDIR.mkdir(parents=True, exist_ok=True)

    hourly, close_by_ts, day_last = load_hourly(HOURLY)
    trades = load_trades(TRADES)

    for y, m in MONTHS:
        start, end = monthly_bounds(y, m)

        x = [t for t, _, _, _ in hourly if start <= t < end]
        yclose = [close_by_ts[t] for t in x]

        # Expand daily limits to hourly timestamps directly to avoid perceived day-shift.
        buy_line = []
        sell_line = []
        for t in x:
            tr = trades.get(t.date())
            buy_line.append(tr['buy'] if tr else float('nan'))
            sell_line.append(tr['sell'] if tr else float('nan'))

        fig, ax = plt.subplots(figsize=(15, 7), constrained_layout=True)

        ax.plot(x, yclose, color='#808080', lw=1.0, alpha=0.55, label='EURUSD Hourly Close')
        ax.plot(x, buy_line, color='#2ca02c', lw=1.8, label='Walk-forward Buy Limit (hourly-expanded)')
        ax.plot(x, sell_line, color='#d62728', lw=1.8, label='Walk-forward Sell Limit (hourly-expanded)')

        shown = set()
        for d, tr in trades.items():
            if not (start.date() <= d < end.date()):
                continue
            if d not in day_last:
                continue
            eod_ts, eod_px = day_last[d]

            if tr['buy_filled'] and tr['buy_ts'] is not None:
                segx = [t for t in x if tr['buy_ts'] <= t <= eod_ts]
                if segx:
                    segy = [close_by_ts[t] for t in segx]
                    ax.plot(segx, segy, color='blue', lw=2.1, alpha=0.9, label='Long period' if 'long' not in shown else None)
                    shown.add('long')
                fp = close_by_ts.get(tr['buy_ts'])
                if fp is not None:
                    ax.scatter(tr['buy_ts'], fp, marker='^', color='blue', s=52, zorder=6, label='Buy fill' if 'buy' not in shown else None)
                    shown.add('buy')

            if tr['sell_filled'] and tr['sell_ts'] is not None:
                segx = [t for t in x if tr['sell_ts'] <= t <= eod_ts]
                if segx:
                    segy = [close_by_ts[t] for t in segx]
                    ax.plot(segx, segy, color='green', lw=2.1, alpha=0.9, label='Short period' if 'short' not in shown else None)
                    shown.add('short')
                fp = close_by_ts.get(tr['sell_ts'])
                if fp is not None:
                    ax.scatter(tr['sell_ts'], fp, marker='v', color='green', s=52, zorder=6, label='Sell fill' if 'sell' not in shown else None)
                    shown.add('sell')

            if tr['buy_filled'] or tr['sell_filled']:
                if tr['net'] > 0:
                    c = 'black'; label = 'EOD close (profit)'; key = 'eodp'
                elif tr['net'] < 0:
                    c = 'red'; label = 'EOD close (loss)'; key = 'eodl'
                else:
                    c = '#555555'; label = 'EOD close (flat)'; key = 'eodf'
                ax.scatter(eod_ts, eod_px, marker='o', color=c, edgecolors='white', linewidths=0.5, s=42,
                           zorder=7, label=label if key not in shown else None)
                shown.add(key)

        ax.set_title(f'EURUSD + Walk-Forward Limits/Trades - {y}-{m:02d} (UTC)')
        ax.set_ylabel('EURUSD')
        ax.set_xlim(start, end)
        ax.grid(True, alpha=0.25)
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        plt.setp(ax.get_xticklabels(), rotation=45, ha='right')
        ax.legend(loc='upper left', ncol=2)

        out = OUTDIR / f'eurusd_{y}_{m:02d}_walkforward_limits_with_trades.png'
        fig.savefig(out, dpi=170)
        plt.close(fig)
        print(f'wrote {out}')

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
