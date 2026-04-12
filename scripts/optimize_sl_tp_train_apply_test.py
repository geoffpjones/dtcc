#!/usr/bin/env python3
from __future__ import annotations

import csv
import subprocess
import tempfile
from pathlib import Path

BT = '/home/geoffpjones/projects/dtcc/scripts/backtest_walkforward_gamma_sl_tp.py'
TRAIN_SIGNALS = '/home/geoffpjones/projects/dtcc/data/eurusd_walkforward_limits_train_2025-06-01_to_2025-11-10.csv'
TEST_SIGNALS = '/home/geoffpjones/projects/dtcc/data/eurusd_walkforward_limits_test_2025-11-11_to_2026-04-04.csv'

OUT_GRID = '/home/geoffpjones/projects/dtcc/data/eurusd_sl_tp_grid_train_2025-06-01_to_2025-11-10.csv'
OUT_BEST = '/home/geoffpjones/projects/dtcc/data/eurusd_sl_tp_best_params_train_to_test.csv'
OUT_TEST_TRADES = '/home/geoffpjones/projects/dtcc/data/eurusd_sl_tp_test_trades_2025-11-11_to_2026-04-04.csv'
OUT_TEST_DAILY = '/home/geoffpjones/projects/dtcc/data/eurusd_sl_tp_test_daily_2025-11-11_to_2026-04-04.csv'
OUT_TRAIN_TRADES_BEST = '/home/geoffpjones/projects/dtcc/data/eurusd_sl_tp_train_best_trades_2025-06-01_to_2025-11-10.csv'
OUT_TRAIN_DAILY_BEST = '/home/geoffpjones/projects/dtcc/data/eurusd_sl_tp_train_best_daily_2025-06-01_to_2025-11-10.csv'


def summarize(trades_csv: str):
    rows = list(csv.DictReader(open(trades_csv, newline='', encoding='utf-8')))
    n = len(rows)
    pnl = sum(float(r['pnl_pips']) for r in rows)
    wins = sum(1 for r in rows if float(r['pnl_pips']) > 0)
    losses = sum(1 for r in rows if float(r['pnl_pips']) < 0)
    win_rate = wins / n if n else 0.0
    avg = pnl / n if n else 0.0
    return {
        'entries': n,
        'wins': wins,
        'losses': losses,
        'win_rate': win_rate,
        'total_pnl_pips': pnl,
        'avg_pnl_pips': avg,
    }


def run_one(signals: str, tp: int, sl: int, out_trades: str, out_daily: str):
    cmd = [
        'python3', BT,
        '--signals', signals,
        '--tp-pips', str(tp),
        '--sl-pips', str(sl),
        '--tp-mode', 'fixed',
        '--max-hold-mode', 'eod_ny',
        '--output-trades', out_trades,
        '--output-daily', out_daily,
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main():
    tps = [10, 15, 20, 25, 30, 35, 40]
    sls = [10, 15, 20, 25, 30, 35, 40]

    grid = []
    with tempfile.TemporaryDirectory() as td:
        for tp in tps:
            for sl in sls:
                tr = str(Path(td) / f'train_tp{tp}_sl{sl}.csv')
                dy = str(Path(td) / f'train_tp{tp}_sl{sl}_daily.csv')
                run_one(TRAIN_SIGNALS, tp, sl, tr, dy)
                s = summarize(tr)
                s.update({'tp_pips': tp, 'sl_pips': sl})
                grid.append(s)

    grid.sort(key=lambda r: (r['total_pnl_pips'], r['win_rate'], r['avg_pnl_pips']), reverse=True)

    with open(OUT_GRID, 'w', newline='', encoding='utf-8') as f:
        fields = ['tp_pips', 'sl_pips', 'entries', 'wins', 'losses', 'win_rate', 'avg_pnl_pips', 'total_pnl_pips']
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(grid)

    best = grid[0]
    tp = int(best['tp_pips'])
    sl = int(best['sl_pips'])

    # Save best train run outputs
    run_one(TRAIN_SIGNALS, tp, sl, OUT_TRAIN_TRADES_BEST, OUT_TRAIN_DAILY_BEST)
    train_best = summarize(OUT_TRAIN_TRADES_BEST)

    # Apply to test window
    run_one(TEST_SIGNALS, tp, sl, OUT_TEST_TRADES, OUT_TEST_DAILY)
    test_sum = summarize(OUT_TEST_TRADES)

    with open(OUT_BEST, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['set', 'tp_pips', 'sl_pips', 'entries', 'wins', 'losses', 'win_rate', 'avg_pnl_pips', 'total_pnl_pips'])
        w.writerow(['train_best_params', tp, sl, train_best['entries'], train_best['wins'], train_best['losses'], f"{train_best['win_rate']:.6f}", f"{train_best['avg_pnl_pips']:.6f}", f"{train_best['total_pnl_pips']:.6f}"])
        w.writerow(['test_with_train_best', tp, sl, test_sum['entries'], test_sum['wins'], test_sum['losses'], f"{test_sum['win_rate']:.6f}", f"{test_sum['avg_pnl_pips']:.6f}", f"{test_sum['total_pnl_pips']:.6f}"])

    print('best tp/sl', tp, sl)
    print('train total pnl', round(train_best['total_pnl_pips'], 4), 'entries', train_best['entries'])
    print('test total pnl', round(test_sum['total_pnl_pips'], 4), 'entries', test_sum['entries'])
    print('wrote', OUT_GRID)
    print('wrote', OUT_BEST)
    print('wrote', OUT_TRAIN_TRADES_BEST)
    print('wrote', OUT_TEST_TRADES)


if __name__ == '__main__':
    main()
