#!/usr/bin/env python3
from __future__ import annotations

import csv
import subprocess
import tempfile
from pathlib import Path

BT = '/home/geoffpjones/projects/dtcc/scripts/backtest_walkforward_gamma_sl_tp.py'


def summarize(trades_csv: str):
    rows = list(csv.DictReader(open(trades_csv, newline='', encoding='utf-8')))
    n = len(rows)
    pnl = sum(float(r['pnl_pips']) for r in rows)
    wins = sum(1 for r in rows if float(r['pnl_pips']) > 0)
    losses = sum(1 for r in rows if float(r['pnl_pips']) < 0)
    return {
        'entries': n,
        'wins': wins,
        'losses': losses,
        'win_rate': wins / n if n else 0.0,
        'total_pnl_pips': pnl,
        'avg_pnl_pips': pnl / n if n else 0.0,
    }


def run_one(signals: str, hourly: str, tp_mode: str, tp: int, sl: int, trail: int, pip_size: float, out_trades: str, out_daily: str):
    cmd = [
        'python3', BT,
        '--signals', signals,
        '--hourly', hourly,
        '--tp-mode', tp_mode,
        '--tp-pips', str(tp),
        '--sl-pips', str(sl),
        '--trail-pips', str(trail),
        '--pip-size', str(pip_size),
        '--max-hold-mode', 'eod_ny',
        '--output-trades', out_trades,
        '--output-daily', out_daily,
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main():
    import argparse

    ap = argparse.ArgumentParser(description='Compare best fixed vs trailing SL/TP on train, apply to test.')
    ap.add_argument('--pair', required=True, help='Pair code like audusd, gbpusd, usdcad, usdjpy')
    ap.add_argument('--train-signals', required=True)
    ap.add_argument('--test-signals', required=True)
    ap.add_argument('--hourly', required=True)
    ap.add_argument('--out-grid', required=True)
    ap.add_argument('--out-summary', required=True)
    ap.add_argument('--pip-size', type=float, default=0.0001)
    args = ap.parse_args()

    pair = args.pair.lower()
    TRAIN_SIGNALS = args.train_signals
    TEST_SIGNALS = args.test_signals
    OUT_GRID = args.out_grid
    OUT_SUM = args.out_summary
    pip_size = args.pip_size
    hourly = args.hourly
    tps = [15, 20, 25, 30, 35, 40]
    sls = [15, 20, 25, 30, 35, 40]
    trails = [5, 10, 15, 20]

    grid = []

    with tempfile.TemporaryDirectory() as td:
        # Fixed mode grid
        for tp in tps:
            for sl in sls:
                tr = str(Path(td) / f'fixed_train_tp{tp}_sl{sl}.csv')
                dy = str(Path(td) / f'fixed_train_tp{tp}_sl{sl}_daily.csv')
                run_one(TRAIN_SIGNALS, hourly, 'fixed', tp, sl, 10, pip_size, tr, dy)
                s = summarize(tr)
                s.update({'mode': 'fixed', 'tp_pips': tp, 'sl_pips': sl, 'trail_pips': ''})
                grid.append(s)

        # Trailing mode grid
        for tp in tps:
            for sl in sls:
                for trl in trails:
                    tr = str(Path(td) / f'trail_train_tp{tp}_sl{sl}_tr{trl}.csv')
                    dy = str(Path(td) / f'trail_train_tp{tp}_sl{sl}_tr{trl}_daily.csv')
                    run_one(TRAIN_SIGNALS, hourly, 'trail_after_tp', tp, sl, trl, pip_size, tr, dy)
                    s = summarize(tr)
                    s.update({'mode': 'trail_after_tp', 'tp_pips': tp, 'sl_pips': sl, 'trail_pips': trl})
                    grid.append(s)

    grid.sort(key=lambda r: (r['total_pnl_pips'], r['win_rate'], r['avg_pnl_pips']), reverse=True)

    with open(OUT_GRID, 'w', newline='', encoding='utf-8') as f:
        fields = ['mode', 'tp_pips', 'sl_pips', 'trail_pips', 'entries', 'wins', 'losses', 'win_rate', 'avg_pnl_pips', 'total_pnl_pips']
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(grid)

    best_fixed = next(r for r in grid if r['mode'] == 'fixed')
    best_trail = next(r for r in grid if r['mode'] == 'trail_after_tp')

    # Apply best fixed on test
    fx_tp, fx_sl = int(best_fixed['tp_pips']), int(best_fixed['sl_pips'])
    fx_train_tr = f'/home/geoffpjones/projects/dtcc/data/{pair}_sl_tp_fixed_train_best_trades.csv'
    fx_train_dy = f'/home/geoffpjones/projects/dtcc/data/{pair}_sl_tp_fixed_train_best_daily.csv'
    fx_test_tr = f'/home/geoffpjones/projects/dtcc/data/{pair}_sl_tp_fixed_test_best_trades.csv'
    fx_test_dy = f'/home/geoffpjones/projects/dtcc/data/{pair}_sl_tp_fixed_test_best_daily.csv'
    run_one(TRAIN_SIGNALS, hourly, 'fixed', fx_tp, fx_sl, 10, pip_size, fx_train_tr, fx_train_dy)
    run_one(TEST_SIGNALS, hourly, 'fixed', fx_tp, fx_sl, 10, pip_size, fx_test_tr, fx_test_dy)
    fx_train = summarize(fx_train_tr)
    fx_test = summarize(fx_test_tr)

    # Apply best trailing on test
    tr_tp, tr_sl, tr_tr = int(best_trail['tp_pips']), int(best_trail['sl_pips']), int(best_trail['trail_pips'])
    tr_train_tr = f'/home/geoffpjones/projects/dtcc/data/{pair}_sl_tp_trailing_train_best_trades.csv'
    tr_train_dy = f'/home/geoffpjones/projects/dtcc/data/{pair}_sl_tp_trailing_train_best_daily.csv'
    tr_test_tr = f'/home/geoffpjones/projects/dtcc/data/{pair}_sl_tp_trailing_test_best_trades.csv'
    tr_test_dy = f'/home/geoffpjones/projects/dtcc/data/{pair}_sl_tp_trailing_test_best_daily.csv'
    run_one(TRAIN_SIGNALS, hourly, 'trail_after_tp', tr_tp, tr_sl, tr_tr, pip_size, tr_train_tr, tr_train_dy)
    run_one(TEST_SIGNALS, hourly, 'trail_after_tp', tr_tp, tr_sl, tr_tr, pip_size, tr_test_tr, tr_test_dy)
    tr_train = summarize(tr_train_tr)
    tr_test = summarize(tr_test_tr)

    with open(OUT_SUM, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['series', 'mode', 'tp_pips', 'sl_pips', 'trail_pips', 'entries', 'wins', 'losses', 'win_rate', 'avg_pnl_pips', 'total_pnl_pips'])
        w.writerow(['train_best_fixed', 'fixed', fx_tp, fx_sl, '', fx_train['entries'], fx_train['wins'], fx_train['losses'], f"{fx_train['win_rate']:.6f}", f"{fx_train['avg_pnl_pips']:.6f}", f"{fx_train['total_pnl_pips']:.6f}"])
        w.writerow(['test_with_best_fixed', 'fixed', fx_tp, fx_sl, '', fx_test['entries'], fx_test['wins'], fx_test['losses'], f"{fx_test['win_rate']:.6f}", f"{fx_test['avg_pnl_pips']:.6f}", f"{fx_test['total_pnl_pips']:.6f}"])
        w.writerow(['train_best_trailing', 'trail_after_tp', tr_tp, tr_sl, tr_tr, tr_train['entries'], tr_train['wins'], tr_train['losses'], f"{tr_train['win_rate']:.6f}", f"{tr_train['avg_pnl_pips']:.6f}", f"{tr_train['total_pnl_pips']:.6f}"])
        w.writerow(['test_with_best_trailing', 'trail_after_tp', tr_tp, tr_sl, tr_tr, tr_test['entries'], tr_test['wins'], tr_test['losses'], f"{tr_test['win_rate']:.6f}", f"{tr_test['avg_pnl_pips']:.6f}", f"{tr_test['total_pnl_pips']:.6f}"])

    print('best fixed:', fx_tp, fx_sl, 'train pnl', round(fx_train['total_pnl_pips'], 3), 'test pnl', round(fx_test['total_pnl_pips'], 3))
    print('best trailing:', tr_tp, tr_sl, tr_tr, 'train pnl', round(tr_train['total_pnl_pips'], 3), 'test pnl', round(tr_test['total_pnl_pips'], 3))
    print('wrote', OUT_GRID)
    print('wrote', OUT_SUM)


if __name__ == '__main__':
    main()
