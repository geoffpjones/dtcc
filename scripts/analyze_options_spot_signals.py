#!/usr/bin/env python3
from __future__ import annotations

import csv
import math
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, median

import numpy as np

IN_DAILY = Path('/home/geoffpjones/projects/dtcc/data/eurusd_options_spot_daily_joined.csv')

OUT_REG = Path('/home/geoffpjones/projects/dtcc/data/eurusd_options_spot_lagged_regressions.csv')
OUT_QUANT = Path('/home/geoffpjones/projects/dtcc/data/eurusd_options_spot_quantile_tests.csv')
OUT_EVENT = Path('/home/geoffpjones/projects/dtcc/data/eurusd_options_spot_event_windows.csv')
OUT_REPORT = Path('/home/geoffpjones/projects/dtcc/data/eurusd_options_spot_deep_dive_report.txt')


@dataclass
class Row:
    date: str
    spot_close: float
    delta_total_notional_eur: float
    put_call_notional_ratio: float
    top_any_notional_share: float
    dist_close_to_top_any_strike: float


def f(v: str) -> float:
    t = (v or '').strip()
    if t == '':
        return float('nan')
    try:
        return float(t)
    except Exception:
        return float('nan')


def load_rows() -> list[Row]:
    rows: list[Row] = []
    with IN_DAILY.open(newline='', encoding='utf-8') as fh:
        r = csv.DictReader(fh)
        for x in r:
            rows.append(
                Row(
                    date=x['date'],
                    spot_close=f(x['spot_close']),
                    delta_total_notional_eur=f(x['delta_total_notional_eur']),
                    put_call_notional_ratio=f(x['put_call_notional_ratio']),
                    top_any_notional_share=f(x['top_any_notional_share']),
                    dist_close_to_top_any_strike=f(x['dist_close_to_top_any_strike']),
                )
            )
    rows.sort(key=lambda z: z.date)
    return rows


def aligned_future_return(rows: list[Row], horizon: int) -> list[float]:
    out = [float('nan')] * len(rows)
    for i in range(len(rows) - horizon):
        c0 = rows[i].spot_close
        c1 = rows[i + horizon].spot_close
        if c0 == 0 or math.isnan(c0) or math.isnan(c1):
            continue
        out[i] = c1 / c0 - 1.0
    return out


def valid_xy(x: list[float], y: list[float]) -> tuple[np.ndarray, np.ndarray]:
    vals = [(a, b) for a, b in zip(x, y) if not (math.isnan(a) or math.isnan(b))]
    if not vals:
        return np.array([]), np.array([])
    xa = np.array([v[0] for v in vals], dtype=float)
    ya = np.array([v[1] for v in vals], dtype=float)
    return xa, ya


def ols_univariate(x: list[float], y: list[float]) -> dict:
    xa, ya = valid_xy(x, y)
    n = len(xa)
    if n < 5:
        return {'n': n, 'alpha': float('nan'), 'beta': float('nan'), 'r2': float('nan'), 't_beta': float('nan')}

    X = np.column_stack([np.ones(n), xa])
    beta_hat = np.linalg.lstsq(X, ya, rcond=None)[0]
    yhat = X @ beta_hat
    resid = ya - yhat

    sse = float(np.sum(resid**2))
    sst = float(np.sum((ya - np.mean(ya)) ** 2))
    r2 = float('nan') if sst == 0 else 1.0 - sse / sst

    dof = n - X.shape[1]
    sigma2 = sse / dof if dof > 0 else float('nan')
    xtx_inv = np.linalg.inv(X.T @ X)
    se_beta = math.sqrt(sigma2 * xtx_inv[1, 1]) if sigma2 == sigma2 else float('nan')
    t_beta = beta_hat[1] / se_beta if se_beta and se_beta == se_beta else float('nan')

    return {
        'n': n,
        'alpha': float(beta_hat[0]),
        'beta': float(beta_hat[1]),
        'r2': r2,
        't_beta': float(t_beta),
    }


def ols_multivariate(features: list[list[float]], y: list[float]) -> dict:
    rows = []
    ys = []
    for i in range(len(y)):
        if math.isnan(y[i]):
            continue
        fx = features[i]
        if any(math.isnan(v) for v in fx):
            continue
        rows.append([1.0] + fx)
        ys.append(y[i])

    if len(rows) < 10:
        return {'n': len(rows), 'r2': float('nan'), 'coef': []}

    X = np.array(rows, dtype=float)
    Y = np.array(ys, dtype=float)
    b = np.linalg.lstsq(X, Y, rcond=None)[0]
    yhat = X @ b
    sse = float(np.sum((Y - yhat) ** 2))
    sst = float(np.sum((Y - np.mean(Y)) ** 2))
    r2 = float('nan') if sst == 0 else 1.0 - sse / sst
    return {'n': len(rows), 'r2': r2, 'coef': [float(v) for v in b]}


def quantile(values: list[float], q: float) -> float:
    clean = sorted([x for x in values if not math.isnan(x)])
    if not clean:
        return float('nan')
    idx = int((len(clean) - 1) * q)
    return clean[idx]


def normal_2sided_p_from_t(t: float) -> float:
    if math.isnan(t):
        return float('nan')
    z = abs(t)
    return math.erfc(z / math.sqrt(2.0))


def welch_test(a: list[float], b: list[float]) -> tuple[float, float, float, float, int, int]:
    aa = [x for x in a if not math.isnan(x)]
    bb = [x for x in b if not math.isnan(x)]
    if len(aa) < 3 or len(bb) < 3:
        return float('nan'), float('nan'), float('nan'), float('nan'), len(aa), len(bb)
    ma, mb = mean(aa), mean(bb)
    va = np.var(np.array(aa), ddof=1)
    vb = np.var(np.array(bb), ddof=1)
    se = math.sqrt(va / len(aa) + vb / len(bb))
    if se == 0:
        return ma, mb, ma - mb, float('nan'), len(aa), len(bb)
    t = (ma - mb) / se
    p = normal_2sided_p_from_t(t)
    return ma, mb, ma - mb, p, len(aa), len(bb)


def run() -> None:
    rows = load_rows()

    futures = {h: aligned_future_return(rows, h) for h in (1, 2, 3)}

    reg_rows: list[dict] = []
    features = {
        'delta_total_notional_eur': [r.delta_total_notional_eur for r in rows],
        'put_call_notional_ratio': [r.put_call_notional_ratio for r in rows],
        'top_any_notional_share': [r.top_any_notional_share for r in rows],
        'dist_close_to_top_any_strike': [r.dist_close_to_top_any_strike for r in rows],
    }

    for h in (1, 2, 3):
        y = futures[h]
        for name, x in features.items():
            m = ols_univariate(x, y)
            reg_rows.append({
                'model': 'univariate',
                'horizon_days': h,
                'feature': name,
                **m,
            })

        Xmulti = [
            [
                rows[i].delta_total_notional_eur,
                rows[i].put_call_notional_ratio,
                rows[i].top_any_notional_share,
                rows[i].dist_close_to_top_any_strike,
            ]
            for i in range(len(rows))
        ]
        mm = ols_multivariate(Xmulti, y)
        reg_rows.append({
            'model': 'multivariate',
            'horizon_days': h,
            'feature': 'delta+put_call+share+dist',
            'n': mm['n'],
            'alpha': mm['coef'][0] if mm['coef'] else float('nan'),
            'beta': float('nan'),
            'r2': mm['r2'],
            't_beta': float('nan'),
            'coef_delta_total_notional': mm['coef'][1] if len(mm['coef']) > 1 else float('nan'),
            'coef_put_call_ratio': mm['coef'][2] if len(mm['coef']) > 2 else float('nan'),
            'coef_top_any_notional_share': mm['coef'][3] if len(mm['coef']) > 3 else float('nan'),
            'coef_dist_close_to_top_any_strike': mm['coef'][4] if len(mm['coef']) > 4 else float('nan'),
        })

    OUT_REG.parent.mkdir(parents=True, exist_ok=True)
    reg_fields = [
        'model', 'horizon_days', 'feature', 'n', 'alpha', 'beta', 'r2', 't_beta',
        'coef_delta_total_notional', 'coef_put_call_ratio', 'coef_top_any_notional_share',
        'coef_dist_close_to_top_any_strike',
    ]
    with OUT_REG.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=reg_fields)
        w.writeheader()
        for r in reg_rows:
            w.writerow(r)

    # Quantile tests (top decile event days)
    quant_rows: list[dict] = []
    x_abs_delta = [abs(r.delta_total_notional_eur) if not math.isnan(r.delta_total_notional_eur) else float('nan') for r in rows]
    x_share = [r.top_any_notional_share for r in rows]

    thr_delta = quantile(x_abs_delta, 0.9)
    thr_share = quantile(x_share, 0.9)

    for h in (1, 2, 3):
        y_abs = [abs(v) if not math.isnan(v) else float('nan') for v in futures[h]]

        top_delta = [y_abs[i] for i in range(len(rows)) if not math.isnan(x_abs_delta[i]) and x_abs_delta[i] >= thr_delta]
        rest_delta = [y_abs[i] for i in range(len(rows)) if not math.isnan(x_abs_delta[i]) and x_abs_delta[i] < thr_delta]
        ma, mb, diff, p, na, nb = welch_test(top_delta, rest_delta)
        quant_rows.append({
            'test': 'top10_abs_delta_notional_vs_rest',
            'horizon_days': h,
            'threshold': thr_delta,
            'top_mean_abs_return': ma,
            'rest_mean_abs_return': mb,
            'difference_top_minus_rest': diff,
            'approx_p_value': p,
            'n_top': na,
            'n_rest': nb,
            'top_median_abs_return': median([x for x in top_delta if not math.isnan(x)]) if na else float('nan'),
            'rest_median_abs_return': median([x for x in rest_delta if not math.isnan(x)]) if nb else float('nan'),
        })

        top_share = [y_abs[i] for i in range(len(rows)) if not math.isnan(x_share[i]) and x_share[i] >= thr_share]
        rest_share = [y_abs[i] for i in range(len(rows)) if not math.isnan(x_share[i]) and x_share[i] < thr_share]
        ma, mb, diff, p, na, nb = welch_test(top_share, rest_share)
        quant_rows.append({
            'test': 'top10_top_any_notional_share_vs_rest',
            'horizon_days': h,
            'threshold': thr_share,
            'top_mean_abs_return': ma,
            'rest_mean_abs_return': mb,
            'difference_top_minus_rest': diff,
            'approx_p_value': p,
            'n_top': na,
            'n_rest': nb,
            'top_median_abs_return': median([x for x in top_share if not math.isnan(x)]) if na else float('nan'),
            'rest_median_abs_return': median([x for x in rest_share if not math.isnan(x)]) if nb else float('nan'),
        })

    quant_fields = [
        'test', 'horizon_days', 'threshold', 'top_mean_abs_return', 'rest_mean_abs_return',
        'difference_top_minus_rest', 'approx_p_value', 'n_top', 'n_rest',
        'top_median_abs_return', 'rest_median_abs_return',
    ]
    with OUT_QUANT.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=quant_fields)
        w.writeheader()
        for r in quant_rows:
            w.writerow(r)

    # Event windows around large-strike days
    event_idx = [i for i, r in enumerate(rows) if not math.isnan(r.top_any_notional_share) and r.top_any_notional_share >= thr_share]

    windows = [-2, -1, 0, 1, 2, 3]
    by_tau: dict[int, list[float]] = {t: [] for t in windows}
    by_tau_abs: dict[int, list[float]] = {t: [] for t in windows}

    # day return at tau means return from day(tau-1) close to day(tau) close within event-relative index
    for e in event_idx:
        for tau in windows:
            j = e + tau
            if j <= 0 or j >= len(rows):
                continue
            c_prev = rows[j - 1].spot_close
            c_cur = rows[j].spot_close
            if c_prev == 0 or math.isnan(c_prev) or math.isnan(c_cur):
                continue
            r = c_cur / c_prev - 1.0
            by_tau[tau].append(r)
            by_tau_abs[tau].append(abs(r))

    event_rows: list[dict] = []
    for tau in windows:
        vals = by_tau[tau]
        avals = by_tau_abs[tau]
        event_rows.append({
            'tau_day': tau,
            'event_count': len(vals),
            'avg_return': mean(vals) if vals else float('nan'),
            'avg_abs_return': mean(avals) if avals else float('nan'),
            'median_return': median(vals) if vals else float('nan'),
            'median_abs_return': median(avals) if avals else float('nan'),
        })

    # forward cumulative returns after event day
    for h in (1, 2, 3):
        ev = []
        non = []
        ev_set = set(event_idx)
        for i in range(len(rows) - h):
            c0 = rows[i].spot_close
            c1 = rows[i + h].spot_close
            if c0 == 0 or math.isnan(c0) or math.isnan(c1):
                continue
            r = c1 / c0 - 1.0
            if i in ev_set:
                ev.append(r)
            else:
                non.append(r)
        ma, mb, diff, p, na, nb = welch_test(ev, non)
        event_rows.append({
            'tau_day': f'post_h{h}',
            'event_count': na,
            'avg_return': ma,
            'avg_abs_return': mean([abs(x) for x in ev]) if ev else float('nan'),
            'median_return': median(ev) if ev else float('nan'),
            'median_abs_return': median([abs(x) for x in ev]) if ev else float('nan'),
            'comparison_non_event_mean': mb,
            'difference_event_minus_non_event': diff,
            'approx_p_value': p,
            'non_event_count': nb,
        })

    event_fields = [
        'tau_day', 'event_count', 'avg_return', 'avg_abs_return', 'median_return', 'median_abs_return',
        'comparison_non_event_mean', 'difference_event_minus_non_event', 'approx_p_value', 'non_event_count',
    ]
    with OUT_EVENT.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=event_fields)
        w.writeheader()
        for r in event_rows:
            w.writerow(r)

    with OUT_REPORT.open('w', encoding='utf-8') as f:
        f.write('EURUSD Options/Spot Deep-Dive\n')
        f.write('===========================\n\n')
        f.write(f'Input rows: {len(rows)}\n')
        f.write(f'Large-strike threshold (top_any_notional_share, 90th pct): {thr_share:.6f}\n')
        f.write(f'Large-interest threshold (abs delta notional, 90th pct): {thr_delta:.2f}\n')
        f.write(f'Event days (large-strike): {len(event_idx)}\n\n')

        f.write('Lagged Regression Highlights (univariate beta / R2):\n')
        for h in (1, 2, 3):
            f.write(f'  Horizon t+{h}:\n')
            for feat in ('delta_total_notional_eur', 'put_call_notional_ratio', 'top_any_notional_share', 'dist_close_to_top_any_strike'):
                r = next(x for x in reg_rows if x['model'] == 'univariate' and x['horizon_days'] == h and x['feature'] == feat)
                f.write(f"    {feat}: beta={r['beta']:.8f}, t={r['t_beta']:.4f}, R2={r['r2']:.6f}, n={r['n']}\n")
        f.write('\nQuantile Test Highlights (top decile vs rest, abs future return):\n')
        for r in quant_rows:
            f.write(
                f"  {r['test']} h={r['horizon_days']}: top={r['top_mean_abs_return']:.6f}, rest={r['rest_mean_abs_return']:.6f}, diff={r['difference_top_minus_rest']:.6f}, p~{r['approx_p_value']:.4f}, n_top={r['n_top']}\n"
            )
        f.write('\nEvent Window (large-strike days): see CSV for full table.\n')

    print(f'Wrote {OUT_REG}')
    print(f'Wrote {OUT_QUANT}')
    print(f'Wrote {OUT_EVENT}')
    print(f'Wrote {OUT_REPORT}')


if __name__ == '__main__':
    run()
