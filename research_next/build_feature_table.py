#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from bisect import bisect_left
from pathlib import Path

from common import (
    PAIRS,
    build_daily_bars,
    load_hourly_csv,
    pair_pip_size,
    parse_iso_date,
    rolling_atr,
    rolling_mean,
    rolling_realized_vol,
    safe_float,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build isolated trade-level feature tables for next-cycle research.")
    p.add_argument("--reopt-dir", required=True)
    p.add_argument("--tick-dir", required=True)
    p.add_argument("--data-dir", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--pairs", default=",".join(PAIRS))
    p.add_argument("--mode", default="trailing", choices=["trailing", "fixed"])
    return p.parse_args()


def load_gamma_daily(path: Path) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            out[row["date"]] = {
                "spot_close": float(row["spot_close"]),
                "active_call_notional": float(row["active_call_notional"]),
                "active_put_notional": float(row["active_put_notional"]),
                "call_gamma": float(row["call_gamma_abs_per_usd"]),
                "put_gamma": float(row["put_gamma_abs_per_usd"]),
                "total_gamma": float(row["total_gamma_abs_per_usd"]),
            }
    return out


def load_gamma_by_strike(path: Path) -> dict[str, list[dict[str, float]]]:
    out: dict[str, list[dict[str, float]]] = {}
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            out.setdefault(row["date"], []).append(
                {
                    "strike": float(row["strike"]),
                    "call_gamma": float(row["call_gamma_abs_per_usd"]),
                    "put_gamma": float(row["put_gamma_abs_per_usd"]),
                    "total_gamma": float(row["total_gamma_abs_per_usd"]),
                    "dist": float(row["dist_spot_to_strike"]),
                }
            )
    for rows in out.values():
        rows.sort(key=lambda r: r["strike"])
    return out


def nearest_strike(rows: list[dict[str, float]], price: float) -> dict[str, float] | None:
    if not rows:
        return None
    strikes = [row["strike"] for row in rows]
    idx = bisect_left(strikes, price)
    candidates = []
    if idx < len(rows):
        candidates.append(rows[idx])
    if idx > 0:
        candidates.append(rows[idx - 1])
    return min(candidates, key=lambda r: abs(r["strike"] - price)) if candidates else None


def build_pair_features(pair: str, split: str, mode: str, reopt_dir: Path, tick_dir: Path, data_dir: Path) -> list[dict[str, object]]:
    trades_path = reopt_dir / f"{pair.lower()}_sl_tp_{mode}_{split}_best_trades.csv"
    hourly_path = tick_dir / f"{pair.lower()}_1h.csv"
    gamma_daily_path = data_dir / f"{pair.lower()}_gamma_proxy_daily_call_put.csv"
    gamma_strike_path = data_dir / f"{pair.lower()}_gamma_proxy_by_strike_call_put.csv"

    trades = list(csv.DictReader(trades_path.open(newline="", encoding="utf-8")))
    daily_bars = build_daily_bars(load_hourly_csv(hourly_path))
    gamma_daily = load_gamma_daily(gamma_daily_path)
    gamma_by_strike = load_gamma_by_strike(gamma_strike_path)

    bar_index = {bar.day.isoformat(): i for i, bar in enumerate(daily_bars)}
    closes = [bar.close for bar in daily_bars]
    pip_size = pair_pip_size(pair)

    out: list[dict[str, object]] = []
    for trade in trades:
        signal_date = trade["signal_date"]
        idx = bar_index.get(signal_date)
        if idx is None or idx < 200:
            # Research dataset needs a reasonable lookback to form momentum, MA and vol features.
            continue

        bar = daily_bars[idx]
        entry_price = float(trade["entry_price"])
        pnl_pips = float(trade["pnl_pips"])
        prev_close = daily_bars[idx - 1].close
        ma20 = rolling_mean(closes, idx, 20)
        ma50 = rolling_mean(closes, idx, 50)
        ma100 = rolling_mean(closes, idx, 100)
        ma200 = rolling_mean(closes, idx, 200)
        ma20_prev = rolling_mean(closes, idx - 1, 20)
        atr5 = rolling_atr(daily_bars, idx, 5)
        atr14 = rolling_atr(daily_bars, idx, 14)
        atr20 = rolling_atr(daily_bars, idx, 20)
        rv5 = rolling_realized_vol(closes, idx, 5)
        rv20 = rolling_realized_vol(closes, idx, 20)
        gamma_day = gamma_daily.get(signal_date, {})
        strike_rows = gamma_by_strike.get(signal_date, [])
        nearest = nearest_strike(strike_rows, entry_price)
        total_gamma = gamma_day.get("total_gamma", 0.0)
        max_strike_gamma = max((row["total_gamma"] for row in strike_rows), default=0.0)

        # Entry price on a filled trade is the realized level. This gives a stable measure of how far
        # from prior close the signal sat when the trade was actually taken.
        distance_to_entry_pips = abs(entry_price - prev_close) / pip_size

        feature_row = {
            "pair": pair,
            "split": split,
            "mode": mode,
            "signal_date": signal_date,
            "side": trade["side"],
            "entry_price": entry_price,
            "exit_price": float(trade["exit_price"]),
            "hold_hours": float(trade["hold_hours"]),
            "pnl_pips": pnl_pips,
            "win_flag": 1 if pnl_pips > 0 else 0,
            "prev_close": prev_close,
            "signal_close": bar.close,
            "distance_to_entry_pips": distance_to_entry_pips,
            "distance_to_entry_atr14": (distance_to_entry_pips * pip_size / atr14) if atr14 else None,
            "ret_1d": (closes[idx] / closes[idx - 1] - 1.0),
            "ret_3d": (closes[idx] / closes[idx - 3] - 1.0),
            "ret_5d": (closes[idx] / closes[idx - 5] - 1.0),
            "ma20_slope": (ma20 - ma20_prev) if ma20 is not None and ma20_prev is not None else None,
            "dist_to_ma20": (bar.close - ma20) if ma20 is not None else None,
            "dist_to_ma50": (bar.close - ma50) if ma50 is not None else None,
            "dist_to_ma100": (bar.close - ma100) if ma100 is not None else None,
            "dist_to_ma200": (bar.close - ma200) if ma200 is not None else None,
            "atr5": atr5,
            "atr14": atr14,
            "atr20": atr20,
            "rv5": rv5,
            "rv20": rv20,
            "active_call_notional": gamma_day.get("active_call_notional"),
            "active_put_notional": gamma_day.get("active_put_notional"),
            "call_gamma_total": gamma_day.get("call_gamma"),
            "put_gamma_total": gamma_day.get("put_gamma"),
            "total_gamma": total_gamma,
            "nearest_strike": nearest["strike"] if nearest else None,
            "nearest_strike_distance": abs(nearest["strike"] - entry_price) if nearest else None,
            "nearest_strike_gamma": nearest["total_gamma"] if nearest else None,
            "top_strike_gamma_share": (max_strike_gamma / total_gamma) if total_gamma else None,
        }
        out.append(feature_row)
    return out


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
      raise ValueError(f"No rows to write for {path}")
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def main() -> int:
    args = parse_args()
    pairs = [pair.strip().upper() for pair in args.pairs.split(",") if pair.strip()]
    reopt_dir = Path(args.reopt_dir)
    tick_dir = Path(args.tick_dir)
    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)

    all_rows: list[dict[str, object]] = []
    for pair in pairs:
        rows: list[dict[str, object]] = []
        for split in ["train", "test"]:
            rows.extend(build_pair_features(pair, split, args.mode, reopt_dir, tick_dir, data_dir))
        write_csv(output_dir / f"{pair.lower()}_trade_features.csv", rows)
        all_rows.extend(rows)

    write_csv(output_dir / "all_pairs_trade_features.csv", all_rows)
    print(output_dir / "all_pairs_trade_features.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
