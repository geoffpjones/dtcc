#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

from common import PAIRS, safe_float


DEFAULT_FEATURES = [
    "distance_to_entry_pips",
    "distance_to_entry_atr14",
    "ret_1d",
    "ret_3d",
    "ret_5d",
    "ma20_slope",
    "dist_to_ma20",
    "dist_to_ma50",
    "dist_to_ma100",
    "dist_to_ma200",
    "atr14",
    "atr20",
    "rv5",
    "rv20",
    "top_strike_gamma_share",
    "nearest_strike_distance",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Simple walk-forward bucket filter test on isolated feature tables.")
    p.add_argument("--features-csv", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--pairs", default=",".join(PAIRS))
    p.add_argument("--features", default=",".join(DEFAULT_FEATURES))
    p.add_argument("--bucket-count", type=int, default=5)
    return p.parse_args()


def percentile_cut_points(values: list[float], bucket_count: int) -> list[float]:
    values = sorted(values)
    cuts: list[float] = []
    for bucket_idx in range(1, bucket_count):
        pos = int(len(values) * bucket_idx / bucket_count)
        pos = min(max(pos, 0), len(values) - 1)
        cuts.append(values[pos])
    return cuts


def assign_bucket(value: float, cuts: list[float]) -> int:
    for idx, cut in enumerate(cuts, start=1):
        if value <= cut:
            return idx
    return len(cuts) + 1


def summarize(rows: list[dict[str, str]]) -> tuple[int, int, int, float]:
    pnls = [float(r["pnl_pips"]) for r in rows]
    wins = sum(1 for pnl in pnls if pnl > 0)
    losses = sum(1 for pnl in pnls if pnl < 0)
    total = sum(pnls)
    return len(rows), wins, losses, total


def main() -> int:
    args = parse_args()
    pairs = [pair.strip().upper() for pair in args.pairs.split(",") if pair.strip()]
    features = [feature.strip() for feature in args.features.split(",") if feature.strip()]
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with Path(args.features_csv).open(newline="", encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    all_summary_rows: list[dict[str, object]] = []
    for pair in pairs:
        pair_rows = [r for r in all_rows if r["pair"] == pair and r["mode"] == "trailing"]
        train_rows = [r for r in pair_rows if r["split"] == "train"]
        test_rows = [r for r in pair_rows if r["split"] == "test"]
        base_test_count, base_test_wins, base_test_losses, base_test_total = summarize(test_rows)

        pair_results: list[dict[str, object]] = []
        for feature in features:
            train_values = [safe_float(r[feature]) for r in train_rows]
            train_values = [v for v in train_values if v is not None]
            if len(train_values) < args.bucket_count:
                continue
            cuts = percentile_cut_points(train_values, args.bucket_count)

            train_bucket_rows: dict[int, list[dict[str, str]]] = {i: [] for i in range(1, args.bucket_count + 1)}
            for row in train_rows:
                value = safe_float(row[feature])
                if value is None:
                    continue
                train_bucket_rows[assign_bucket(value, cuts)].append(row)

            best_bucket = None
            best_train_total = None
            for bucket, rows in train_bucket_rows.items():
                if not rows:
                    continue
                total = sum(float(r["pnl_pips"]) for r in rows)
                if best_train_total is None or total > best_train_total:
                    best_train_total = total
                    best_bucket = bucket

            if best_bucket is None:
                continue

            filtered_test_rows = []
            for row in test_rows:
                value = safe_float(row[feature])
                if value is None:
                    continue
                if assign_bucket(value, cuts) == best_bucket:
                    filtered_test_rows.append(row)

            filt_count, filt_wins, filt_losses, filt_total = summarize(filtered_test_rows)
            pair_results.append(
                {
                    "pair": pair,
                    "feature": feature,
                    "selected_train_bucket": best_bucket,
                    "train_bucket_total_pnl_pips": round(best_train_total, 6),
                    "baseline_test_trades": base_test_count,
                    "baseline_test_wins": base_test_wins,
                    "baseline_test_losses": base_test_losses,
                    "baseline_test_total_pnl_pips": round(base_test_total, 6),
                    "filtered_test_trades": filt_count,
                    "filtered_test_wins": filt_wins,
                    "filtered_test_losses": filt_losses,
                    "filtered_test_total_pnl_pips": round(filt_total, 6),
                    "test_delta_pnl_pips": round(filt_total - base_test_total, 6),
                    "filtered_test_avg_pnl_pips": round((filt_total / filt_count), 6) if filt_count else None,
                }
            )

        pair_results.sort(key=lambda r: (r["test_delta_pnl_pips"], r["filtered_test_total_pnl_pips"]), reverse=True)
        with (output_dir / f"{pair.lower()}_walkforward_filter_results.csv").open("w", newline="", encoding="utf-8") as f:
            if pair_results:
                w = csv.DictWriter(f, fieldnames=list(pair_results[0].keys()))
                w.writeheader()
                w.writerows(pair_results)
                all_summary_rows.append(pair_results[0])
            else:
                f.write("")

    if all_summary_rows:
        with (output_dir / "all_pairs_top_walkforward_filters.csv").open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(all_summary_rows[0].keys()))
            w.writeheader()
            w.writerows(all_summary_rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
