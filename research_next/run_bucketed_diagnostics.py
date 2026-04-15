#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

from common import PAIRS, quantile_bucket, safe_float


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
    p = argparse.ArgumentParser(description="Run bucketed diagnostics over isolated trade-level feature tables.")
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


def summarize_bucket(rows: list[dict[str, str]], feature: str, bucket_count: int) -> list[dict[str, object]]:
    values = [safe_float(row[feature]) for row in rows]
    values = [value for value in values if value is not None]
    if len(values) < bucket_count:
        return []
    cuts = percentile_cut_points(values, bucket_count)
    buckets: dict[int, list[dict[str, str]]] = {i: [] for i in range(1, bucket_count + 1)}

    for row in rows:
        value = safe_float(row[feature])
        if value is None:
            continue
        bucket = quantile_bucket(value, cuts)
        buckets[bucket].append(row)

    out: list[dict[str, object]] = []
    for bucket, bucket_rows in buckets.items():
        if not bucket_rows:
            continue
        pnls = [float(row["pnl_pips"]) for row in bucket_rows]
        wins = sum(1 for pnl in pnls if pnl > 0)
        losses = sum(1 for pnl in pnls if pnl < 0)
        out.append(
            {
                "feature": feature,
                "bucket": bucket,
                "trades": len(bucket_rows),
                "wins": wins,
                "losses": losses,
                "win_rate": wins / len(bucket_rows),
                "avg_pnl_pips": sum(pnls) / len(bucket_rows),
                "total_pnl_pips": sum(pnls),
                "min_value": min(float(row[feature]) for row in bucket_rows),
                "max_value": max(float(row[feature]) for row in bucket_rows),
            }
        )
    return out


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        if rows:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        else:
            f.write("")


def main() -> int:
    args = parse_args()
    pairs = [pair.strip().upper() for pair in args.pairs.split(",") if pair.strip()]
    features = [feature.strip() for feature in args.features.split(",") if feature.strip()]
    output_dir = Path(args.output_dir)

    with Path(args.features_csv).open(newline="", encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    for pair in pairs:
        pair_rows = [row for row in all_rows if row["pair"] == pair]
        summaries: list[dict[str, object]] = []
        for feature in features:
            summaries.extend(summarize_bucket(pair_rows, feature, args.bucket_count))
        write_csv(output_dir / f"{pair.lower()}_bucketed_feature_summary.csv", summaries)

        ranked = sorted(
            summaries,
            key=lambda row: (row["avg_pnl_pips"], row["win_rate"], row["trades"]),
            reverse=True,
        )
        write_csv(output_dir / f"{pair.lower()}_top_feature_edges.csv", ranked[:25])
        print(output_dir / f"{pair.lower()}_bucketed_feature_summary.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
