from __future__ import annotations

import csv
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import pstdev


PAIRS = ["EURUSD", "GBPUSD", "AUDUSD", "USDCAD", "USDJPY"]


def parse_iso_date(value: str) -> date:
    return date.fromisoformat(value)


def parse_ts_utc(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def pair_pip_size(pair: str) -> float:
    return 0.01 if pair.endswith("JPY") else 0.0001


@dataclass
class DailyBar:
    day: date
    open: float
    high: float
    low: float
    close: float


def load_hourly_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def build_daily_bars(hourly_rows: list[dict[str, str]]) -> list[DailyBar]:
    grouped: dict[date, list[dict[str, str]]] = defaultdict(list)
    for row in hourly_rows:
        grouped[parse_ts_utc(row["timestamp_utc"]).date()].append(row)

    out: list[DailyBar] = []
    for day in sorted(grouped):
        rows = sorted(grouped[day], key=lambda r: r["timestamp_utc"])
        out.append(
            DailyBar(
                day=day,
                open=float(rows[0]["open"]),
                high=max(float(r["high"]) for r in rows),
                low=min(float(r["low"]) for r in rows),
                close=float(rows[-1]["close"]),
            )
        )
    return out


def rolling_mean(values: list[float], end_idx: int, window: int) -> float | None:
    start = end_idx - window + 1
    if start < 0:
        return None
    sample = values[start : end_idx + 1]
    return sum(sample) / window


def rolling_realized_vol(close_values: list[float], end_idx: int, window: int) -> float | None:
    start = end_idx - window
    if start < 0:
        return None
    rets: list[float] = []
    for i in range(start + 1, end_idx + 1):
        prev_close = close_values[i - 1]
        curr_close = close_values[i]
        if prev_close <= 0:
            return None
        rets.append(math.log(curr_close / prev_close))
    if len(rets) < 2:
        return None
    return pstdev(rets) * math.sqrt(252.0)


def rolling_atr(bars: list[DailyBar], end_idx: int, window: int) -> float | None:
    start = end_idx - window + 1
    if start < 1:
        return None
    true_ranges: list[float] = []
    for i in range(start, end_idx + 1):
        prev_close = bars[i - 1].close
        curr = bars[i]
        true_ranges.append(max(curr.high - curr.low, abs(curr.high - prev_close), abs(curr.low - prev_close)))
    if not true_ranges:
        return None
    return sum(true_ranges) / len(true_ranges)


def safe_float(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def quantile_bucket(value: float, cut_points: list[float]) -> int:
    for i, cut in enumerate(cut_points, start=1):
        if value <= cut:
            return i
    return len(cut_points) + 1

