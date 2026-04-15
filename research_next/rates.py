from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass
class RatePoint:
    tenor_days: int
    domestic_rate: float
    foreign_rate: float
    source: str


def load_tenor_rates(path: Path, pair: str) -> dict[date, list[RatePoint]]:
    out: dict[date, list[RatePoint]] = {}
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("pair") != pair:
                continue
            day = date.fromisoformat(row["date"])
            out.setdefault(day, []).append(
                RatePoint(
                    tenor_days=int(row["tenor_days"]),
                    domestic_rate=float(row["domestic_rate"]),
                    foreign_rate=float(row["foreign_rate"]),
                    source=row.get("source", ""),
                )
            )
    for points in out.values():
        points.sort(key=lambda x: x.tenor_days)
    return out


def interpolate_rate(
    points: list[RatePoint],
    target_tenor_days: int,
) -> tuple[float, float, str] | None:
    if not points:
        return None
    if target_tenor_days <= points[0].tenor_days:
        p = points[0]
        return p.domestic_rate, p.foreign_rate, f"{p.source}|clamped_low"
    if target_tenor_days >= points[-1].tenor_days:
        p = points[-1]
        return p.domestic_rate, p.foreign_rate, f"{p.source}|clamped_high"

    for left, right in zip(points, points[1:]):
        if left.tenor_days <= target_tenor_days <= right.tenor_days:
            if left.tenor_days == right.tenor_days:
                return left.domestic_rate, left.foreign_rate, f"{left.source}|exact"
            weight = (target_tenor_days - left.tenor_days) / (right.tenor_days - left.tenor_days)
            rd = left.domestic_rate + weight * (right.domestic_rate - left.domestic_rate)
            rf = left.foreign_rate + weight * (right.foreign_rate - left.foreign_rate)
            return rd, rf, f"{left.source}|interp"
    return None


def resolve_rates_for_trade(
    rates_by_date: dict[date, list[RatePoint]],
    trade_date: date,
    tenor_days: int,
) -> tuple[float, float, str] | None:
    if trade_date in rates_by_date:
        resolved = interpolate_rate(rates_by_date[trade_date], tenor_days)
        if resolved is not None:
            return resolved

    prior_dates = [d for d in rates_by_date if d <= trade_date]
    if not prior_dates:
        return None
    nearest = max(prior_dates)
    resolved = interpolate_rate(rates_by_date[nearest], tenor_days)
    if resolved is None:
        return None
    rd, rf, source = resolved
    return rd, rf, f"{source}|prior_date={nearest.isoformat()}"
