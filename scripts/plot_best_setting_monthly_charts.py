#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import matplotlib.dates as mdates
import matplotlib.pyplot as plt


@dataclass
class HourBar:
    ts: datetime
    close: float


@dataclass
class Trade:
    signal_date: str
    side: str
    entry_ts: datetime
    entry_price: float
    exit_ts: datetime
    exit_price: float
    pnl_pips: float
    exit_reason: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Plot monthly charts for best-setting backtest trades.")
    p.add_argument("--pair", required=True, help="Pair code like eurusd")
    p.add_argument("--hourly", required=True)
    p.add_argument("--train-trades", required=True)
    p.add_argument("--test-trades", required=True)
    p.add_argument("--outdir", required=True)
    return p.parse_args()


def parse_ts(v: str) -> datetime:
    ts = datetime.fromisoformat(v)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def load_hourly(path: Path) -> tuple[list[HourBar], dict[datetime, float]]:
    hourly: list[HourBar] = []
    close_by_ts: dict[datetime, float] = {}
    with path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            ts = parse_ts(row["timestamp_utc"])
            close = float(row["close"])
            hourly.append(HourBar(ts=ts, close=close))
            close_by_ts[ts] = close
    hourly.sort(key=lambda x: x.ts)
    return hourly, close_by_ts


def load_trades(paths: Iterable[Path]) -> list[Trade]:
    out: list[Trade] = []
    for path in paths:
        with path.open(newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                out.append(
                    Trade(
                        signal_date=row["signal_date"],
                        side=row["side"],
                        entry_ts=parse_ts(row["entry_ts_utc"]),
                        entry_price=float(row["entry_price"]),
                        exit_ts=parse_ts(row["exit_ts_utc"]),
                        exit_price=float(row["exit_price"]),
                        pnl_pips=float(row["pnl_pips"]),
                        exit_reason=row["exit_reason"],
                    )
                )
    out.sort(key=lambda t: (t.entry_ts, t.exit_ts))
    return out


def month_iter(hourly: list[HourBar], trades: list[Trade]) -> list[tuple[int, int]]:
    if trades:
        start = min(t.entry_ts for t in trades)
        end = max(t.exit_ts for t in trades)
        months = sorted({
            (b.ts.year, b.ts.month)
            for b in hourly
            if start.replace(day=1, hour=0, minute=0, second=0, microsecond=0) <= b.ts <= end
        })
        return months
    months = sorted({(b.ts.year, b.ts.month) for b in hourly})
    return months


def main() -> int:
    args = parse_args()
    pair = args.pair.lower()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    hourly, close_by_ts = load_hourly(Path(args.hourly))
    trades = load_trades([Path(args.train_trades), Path(args.test_trades)])
    cumulative_by_exit: list[tuple[datetime, float]] = []
    cumulative = 0.0
    for trade in trades:
        cumulative += trade.pnl_pips
        cumulative_by_exit.append((trade.exit_ts, cumulative))

    shown_months = month_iter(hourly, trades)
    for year, month in shown_months:
        start = datetime(year, month, 1, tzinfo=timezone.utc)
        end = datetime(year + (month == 12), (month % 12) + 1, 1, tzinfo=timezone.utc)
        month_bars = [b for b in hourly if start <= b.ts < end]
        if not month_bars:
            continue

        fig, ax = plt.subplots(figsize=(15, 7), constrained_layout=True)
        ax2 = ax.twinx()
        x = [b.ts for b in month_bars]
        y = [b.close for b in month_bars]
        ax.plot(x, y, color="#808080", lw=1.0, alpha=0.6, label=f"{pair.upper()} hourly close")

        cum_x = [ts for ts, _ in cumulative_by_exit if start <= ts < end]
        if cum_x:
            cum_y = [cum for ts, cum in cumulative_by_exit if start <= ts < end]
            ax2.plot(cum_x, cum_y, color="orange", lw=1.8, alpha=0.9, label="Cumulative PnL (pips)")
            ax2.set_ylabel("Cumulative PnL (pips)", color="orange")
            ax2.tick_params(axis="y", colors="orange")

        shown = set()
        for trade in trades:
            if trade.entry_ts >= end or trade.exit_ts < start:
                continue

            segx = [b.ts for b in month_bars if trade.entry_ts <= b.ts <= trade.exit_ts]
            if segx:
                segy = [close_by_ts[t] for t in segx]
                if trade.side == "long":
                    ax.plot(segx, segy, color="blue", lw=2.0, alpha=0.95, label="Long period" if "long" not in shown else None)
                    shown.add("long")
                else:
                    ax.plot(segx, segy, color="green", lw=2.0, alpha=0.95, label="Short period" if "short" not in shown else None)
                    shown.add("short")

            if start <= trade.entry_ts < end:
                marker = "^" if trade.side == "long" else "v"
                color = "blue" if trade.side == "long" else "green"
                label = "Buy fill" if trade.side == "long" else "Sell fill"
                key = "buy_fill" if trade.side == "long" else "sell_fill"
                ax.scatter(trade.entry_ts, trade.entry_price, marker=marker, color=color, s=52, zorder=6,
                           label=label if key not in shown else None)
                shown.add(key)

            if start <= trade.exit_ts < end:
                if trade.pnl_pips > 0:
                    color = "black"
                    label = "Exit profit"
                    key = "exit_profit"
                elif trade.pnl_pips < 0:
                    color = "red"
                    label = "Exit loss"
                    key = "exit_loss"
                else:
                    color = "#555555"
                    label = "Exit flat"
                    key = "exit_flat"
                ax.scatter(trade.exit_ts, trade.exit_price, marker="o", color=color, edgecolors="white",
                           linewidths=0.5, s=42, zorder=7, label=label if key not in shown else None)
                shown.add(key)

        ax.set_title(f"{pair.upper()} best trailing setting - {year}-{month:02d}")
        ax.set_ylabel(pair.upper())
        ax.set_xlim(start, end)
        ax.grid(True, alpha=0.25)
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
        lines1, labels1 = ax.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax.legend(lines1 + lines2, labels1 + labels2, loc="upper left", ncol=2)

        out = outdir / f"{pair}_{year}_{month:02d}_best_setting_walkforward.png"
        fig.savefig(out, dpi=170)
        plt.close(fig)
        print(out)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
