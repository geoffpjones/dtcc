#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


PAIRS = ["EURUSD", "GBPUSD", "AUDUSD", "USDCAD", "USDJPY"]


@dataclass
class DailyRow:
    pair: str
    effective_signal_date: str
    selected_signal: str
    selected_buy_limit: str
    selected_sell_limit: str
    selected_exit_mode: str
    selected_tp_pips: str
    selected_sl_pips: str
    selected_trail_pips: str
    selected_buy_tp_level: str
    selected_buy_sl_level: str
    selected_sell_tp_level: str
    selected_sell_sl_level: str
    selected_notes: str


@dataclass
class MtdStats:
    pair: str
    trade_count: int
    wins: int
    losses: int
    win_rate: float
    cumulative_pnl_pips: float


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate one-page PDF daily FX options report.")
    p.add_argument("--report-csv", required=True)
    p.add_argument("--reopt-dir", required=True)
    p.add_argument("--report-date", required=True)
    p.add_argument("--output-pdf", required=True)
    return p.parse_args()


def load_daily_rows(path: Path) -> list[DailyRow]:
    out: list[DailyRow] = []
    with path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            out.append(
                DailyRow(
                    pair=row["pair"],
                    effective_signal_date=row["effective_signal_date"],
                    selected_signal=row["selected_signal"],
                    selected_buy_limit=row["selected_buy_limit"],
                    selected_sell_limit=row["selected_sell_limit"],
                    selected_exit_mode=row["selected_exit_mode"],
                    selected_tp_pips=row["selected_tp_pips"],
                    selected_sl_pips=row["selected_sl_pips"],
                    selected_trail_pips=row["selected_trail_pips"],
                    selected_buy_tp_level=row["selected_buy_tp_level"],
                    selected_buy_sl_level=row["selected_buy_sl_level"],
                    selected_sell_tp_level=row["selected_sell_tp_level"],
                    selected_sell_sl_level=row["selected_sell_sl_level"],
                    selected_notes=row["selected_notes"],
                )
            )
    out.sort(key=lambda x: PAIRS.index(x.pair) if x.pair in PAIRS else x.pair)
    return out


def load_mtd_stats(reopt_dir: Path, report_date: date) -> list[MtdStats]:
    month_prefix = report_date.strftime("%Y-%m")
    out: list[MtdStats] = []
    for pair in PAIRS:
        trades_path = reopt_dir / f"{pair.lower()}_sl_tp_trailing_test_best_trades.csv"
        trades = list(csv.DictReader(trades_path.open(newline="", encoding="utf-8")))
        mtd = [r for r in trades if r["signal_date"].startswith(month_prefix) and r["signal_date"] <= report_date.isoformat()]
        wins = sum(1 for r in mtd if float(r["pnl_pips"]) > 0)
        losses = sum(1 for r in mtd if float(r["pnl_pips"]) < 0)
        total = sum(float(r["pnl_pips"]) for r in mtd)
        count = len(mtd)
        out.append(
            MtdStats(
                pair=pair,
                trade_count=count,
                wins=wins,
                losses=losses,
                win_rate=(wins / count) if count else 0.0,
                cumulative_pnl_pips=total,
            )
        )
    return out


def shorten_note(note: str) -> str:
    if not note:
        return ""
    note = note.replace("signal=", "")
    if len(note) > 60:
        return note[:57] + "..."
    return note


def price_decimals(pair: str) -> int:
    return 2 if pair.endswith("JPY") else 4


def format_price(pair: str, value: str) -> str:
    if not value:
        return ""
    try:
        return f"{float(value):.{price_decimals(pair)}f}"
    except ValueError:
        return value


def main() -> int:
    args = parse_args()
    report_date = date.fromisoformat(args.report_date)
    report_rows = load_daily_rows(Path(args.report_csv))
    mtd_stats = {row.pair: row for row in load_mtd_stats(Path(args.reopt_dir), report_date)}

    output = Path(args.output_pdf)
    output.parent.mkdir(parents=True, exist_ok=True)

    fig = plt.figure(figsize=(16, 9))
    fig.patch.set_facecolor("white")

    title = f"FX Options Daily Report - {report_date.isoformat()}"
    subtitle = "Selected limit levels, optimized exits, and month-to-date backtest performance"
    fig.text(0.03, 0.965, title, fontsize=20, fontweight="bold", ha="left", va="top")
    fig.text(0.03, 0.935, subtitle, fontsize=11, ha="left", va="top", color="#444444")

    levels_ax = fig.add_axes([0.03, 0.48, 0.94, 0.38])
    levels_ax.axis("off")
    levels_cols = [
        "Pair", "Eff Date", "Signal", "Buy", "Sell",
        "Exit", "TP", "SL", "Trail",
        "Buy TP", "Buy SL", "Sell TP", "Sell SL",
    ]
    levels_data = []
    for row in report_rows:
        # PDF output is presentation-focused: cap displayed precision so wide float strings
        # from the CSV do not overflow the table while preserving pair-appropriate quoting.
        levels_data.append([
            row.pair,
            row.effective_signal_date,
            row.selected_signal,
            format_price(row.pair, row.selected_buy_limit),
            format_price(row.pair, row.selected_sell_limit),
            row.selected_exit_mode,
            row.selected_tp_pips,
            row.selected_sl_pips,
            row.selected_trail_pips,
            format_price(row.pair, row.selected_buy_tp_level),
            format_price(row.pair, row.selected_buy_sl_level),
            format_price(row.pair, row.selected_sell_tp_level),
            format_price(row.pair, row.selected_sell_sl_level),
        ])
    table1 = levels_ax.table(cellText=levels_data, colLabels=levels_cols, loc="center", cellLoc="center")
    table1.auto_set_font_size(False)
    table1.set_fontsize(9)
    table1.scale(1, 1.5)
    for (r, c), cell in table1.get_celld().items():
        if r == 0:
            cell.set_facecolor("#d9e8f5")
            cell.set_text_props(weight="bold")
        elif r % 2 == 1:
            cell.set_facecolor("#f7f7f7")

    mtd_ax = fig.add_axes([0.03, 0.18, 0.62, 0.22])
    mtd_ax.axis("off")
    mtd_cols = ["Pair", "MTD Trades", "Wins", "Losses", "Win Rate", "MTD PnL (pips)"]
    mtd_data = []
    for pair in PAIRS:
        s = mtd_stats[pair]
        mtd_data.append([
            s.pair,
            str(s.trade_count),
            str(s.wins),
            str(s.losses),
            f"{s.win_rate:.1%}",
            f"{s.cumulative_pnl_pips:.1f}",
        ])
    table2 = mtd_ax.table(cellText=mtd_data, colLabels=mtd_cols, loc="center", cellLoc="center")
    table2.auto_set_font_size(False)
    table2.set_fontsize(10)
    table2.scale(1, 1.6)
    for (r, c), cell in table2.get_celld().items():
        if r == 0:
            cell.set_facecolor("#f5d7a1")
            cell.set_text_props(weight="bold")
        elif r % 2 == 1:
            cell.set_facecolor("#fcfcfc")

    notes_ax = fig.add_axes([0.68, 0.16, 0.29, 0.24])
    notes_ax.axis("off")
    note_lines = ["Notes"]
    for row in report_rows:
        note_lines.append(f"{row.pair}: {shorten_note(row.selected_notes)}")
    notes_ax.text(
        0.0, 1.0, "\n".join(note_lines),
        ha="left", va="top", fontsize=10,
        family="monospace",
        bbox=dict(facecolor="#fff8e8", edgecolor="#d0c7b8", boxstyle="round,pad=0.5"),
    )

    footer = f"Source files: {Path(args.report_csv).name} and {Path(args.reopt_dir).name}/*_sl_tp_trailing_test_best_trades.csv"
    fig.text(0.03, 0.04, footer, fontsize=9, color="#666666", ha="left")

    with PdfPages(output) as pdf:
        pdf.savefig(fig)
    plt.close(fig)
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
