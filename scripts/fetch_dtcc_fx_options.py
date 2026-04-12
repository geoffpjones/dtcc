#!/usr/bin/env python3
"""Download historical DTCC public cumulative files and extract FX options rows.

Usage:
  python3 scripts/fetch_dtcc_fx_options.py \
    --start-date 2026-03-01 \
    --end-date 2026-04-11 \
    --output data/cftc_fx_options_2026-03-01_2026-04-11.csv
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import re
import sys
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

BASE = "https://pddata.dtcc.com/ppd/api"
LIST_URL = BASE + "/cumulative/{regime}/{asset}"
DOWNLOAD_URL = BASE + "/report/cumulative/{prefix}/{filename}"
DATE_RE = re.compile(r"_(\d{4})_(\d{2})_(\d{2})\.zip$")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch DTCC historical FX options rows from public cumulative reports.")
    p.add_argument("--regime", default="CFTC", choices=["CFTC", "SEC", "CA"], help="Reporting regime")
    p.add_argument("--asset", default="FX", choices=["FX", "IR", "CR", "EQ", "CO"], help="Asset code")
    p.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--output", required=True, help="Options-only output CSV path")
    p.add_argument("--full-output", help="Full unfiltered merged CSV path (default: <output>_full.csv)")
    p.add_argument("--raw-dir", help="Folder for raw ZIP/CSV files (default: <output_dir>/raw/<regime>_<asset>)")
    p.add_argument("--skip-raw-save", action="store_true", help="Do not store raw ZIP/CSV files")
    p.add_argument("--timeout", type=int, default=60, help="HTTP timeout seconds")
    return p.parse_args()


def fetch_json(url: str, timeout: int) -> list[dict]:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r)


def file_date(filename: str) -> dt.date | None:
    m = DATE_RE.search(filename)
    if not m:
        return None
    y, mm, d = map(int, m.groups())
    return dt.date(y, mm, d)


def is_option_row(row: dict[str, str]) -> bool:
    embedded = (row.get("Embedded Option type") or "").strip()
    otype = (row.get("Option Type") or "").strip()
    ostyle = (row.get("Option Style") or "").strip()
    pname = (row.get("Product name") or "").upper()
    upi_fisn = (row.get("UPI FISN") or "").upper()
    return bool(embedded or otype or ostyle or "OPTION" in pname or "OPTION" in upi_fisn)


def download_zip_bytes(filename: str, timeout: int) -> bytes:
    prefix = filename.split("_", 1)[0].lower()
    url = DOWNLOAD_URL.format(prefix=prefix, filename=filename)
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read()


def extract_csv_from_zip(zbytes: bytes) -> tuple[str, bytes]:
    with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
        names = zf.namelist()
        if not names:
            raise ValueError("zip has no members")
        csv_name = names[0]
        return csv_name, zf.read(csv_name)


def derive_full_output_path(options_output: Path) -> Path:
    return options_output.with_name(f"{options_output.stem}_full{options_output.suffix}")


def derive_raw_dir(options_output: Path, regime: str, asset: str) -> Path:
    return options_output.parent / "raw" / f"{regime.lower()}_{asset.lower()}"


def main() -> int:
    args = parse_args()

    start = dt.date.fromisoformat(args.start_date)
    end = dt.date.fromisoformat(args.end_date)
    if start > end:
        raise ValueError("start-date must be <= end-date")

    options_out = Path(args.output)
    options_out.parent.mkdir(parents=True, exist_ok=True)
    full_out = Path(args.full_output) if args.full_output else derive_full_output_path(options_out)
    full_out.parent.mkdir(parents=True, exist_ok=True)

    save_raw = not args.skip_raw_save
    raw_dir = Path(args.raw_dir) if args.raw_dir else derive_raw_dir(options_out, args.regime, args.asset)
    if save_raw:
        raw_dir.mkdir(parents=True, exist_ok=True)

    list_url = LIST_URL.format(regime=args.regime, asset=args.asset)
    try:
        files = fetch_json(list_url, timeout=args.timeout)
    except urllib.error.HTTPError as e:
        print(f"Failed to fetch list: HTTP {e.code} {e.reason}", file=sys.stderr)
        return 2

    candidates: list[tuple[dt.date, str]] = []
    for item in files:
        fn = item.get("fileName", "")
        d = file_date(fn)
        if d is not None and start <= d <= end:
            candidates.append((d, fn))

    candidates.sort()
    if not candidates:
        print("No files in requested date range.", file=sys.stderr)
        return 1

    total_rows = 0
    total_option_rows = 0
    parsed_files = 0

    full_writer: csv.DictWriter | None = None
    options_writer: csv.DictWriter | None = None

    with full_out.open("w", newline="", encoding="utf-8") as full_f, options_out.open(
        "w", newline="", encoding="utf-8"
    ) as options_f:
        for i, (d, fn) in enumerate(candidates, start=1):
            print(f"[{i}/{len(candidates)}] {fn}")
            try:
                zbytes = download_zip_bytes(fn, timeout=args.timeout)
                csv_name, csv_bytes = extract_csv_from_zip(zbytes)
            except Exception as e:  # noqa: BLE001
                print(f"  skip (download/parse error): {e}", file=sys.stderr)
                continue

            if save_raw:
                (raw_dir / fn).write_bytes(zbytes)
                (raw_dir / csv_name).write_bytes(csv_bytes)

            text = io.StringIO(csv_bytes.decode("utf-8"))
            reader = csv.DictReader(text)

            if reader.fieldnames is None:
                print("  skip (no CSV header)", file=sys.stderr)
                continue

            parsed_files += 1
            out_fields = list(reader.fieldnames) + ["source_file", "source_date"]
            if full_writer is None:
                full_writer = csv.DictWriter(full_f, fieldnames=out_fields, extrasaction="ignore")
                full_writer.writeheader()
            if options_writer is None:
                options_writer = csv.DictWriter(options_f, fieldnames=out_fields, extrasaction="ignore")
                options_writer.writeheader()

            for row in reader:
                row["source_file"] = fn
                row["source_date"] = d.isoformat()
                full_writer.writerow(row)
                total_rows += 1
                if is_option_row(row):
                    options_writer.writerow(row)
                    total_option_rows += 1

    if parsed_files == 0:
        print("No files could be parsed.", file=sys.stderr)
        return 1

    print("----")
    print(f"Files selected: {len(candidates)}")
    print(f"Files parsed: {parsed_files}")
    print(f"Total rows written (full): {total_rows}")
    print(f"Options rows written: {total_option_rows}")
    print(f"Full output: {full_out}")
    print(f"Options output: {options_out}")
    if save_raw:
        print(f"Raw files: {raw_dir}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
