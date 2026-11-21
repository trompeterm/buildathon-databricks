#!/usr/bin/env python3
"""Exact, order-insensitive check of pipeline output against expected CSV tables.

  check.py OUT EXPECTED            one output folder vs one folder holding user_daily.csv / campaign_daily.csv
  check.py --fixtures OUTROOT FIXROOT   OUTROOT/<case>/ vs FIXROOT/<case>/expected/ for every case
"""
import sys
from pathlib import Path

import polars as pl

SCHEMAS = {
    "user_daily": {
        "user_id": pl.Utf8, "day": pl.Date, "sessions": pl.Int64, "events": pl.Int64,
        "revenue_cents": pl.Int64, "top_category": pl.Utf8,
    },
    "campaign_daily": {
        "campaign_id": pl.Utf8, "channel": pl.Utf8, "day": pl.Date, "sessions": pl.Int64,
        "purchasing_sessions": pl.Int64, "revenue_cents": pl.Int64, "assisted_sessions": pl.Int64,
    },
}


def load_expected(path, schema):
    df = pl.read_csv(path, infer_schema=False)
    df = df.with_columns(pl.col("day").str.to_date("%Y-%m-%d"))
    for k, v in schema.items():
        df = df.with_columns(pl.col(k).cast(v))
    return df.select(list(schema))


def load_actual(folder, schema):
    files = sorted(p for p in folder.glob("*.parquet") if not p.name.startswith((".", "_")) and p.is_file())
    if not files:
        raise SystemExit(f"no parquet in {folder}")
    dfs = [pl.read_parquet(f) for f in files]
    df = pl.concat(dfs)
    if set(df.columns) != set(schema):
        raise SystemExit(f"{folder}: columns {df.columns} != {list(schema)}")
    if df.schema["day"] != pl.Date:
        raise SystemExit(f"{folder}: day is {df.schema['day']}")
    return df.select([pl.col(k).cast(v) for k, v in schema.items()])


def compare(out, exp, verbose=True):
    ok = True
    for table, schema in SCHEMAS.items():
        a = load_actual(out / table, schema)
        b = load_expected(exp / f"{table}.csv", schema)
        only_a = a.join(b, on=list(schema), how="anti", nulls_equal=True)
        only_b = b.join(a, on=list(schema), how="anti", nulls_equal=True)
        keys = [c for c in ("user_id", "campaign_id", "day") if c in schema]
        dup = a.height - a.n_unique(subset=keys)
        good = not (only_a.height or only_b.height or dup or a.height != b.height)
        if verbose or not good:
            print(f"  {table}: rows {a.height} vs {b.height}; only-out {only_a.height}; only-exp {only_b.height}; dup {dup}")
        if not good:
            ok = False
            with pl.Config(tbl_cols=-1, tbl_width_chars=200):
                print(only_a.sort(keys).head(8))
                print(only_b.sort(keys).head(8))
    return ok


def main():
    args = sys.argv[1:]
    if args and args[0] == "--fixtures":
        outroot, fixroot = Path(args[1]), Path(args[2])
        cases = sorted(p.name for p in fixroot.iterdir() if (p / "expected").is_dir())
        bad = []
        for c in cases:
            print(c)
            if not compare(outroot / c, fixroot / c / "expected", verbose=False):
                bad.append(c)
        print(f"{len(cases) - len(bad)}/{len(cases)} fixtures pass", "FAILED: " + " ".join(bad) if bad else "")
        return 1 if bad else 0
    ok = compare(Path(args[0]), Path(args[1]))
    print("MATCH" if ok else "DIFFER")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
