#!/usr/bin/env python3
"""Cleaning pipeline for a raw GBIF occurrence download.

Reads the raw occurrence parquet (a file, a directory of part-files, or a glob)
and writes ONE cleaned parquet, applying quality filters with DuckDB so it never
loads the whole set into memory. Every rule is optional and any rule whose
column is absent from the data is skipped, so this degrades gracefully.

    python clean.py --src 'occurrence.parquet/*' --out occurrences_clean.parquet

Rules (toggle via flags / the pipeline config):
  * basis-of-record   keep observations, drop specimen / fossil / living-specimen
                      (LIVING_SPECIMEN is GBIF's main captive/zoo/garden signal)
  * establishmentMeans drop MANAGED / CULTIVATED / CAPTIVE  (captive & zoo records)
  * institution block  drop an editable list of zoo/captive institutionCodes
  * coordinates        drop null-island, out-of-range, and over-uncertain points
  * occurrenceStatus   keep PRESENT only
  * year range         keep plausible years
  * null species       optionally drop records not identified to species
"""
from __future__ import annotations

import argparse
import duckdb

# Observation basisOfRecord values we keep; everything else (PRESERVED_SPECIMEN,
# FOSSIL_SPECIMEN, LIVING_SPECIMEN, MATERIAL_SAMPLE, MATERIAL_CITATION) is dropped.
KEEP_BASIS = ("HUMAN_OBSERVATION", "MACHINE_OBSERVATION", "OBSERVATION")
# establishmentMeans values that indicate captive / cultivated / managed records.
CAPTIVE_MEANS = ("MANAGED", "CULTIVATED", "CAPTIVE", "INTRODUCED: MANAGED")


def _sql_list(xs):
    return "(" + ", ".join("'" + str(x).replace("'", "''").upper() + "'" for x in xs) + ")"


def build_rules(cols, args):
    """Return [(label, keep_predicate_sql), ...] for the columns that exist."""
    rules = []
    has = lambda c: c in cols

    if args.basis and has("basisofrecord"):
        rules.append(("non-observation basisOfRecord (specimen/fossil/living)",
                      f"(basisofrecord IS NULL OR upper(basisofrecord) IN {_sql_list(KEEP_BASIS)})"))
    if args.captive and has("establishmentmeans"):
        rules.append(("captive / cultivated (establishmentMeans)",
                      f"(establishmentmeans IS NULL OR upper(establishmentmeans) NOT IN {_sql_list(CAPTIVE_MEANS)})"))
    if args.block_institutions and has("institutioncode"):
        rules.append((f"blocked institutionCode ({len(args.block_institutions)})",
                      f"(institutioncode IS NULL OR upper(institutioncode) NOT IN {_sql_list(args.block_institutions)})"))
    if has("decimallatitude") and has("decimallongitude"):
        rules.append(("coordinates out of range",
                      "(decimallatitude BETWEEN -90 AND 90 AND decimallongitude BETWEEN -180 AND 180)"))
        rules.append(("null-island (0,0)",
                      "NOT (decimallatitude = 0 AND decimallongitude = 0)"))
    if args.max_uncertainty_m is not None and has("coordinateuncertaintyinmeters"):
        rules.append((f"coordinate uncertainty > {args.max_uncertainty_m} m",
                      f"(coordinateuncertaintyinmeters IS NULL OR coordinateuncertaintyinmeters <= {args.max_uncertainty_m})"))
    if args.present_only and has("occurrencestatus"):
        rules.append(("occurrenceStatus != PRESENT",
                      "(occurrencestatus IS NULL OR upper(occurrencestatus) = 'PRESENT')"))
    if has("year"):
        rules.append((f"year outside [{args.min_year}, {args.max_year}]",
                      f"(year IS NULL OR year BETWEEN {args.min_year} AND {args.max_year})"))
    if args.drop_null_species and has("species"):
        rules.append(("species is NULL",
                      "(species IS NOT NULL AND trim(species) <> '')"))
    return rules


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", required=True, help="raw occurrence parquet (file / dir / glob)")
    ap.add_argument("--out", required=True, help="cleaned parquet to write")
    ap.add_argument("--no-basis", dest="basis", action="store_false", help="keep all basisOfRecord")
    ap.add_argument("--no-captive", dest="captive", action="store_false", help="keep captive/managed records")
    ap.add_argument("--no-present-only", dest="present_only", action="store_false")
    ap.add_argument("--max-uncertainty-m", type=float, default=10000,
                    help="drop points with coordinateUncertaintyInMeters above this (null kept); "
                         "use -1 to disable")
    ap.add_argument("--min-year", type=int, default=1950)
    ap.add_argument("--max-year", type=int, default=2100)
    ap.add_argument("--drop-null-species", action="store_true", help="drop records with no species")
    ap.add_argument("--block-institutions", default="",
                    help="comma-separated institutionCodes to exclude (zoos etc.)")
    args = ap.parse_args()
    if args.max_uncertainty_m is not None and args.max_uncertainty_m < 0:
        args.max_uncertainty_m = None
    args.block_institutions = [s.strip() for s in args.block_institutions.split(",") if s.strip()]

    # glob helper: drop GBIF's extensionless marker files a '*' would sweep in
    src = args.src
    if any(c in src for c in "*?[]") or src.endswith(".parquet") is False:
        import os, glob
        if os.path.isdir(src):
            src = os.path.join(src, "*")
    con = duckdb.connect()

    def read_expr(path):
        import glob, os
        if any(c in path for c in "*?[]"):
            good = [f for f in sorted(glob.glob(path))
                    if os.path.getsize(f) > 8 and open(f, "rb").read(4) == b"PAR1"]
            if not good:
                raise SystemExit(f"no valid parquet files matched {path!r}")
            lst = "[" + ", ".join(f"'{f}'" for f in good) + "]"
            return f"read_parquet({lst})"
        return f"read_parquet('{path}')"

    rd = read_expr(src)
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {rd}").fetchall()]
    rules = build_rules(cols, args)
    where = " AND ".join(f"({p})" for _, p in rules) if rules else "TRUE"

    n_in = con.execute(f"SELECT count(*) FROM {rd}").fetchone()[0]
    print(f"input rows: {n_in:,}")
    # per-rule removed counts (one pass)
    if rules:
        sel = ", ".join(f"count(*) FILTER (WHERE NOT ({p})) AS r{i}" for i, (_, p) in enumerate(rules))
        counts = con.execute(f"SELECT {sel} FROM {rd}").fetchdf().iloc[0]
        print("removed by rule (overlapping):")
        for i, (label, _) in enumerate(rules):
            print(f"  - {label}: {int(counts[f'r{i}']):,}")

    con.execute(f"COPY (SELECT * FROM {rd} WHERE {where}) TO '{args.out}' "
                f"(FORMAT PARQUET, COMPRESSION zstd)")
    n_out = con.execute(f"SELECT count(*) FROM read_parquet('{args.out}')").fetchone()[0]
    print(f"kept rows:  {n_out:,}  ({n_out / max(n_in,1):.1%})  -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
