#!/usr/bin/env python3
"""Aggregate tagged GBIF occurrences into CoreLens-compatible annual metric tables.

CoreLens (https://github.com/ApoorvaKashyap/core-lens) reads biodiversity as
per-entity, per-year *metrics* sitting next to an entity's static geometry file,
not as raw occurrences. This turns your tagged occurrence parquet (output of
build_dataset.py, carrying a tehsil_id / mws_id column) into:

    <data_root>/tehsil/annual.parquet             (single file, tehsil style)
      or
    <data_root>/mws/annual/year=YYYY/*.parquet    (hive-partitioned, mws style)

Columns:  <key>, year, n_observations, n_species, n_threatened_species,
          exploiter_share, avoider_share, threatened_share

Usage:
    python make_corelens_annual.py --data india_biodiversity.parquet \
        --key tehsil_id --data-root ./corelens_data

    python make_corelens_annual.py --data india_biodiversity.parquet \
        --key mws_id --entity mws --partition-by-year --data-root ./corelens_data
"""
from __future__ import annotations

import argparse
import os

import duckdb

# --- species classification (edit these lists) -----------------------------
# Urban exploiters: established generalists, matched on GBIF's `species` binomial.
# Avoiders: a-priori / illustrative — validate before relying on it.
# Threatened is derived from the IUCN category column, not a fixed list.
EXPLOITERS = [
    "Corvus splendens", "Corvus macrorhynchos", "Psittacula krameri",
    "Columba livia", "Milvus migrans", "Passer domesticus",
]
AVOIDERS: list[str] = [
    # e.g. "Gracula religiosa", "Harpactes fasciatus", ...  (fill in / validate)
]
# Covers both GBIF-style codes (CR/EN/VU) and IUCN bulk full words, with the
# "Critically Endangered" space/underscore variants, all matched case-insensitively.
THREATENED_IUCN = (
    "CR", "EN", "VU",
    "CRITICALLY ENDANGERED", "CRITICALLY_ENDANGERED",
    "ENDANGERED", "VULNERABLE",
)


def sql_list(xs) -> str:
    return "(" + ", ".join("'" + x.replace("'", "''").upper() + "'" for x in xs) + ")"


def _sp_list(xs) -> str:
    return "(" + ", ".join("'" + x.replace("'", "''") + "'" for x in xs) + ")"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", required=True, help="tagged occurrence parquet (file or glob)")
    ap.add_argument("--key", required=True, help="entity key column, e.g. tehsil_id or mws_id")
    ap.add_argument("--entity", default=None, help="entity folder name (default: key without _id)")
    ap.add_argument("--data-root", default="corelens_data")
    ap.add_argument("--species-col", default="species")
    ap.add_argument("--year-col", default="year", help="integer year column, if present")
    ap.add_argument("--date-col", default="eventdate", help="fallback date/text col to derive year")
    ap.add_argument("--iucn-col", default="iucn_category", help="IUCN category col (skipped if absent)")
    ap.add_argument("--partition-by-year", action="store_true",
                    help="write annual/year=YYYY/… (recommended for mws); "
                         "default writes a single annual.parquet")
    ap.add_argument("--min-year", type=int, default=1990)
    ap.add_argument("--max-year", type=int, default=2026)
    args = ap.parse_args()

    entity = args.entity or args.key.replace("_id", "")
    con = duckdb.connect()
    src = f"read_parquet('{args.data}')"
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()]
    key, sp = args.key, args.species_col
    if key not in cols:
        raise SystemExit(f"key column {key!r} not in data. Columns: {cols[:40]}")
    if sp not in cols:
        raise SystemExit(f"species column {sp!r} not in data. Columns: {cols[:40]}")

    # year: prefer the integer year column, else pull 4 digits out of a date/text col
    if args.year_col in cols:
        yr = f'TRY_CAST("{args.year_col}" AS INTEGER)'
    elif args.date_col in cols:
        yr = (f"TRY_CAST(regexp_extract(CAST(\"{args.date_col}\" AS VARCHAR), "
              f"'(\\d{{4}})', 1) AS INTEGER)")
    else:
        raise SystemExit(f"need {args.year_col!r} or {args.date_col!r} to derive year")

    has_iucn = args.iucn_col in cols
    thr_obs = (f"count(*) FILTER (WHERE upper(CAST(\"{args.iucn_col}\" AS VARCHAR)) "
               f"IN {sql_list(THREATENED_IUCN)})" if has_iucn else "CAST(0 AS BIGINT)")
    thr_sp = (f"count(DISTINCT CASE WHEN upper(CAST(\"{args.iucn_col}\" AS VARCHAR)) "
              f"IN {sql_list(THREATENED_IUCN)} THEN \"{sp}\" END)"
              if has_iucn else "CAST(0 AS BIGINT)")

    q = f"""
    SELECT
        "{key}"                                        AS {key},
        {yr}                                           AS year,
        count(*)                                       AS n_observations,
        count(DISTINCT "{sp}")                         AS n_species,
        {thr_sp}                                       AS n_threatened_species,
        round(count(*) FILTER (WHERE "{sp}" IN {_sp_list(EXPLOITERS)})*1.0
              / nullif(count(*), 0), 6)                AS exploiter_share,
        round(count(*) FILTER (WHERE "{sp}" IN {_sp_list(AVOIDERS or ['__none__'])})*1.0
              / nullif(count(*), 0), 6)                AS avoider_share,
        round({thr_obs}*1.0 / nullif(count(*), 0), 6)  AS threatened_share
    FROM {src}
    WHERE "{key}" IS NOT NULL AND "{sp}" IS NOT NULL
      AND {yr} BETWEEN {args.min_year} AND {args.max_year}
    GROUP BY "{key}", {yr}
    ORDER BY "{key}", year
    """

    out_dir = os.path.join(args.data_root, entity)
    os.makedirs(out_dir, exist_ok=True)
    if args.partition_by_year:
        out = os.path.join(out_dir, "annual")
        con.execute(f"COPY ({q}) TO '{out}' (FORMAT PARQUET, PARTITION_BY (year), "
                    f"COMPRESSION zstd, OVERWRITE_OR_IGNORE)")
        n = con.execute(f"SELECT count(*) FROM read_parquet('{out}/**/*.parquet', "
                        f"hive_partitioning=true)").fetchone()[0]
    else:
        out = os.path.join(out_dir, "annual.parquet")
        con.execute(f"COPY ({q}) TO '{out}' (FORMAT PARQUET, COMPRESSION zstd)")
        n = con.execute(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]

    print(f"wrote {out}  ({n:,} rows = {key} x year)")
    print(f"IUCN metrics: {'ON' if has_iucn else 'OFF (no ' + args.iucn_col + ' column)'}")
    print(f"\nDrop your boundary file in as:  {out_dir}/static.parquet")
    print(f"CoreLens will then read {entity}.annual with your biodiversity metrics.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
