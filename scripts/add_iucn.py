#!/usr/bin/env python3
"""Enrich the joined occurrences with IUCN Red List status and slim the file.

Two things at once:
  1. Join a species -> IUCN category table onto your occurrences (tabular join
     on the scientific name, nothing spatial).
  2. Write the result far smaller: drop the redundant geometry column (lat/lon
     already carry the location), sort for compression, use zstd, and optionally
     partition by year or tehsil so queries only read the slice they need.

    # get IUCN table first (see README): a CSV/parquet with columns
    #   scientificName, redlistCategory
    python enrich_and_optimize.py \
        --occurrences biodiversity.parquet \
        --iucn iucn_species.csv \
        --iucn-name-col scientificName \
        --iucn-category-col redlistCategory \
        --species-col species \
        --out biodiversity_iucn.parquet \
        --partition-by year        # optional

No IUCN file? Run with --no-iucn to just slim/partition the existing data.
"""

from __future__ import annotations

import argparse
import os

import duckdb


def norm(col: str) -> str:
    """SQL expression that normalises a name for joining: trim + lowercase."""
    return f"lower(trim({col}))"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--occurrences", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--iucn", help="species->category table (csv/parquet)")
    p.add_argument("--no-iucn", action="store_true", help="skip IUCN, just optimize")
    p.add_argument("--iucn-name-col", default="scientificName")
    p.add_argument("--iucn-category-col", default="redlistCategory")
    p.add_argument("--iucn-id-col", default="internalTaxonId",
                   help="stable IUCN taxon id carried through as iucn_id, so future "
                        "Red List updates can be joined on the id rather than the name. "
                        "Set to '' to skip.")
    p.add_argument("--species-col", default="species",
                   help="species column in YOUR occurrences to match on")
    p.add_argument("--drop-geometry", action="store_true", default=True,
                   help="drop the WKB geometry column (default: yes; lat/lon remain)")
    p.add_argument("--keep-geometry", dest="drop_geometry", action="store_false")
    p.add_argument("--partition-by", default=None,
                   help="column to partition the output folder by (e.g. year, tehsil_id)")
    p.add_argument("--compression", default="zstd")
    args = p.parse_args()

    con = duckdb.connect()
    occ_cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{args.occurrences}')").fetchall()]

    # Build the SELECT list, dropping geometry if asked.
    select_cols = [c for c in occ_cols if not (args.drop_geometry and c.lower() == "geometry")]
    select_expr = ", ".join(f'o."{c}"' for c in select_cols)

    reader = (f"read_csv_auto('{args.iucn}')" if args.iucn and args.iucn.endswith(".csv")
              else f"read_parquet('{args.iucn}')" if args.iucn else None)

    if args.no_iucn or not args.iucn:
        query = f"SELECT {select_expr} FROM read_parquet('{args.occurrences}') o"
        print("Optimizing without IUCN enrichment.")
    else:
        # LEFT JOIN so occurrences whose species isn't in the IUCN table are kept
        # (their iucn_category is NULL), rather than dropped.
        # Also carry the stable IUCN taxon id (iucn_id) when available, so a
        # future Red List release can be re-joined on the id, not the name.
        id_select = ""
        if args.iucn_id_col:
            id_select = f',\n                   i."{args.iucn_id_col}" AS iucn_id'
        query = f"""
            SELECT {select_expr},
                   i."{args.iucn_category_col}" AS iucn_category{id_select}
            FROM read_parquet('{args.occurrences}') o
            LEFT JOIN {reader} i
              ON {norm(f'o."{args.species_col}"')} = {norm(f'i."{args.iucn_name_col}"')}
        """
        print(f"Joining IUCN status on {args.species_col} = {args.iucn_name_col}"
              + (f", carrying {args.iucn_id_col} -> iucn_id" if args.iucn_id_col else ""))

    # Sorting by species groups repeated values -> better dictionary compression.
    order = f'ORDER BY "{args.species_col}"' if args.species_col in select_cols else ""

    if args.partition_by:
        con.execute(f"""
            COPY ({query} {order})
            TO '{args.out}' (FORMAT PARQUET, PARTITION_BY ({args.partition_by}),
                             COMPRESSION {args.compression}, OVERWRITE_OR_IGNORE)
        """)
        files = _walk(args.out)
        total = sum(os.path.getsize(f) for f in files)
        print(f"Wrote partitioned dataset: {len(files)} files, {total/1e6:.1f} MB")
        print(f"Partitioned by {args.partition_by}. Query with:")
        print(f"  read_parquet('{args.out}/**/*.parquet', hive_partitioning=true)")
    else:
        con.execute(f"""
            COPY ({query} {order})
            TO '{args.out}' (FORMAT PARQUET, COMPRESSION {args.compression})
        """)
        print(f"Wrote {args.out}: {os.path.getsize(args.out)/1e6:.1f} MB")

    # Report IUCN coverage if we joined
    if args.iucn and not args.no_iucn:
        src = (f"read_parquet('{args.out}/**/*.parquet', hive_partitioning=true)"
               if args.partition_by else f"read_parquet('{args.out}')")
        row = con.execute(f"""
            SELECT count(*) AS "total",
                   count(iucn_category) AS "n_matched",
                   count(DISTINCT CASE WHEN iucn_category IN
                        ('Critically Endangered','Endangered','Vulnerable','CR','EN','VU')
                        THEN "{args.species_col}" END) AS "n_threatened"
            FROM {src}
        """).fetchone()
        print(f"\nIUCN coverage: {row[1]:,}/{row[0]:,} records matched a category")
        print(f"Threatened species (CR/EN/VU): {row[2]}")
    return 0


def _walk(root: str) -> list[str]:
    out = []
    for d, _, fs in os.walk(root):
        out += [os.path.join(d, f) for f in fs if f.endswith(".parquet")]
    return out


if __name__ == "__main__":
    raise SystemExit(main())
