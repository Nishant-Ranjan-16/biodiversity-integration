#!/usr/bin/env python3
"""Match GBIF species to IUCN, resolving synonyms — all local, no API.

GBIF often uses an older scientific name than IUCN's accepted name
(e.g. GBIF 'Psittacula cyanocephala' == IUCN 'Himalayapsitta cyanocephala').
The IUCN bulk download ships a synonyms table precisely so you can bridge this.

This builds ONE lookup table  species_name -> (iucn_id, redlistCategory)  that
covers BOTH accepted names and synonyms, which you then join onto your data.

    python resolve_iucn_synonyms.py \
        --assessments assessments.csv \
        --synonyms synonyms.csv \
        --out iucn_lookup.parquet

Then enrich with that lookup as the --iucn table (it has columns
scientificName, redlistCategory, internalTaxonId), and enrich_and_optimize.py
matches far more species because the lookup now includes old names too.
"""

from __future__ import annotations

import argparse
import sys
import duckdb


def norm(col):  # trim + lowercase for case/space-insensitive matching
    return f"lower(trim({col}))"


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--assessments", required=True, help="IUCN assessments.csv")
    p.add_argument("--synonyms", required=True, help="IUCN synonyms.csv")
    p.add_argument("--out", default="iucn_lookup.parquet")
    # column names — defaults match the standard IUCN bulk export
    p.add_argument("--assess-id", default="internalTaxonId")
    p.add_argument("--assess-name", default="scientificName")
    p.add_argument("--assess-cat", default="redlistCategory")
    p.add_argument("--syn-id", default="internalTaxonId")
    p.add_argument("--syn-name", default="name",
                   help="synonym name column. NOTE: in the standard IUCN export the "
                        "'name' column includes authorship and messy whitespace "
                        "(e.g. 'Notidanus  griseus (Bonnaterre, 1788)'), which will "
                        "NOT match a clean GBIF binomial. Prefer --syn-from-parts.")
    p.add_argument("--syn-from-parts", action="store_true", default=True,
                   help="build the synonym binomial from genusName + speciesName "
                        "(clean, matches GBIF) instead of the author-laden name column. "
                        "On by default; use --syn-use-name to override.")
    p.add_argument("--syn-use-name", dest="syn_from_parts", action="store_false",
                   help="use the raw 'name' column instead of genus+species parts")
    p.add_argument("--syn-genus", default="genusName")
    p.add_argument("--syn-species", default="speciesName")
    args = p.parse_args()

    con = duckdb.connect()

    # Show the synonyms schema and validate the needed columns exist.
    syn_cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_csv_auto('{args.synonyms}')").fetchall()]
    print(f"synonyms.csv columns: {syn_cols}")
    if args.syn_from_parts:
        missing = [c for c in (args.syn_genus, args.syn_species) if c not in syn_cols]
        if missing:
            sys.exit(f"--syn-from-parts needs {args.syn_genus} and {args.syn_species}; "
                     f"missing {missing}. Columns present: {syn_cols}")
        print(f"Building synonym binomials from {args.syn_genus} + {args.syn_species} "
              f"(clean, matches GBIF).")
    elif args.syn_name not in syn_cols:
        sys.exit(f"--syn-name {args.syn_name!r} not in {syn_cols}")

    a, s = f"read_csv_auto('{args.assessments}')", f"read_csv_auto('{args.synonyms}')"

    # How to derive the synonym's matchable name:
    #  - from parts (default): genusName + ' ' + speciesName, each stripped of
    #    stray whitespace/tabs -> a clean binomial like 'Notidanus griseus' that
    #    matches a GBIF species column.
    #  - from the raw 'name' column: includes authorship, won't match GBIF.
    if args.syn_from_parts:
        # regexp_replace collapses internal whitespace/tabs to a single space; trim ends.
        syn_name_expr = (
            f"trim(regexp_replace("
            f"trim(CAST(syn.\"{args.syn_genus}\" AS VARCHAR)) || ' ' || "
            f"trim(CAST(syn.\"{args.syn_species}\" AS VARCHAR)), '\\s+', ' ', 'g'))"
        )
        syn_name_notnull = (f'syn."{args.syn_genus}" IS NOT NULL '
                            f'AND syn."{args.syn_species}" IS NOT NULL')
    else:
        syn_name_expr = f'syn."{args.syn_name}"'
        syn_name_notnull = f'syn."{args.syn_name}" IS NOT NULL'

    # Build the unified lookup:
    #   (1) every accepted name from assessments
    #   (2) every synonym name, carrying its taxon's id and (via join) category
    # Then collapse to EXACTLY ONE ROW PER NAME. This is critical: if a name
    # appears twice (an accepted name that is also someone's synonym, or a
    # synonym shared by two taxa), a LEFT JOIN onto occurrences would DUPLICATE
    # those occurrence rows and inflate every count. We keep one row per name,
    # preferring an 'accepted' match over a 'synonym' one, breaking further ties
    # deterministically by the smallest taxon id.
    con.execute(f"""
        COPY (
          WITH all_names AS (
            SELECT lower(trim("{args.assess_name}")) AS nm_key,
                   "{args.assess_name}"        AS scientificName,
                   "{args.assess_id}"          AS internalTaxonId,
                   "{args.assess_cat}"         AS redlistCategory,
                   'accepted'                  AS match_type
            FROM {a}
            WHERE "{args.assess_name}" IS NOT NULL

            UNION ALL

            SELECT lower(trim({syn_name_expr})) AS nm_key,
                   {syn_name_expr}             AS scientificName,
                   asmt."{args.assess_id}"     AS internalTaxonId,
                   asmt."{args.assess_cat}"    AS redlistCategory,
                   'synonym'                   AS match_type
            FROM {s} syn
            JOIN {a} asmt ON syn."{args.syn_id}" = asmt."{args.assess_id}"
            WHERE {syn_name_notnull}
          ),
          ranked AS (
            SELECT *,
                   row_number() OVER (
                     PARTITION BY nm_key
                     ORDER BY (match_type = 'accepted') DESC, internalTaxonId
                   ) AS rn
            FROM all_names
            WHERE nm_key IS NOT NULL AND nm_key <> ''
          )
          SELECT scientificName, internalTaxonId, redlistCategory, match_type
          FROM ranked WHERE rn = 1
        )
        TO '{args.out}' (FORMAT PARQUET, COMPRESSION zstd)
    """)

    # Integrity guarantee: assert one row per (lowercased) name.
    dup = con.execute(f"""
        SELECT count(*) FROM (
          SELECT lower(trim(scientificName)) nm, count(*) c
          FROM read_parquet('{args.out}') GROUP BY 1 HAVING count(*) > 1
        )
    """).fetchone()[0]
    print(f"names with >1 row after dedup (must be 0): {dup}")

    # report
    r = con.execute(f"""
        SELECT match_type, count(*) AS names, count(DISTINCT internalTaxonId) AS taxa
        FROM read_parquet('{args.out}') GROUP BY 1
    """).fetchdf()
    total = con.execute(f"SELECT count(*) FROM read_parquet('{args.out}')").fetchone()[0]
    print(f"\nWrote {args.out}: {total:,} name->category rows")
    print(r.to_string(index=False))
    print(f"\nNow enrich with this as the IUCN table — it includes old names too:")
    print(f"  python enrich_and_optimize.py --occurrences <data> \\")
    print(f"      --iucn {args.out} --iucn-name-col scientificName \\")
    print(f"      --iucn-category-col redlistCategory --iucn-id-col internalTaxonId \\")
    print(f"      --species-col species --out <enriched>")
    return 0


if __name__ == "__main__":
    sys.exit(main())
