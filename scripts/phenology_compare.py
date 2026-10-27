#!/usr/bin/env python3
"""Compare monthly occurrence profiles of migratory vs non-migratory birds.

For each species it plots the PERCENTAGE of that species' occurrences falling in
each month (so species with very different totals are directly comparable).
Migratory species are drawn as solid coloured lines, residents as dashed grey —
migrants show seasonal peaks, residents stay near the flat 1/12 (8.3%) line.

    python phenology_compare.py --data india_biodiversity.parquet \
        --migratory "Anas acuta,Clamator jacobinus" \
        --resident  "Corvus splendens,Pycnonotus cafer" \
        --out phenology.png

--normalize effort  divides each month by that month's TOTAL bird records first,
which removes the birding-effort seasonality (winter birding boom). With it,
true residents flatten almost exactly onto the uniform line.
"""
from __future__ import annotations

import argparse

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import calendar


def norm_expr(col):
    return f"lower(trim(regexp_replace(\"{col}\", '\\s+', ' ', 'g')))"


def month_expr(dc):
    return (f"COALESCE(month(TRY_CAST(\"{dc}\" AS DATE)), "
            f"TRY_CAST(regexp_extract(CAST(\"{dc}\" AS VARCHAR),'^\\d{{4}}-(\\d{{2}})',1) AS INTEGER))")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", required=True)
    ap.add_argument("--migratory", default="", help="comma-separated species")
    ap.add_argument("--resident", default="", help="comma-separated species")
    ap.add_argument("--species-col", default="species")
    ap.add_argument("--date-col", default="eventdate")
    ap.add_argument("--normalize", choices=["none", "effort"], default="none",
                    help="none = % of each species' own records; "
                         "effort = divide by monthly total bird records first (removes effort)")
    ap.add_argument("--out", default="phenology.png")
    args = ap.parse_args()

    mig = [s.strip() for s in args.migratory.split(",") if s.strip()]
    res = [s.strip() for s in args.resident.split(",") if s.strip()]
    species = mig + res
    if not species:
        raise SystemExit("give --migratory and/or --resident species")

    con = duckdb.connect()
    sp, dc = args.species_col, args.date_col
    ncol, mcol = norm_expr(sp), month_expr(dc)
    norm_list = [s.strip().lower() for s in species]

    # per species x month counts (listed species only)
    ph = ", ".join("?" for _ in norm_list)
    sp_month = con.execute(f"""
        SELECT "{sp}" AS species_raw, {ncol} AS norm, {mcol} AS month, count(*) AS n
        FROM read_parquet('{args.data}')
        WHERE "{sp}" IS NOT NULL AND {mcol} BETWEEN 1 AND 12 AND {ncol} IN ({ph})
        GROUP BY "{sp}", {ncol}, {mcol}
    """, norm_list).fetchdf()
    if sp_month.empty:
        raise SystemExit("no records matched those species names — check spelling / taxonomy.")

    if args.normalize == "effort":
        tot = con.execute(f"""
            SELECT {mcol} AS month, count(*) AS tot
            FROM read_parquet('{args.data}')
            WHERE {mcol} BETWEEN 1 AND 12
            GROUP BY {mcol}
        """).fetchdf().set_index("month")["tot"]
        sp_month["val"] = sp_month.apply(lambda r: r["n"] / tot.get(r["month"], 1), axis=1)
    else:
        sp_month["val"] = sp_month["n"].astype(float)

    def series_for(name):
        sub = sp_month[sp_month["norm"] == name.strip().lower()]
        if sub.empty:
            return None, 0
        s = sub.groupby("month")["val"].sum().reindex(range(1, 13), fill_value=0)
        n = int(sub["n"].sum())
        pct = 100 * s / s.sum() if s.sum() else s
        return pct, n

    fig, ax = plt.subplots(figsize=(12, 6.5))
    months = range(1, 13)
    ax.axhline(100 / 12, color="0.6", lw=1, ls=":", label="uniform (8.3%, no seasonality)")

    cmap = plt.cm.tab10.colors
    for i, name in enumerate(mig):
        pct, n = series_for(name)
        if pct is None:
            print(f"  (skipped, no data) {name}"); continue
        ax.plot(months, pct.values, "-o", ms=4, lw=2, color=cmap[i % 10],
                label=f"{name}  (migratory, n={n:,})")
    for j, name in enumerate(res):
        pct, n = series_for(name)
        if pct is None:
            print(f"  (skipped, no data) {name}"); continue
        shade = str(0.25 + 0.15 * j)
        ax.plot(months, pct.values, "--s", ms=4, lw=2, color=shade,
                label=f"{name}  (resident, n={n:,})")

    ax.set_xticks(list(months))
    ax.set_xticklabels([calendar.month_abbr[m] for m in months])
    ax.set_ylabel("% of the species' occurrences")
    ttl = "Monthly occurrence profile — migratory vs resident"
    if args.normalize == "effort":
        ttl += "  (effort-adjusted)"
    ax.set_title(ttl, fontsize=14)
    ax.grid(axis="y", alpha=0.3)
    ax.legend(fontsize=9, loc="upper center", bbox_to_anchor=(0.5, -0.1), ncol=2)
    fig.tight_layout()
    fig.savefig(args.out, dpi=150, bbox_inches="tight")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
