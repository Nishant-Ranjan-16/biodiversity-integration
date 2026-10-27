#!/usr/bin/env python3
"""Seasonal migration maps for every AVONET-migratory species above a record threshold.

Pipeline:
  1. AVONET  -> migratory species (Migration trait 2/3).
  2. GBIF    -> count records per migratory species; keep those with
     >= --min-points records (default 10,000).
  3. For each kept species, write the seasonal figures:
       <out>/<species>/monthly_grid.png
       <out>/<species>/centroid_track.png
       <out>/<species>/latitude_by_month.png
     plus kept_species.csv listing what passed / was skipped.

    python migratory_maps_batch.py --data india_biodiversity.parquet \
        --avonet "AVONET Supplementary dataset 1.xlsx" --avonet-species-col Species2 \
        --boundaries static.parquet --min-points 10000 --out-dir migratory_maps

Memory-safe: GBIF is scanned twice (count, then pull only the kept species'
lon/lat/month). The India outline is built once and reused across species.
"""
from __future__ import annotations

import argparse
import calendar
import os
import re

import duckdb
import geopandas as gpd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# ---------------------------------------------------------------- AVONET
def norm_name(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower().str.replace(r"\s+", " ", regex=True)


def _has_mig(df):
    return any(str(c).lower() == "migration" for c in df.columns)


def _pick_sheet(book, sheet, sp_col):
    """Choose the right AVONET trait sheet. The workbook has one sheet per
    taxonomy (BirdLife/Species1, eBird/Species2, BirdTree/Species3), each with
    its own Migration column — so we must pick by the requested species column
    (or sheet name), not just 'first sheet with Migration'."""
    if sheet is not None:
        if sheet not in book:
            raise SystemExit(f"Sheet {sheet!r} not found. Sheets: {list(book.keys())}")
        return book[sheet]
    if sp_col is not None:                                   # match the asked column
        for name, df in book.items():
            if sp_col in df.columns and _has_mig(df):
                return df
        raise SystemExit(f"No sheet has both {sp_col!r} and 'Migration'. "
                         f"Sheets: {list(book.keys())}")
    for name, df in book.items():                            # prefer eBird sheet
        if "ebird" in name.lower() and _has_mig(df):
            return df
    for name, df in book.items():                            # else first usable
        if _has_mig(df) and any(c in df.columns for c in
                                ("Species1", "Species2", "Species3", "species",
                                 "scientificName", "Species")):
            return df
    raise SystemExit(f"No sheet with 'Migration' + a species column. Sheets: {list(book.keys())}")


def load_avonet_migratory(path, sheet, sp_col, mig_col, codes):
    if str(path).lower().endswith((".xlsx", ".xls")):
        book = pd.read_excel(path, sheet_name=None)
        av = _pick_sheet(book, sheet, sp_col)
    else:
        av = pd.read_csv(path)
    cols = {c.lower(): c for c in map(str, av.columns)}
    mcol = mig_col or cols.get("migration")
    if mcol not in av.columns:
        raise SystemExit(f"Migration column not found. Columns: {list(av.columns)[:30]}")
    if sp_col is None:
        for c in ("Species1", "Species2", "Species3", "species", "scientificName", "Species"):
            if c in av.columns:
                sp_col = c; break
    if sp_col not in av.columns:
        raise SystemExit(f"Species column not found. Pass --avonet-species-col. Columns: {list(av.columns)[:30]}")
    av = av[[sp_col, mcol]].dropna()
    av[mcol] = pd.to_numeric(av[mcol], errors="coerce")
    out = pd.DataFrame({"norm": norm_name(av[av[mcol].isin(codes)][sp_col]).unique()})
    print(f"AVONET: {len(av):,} assessed, {len(out):,} migratory (codes {sorted(codes)}, col {sp_col!r})")
    return out


# ---------------------------------------------------------------- outline
def load_outline(path):
    if not path:
        return None
    g = gpd.read_parquet(path) if str(path).endswith(".parquet") else gpd.read_file(path)
    if g.crs is None:
        g = g.set_crs("EPSG:4326")
    elif g.crs.to_epsg() != 4326:
        g = g.to_crs("EPSG:4326")
    g = g[g.geometry.notna()].copy()
    g["geometry"] = g.geometry.buffer(0)
    g = g[~g.geometry.is_empty]
    if g.empty:
        return None
    try:
        out = g.dissolve().boundary
    except Exception:
        out = g.boundary
    out = out[out.notna() & ~out.is_empty]
    return out if len(out) else None


def underlay(ax, outline):
    if outline is None:
        return
    try:
        outline.plot(ax=ax, color="0.75", linewidth=0.3, aspect=None)
    except Exception:
        pass


# ---------------------------------------------------------------- figures
def monthly_grid(df, outline, species, out):
    fig, axes = plt.subplots(3, 4, figsize=(14, 11), sharex=True, sharey=True)
    xmin, xmax, ymin, ymax = df.lon.min(), df.lon.max(), df.lat.min(), df.lat.max()
    for m in range(1, 13):
        ax = axes[(m - 1) // 4][(m - 1) % 4]
        underlay(ax, outline)
        sub = df[df.month == m]
        ax.scatter(sub.lon, sub.lat, s=3, c="crimson", alpha=0.35, linewidths=0)
        ax.set_title(f"{calendar.month_abbr[m]}  (n={len(sub):,})", fontsize=11)
        ax.set_xlim(xmin - 0.5, xmax + 0.5); ax.set_ylim(ymin - 0.5, ymax + 0.5)
        ax.set_aspect("equal"); ax.tick_params(labelsize=7)
    fig.suptitle(f"{species} — monthly occurrences (India GBIF)", fontsize=15, y=0.995)
    fig.tight_layout(); fig.savefig(out, dpi=140); plt.close(fig)


def centroid_track(df, outline, species, out):
    cen = df.groupby("month")[["lon", "lat"]].mean().reindex(range(1, 13)).dropna()
    fig, ax = plt.subplots(figsize=(9, 10))
    underlay(ax, outline)
    ax.scatter(df.lon, df.lat, s=2, c="0.8", alpha=0.25, linewidths=0, label="all records")
    cmap = plt.cm.twilight_shifted(np.linspace(0, 1, 12))
    ax.plot(cen.lon, cen.lat, "-", color="0.4", lw=1, zorder=2)
    for m in cen.index:
        ax.scatter(cen.lon[m], cen.lat[m], s=140, color=cmap[m - 1], edgecolor="k", zorder=3)
        ax.annotate(calendar.month_abbr[m], (cen.lon[m], cen.lat[m]),
                    fontsize=9, ha="center", va="center", zorder=4)
    ax.set_title(f"{species} — monthly mean location (Jan→Dec)", fontsize=13)
    ax.set_xlabel("longitude"); ax.set_ylabel("latitude"); ax.set_aspect("equal")
    ax.legend(loc="lower left", markerscale=3)
    fig.tight_layout(); fig.savefig(out, dpi=140); plt.close(fig)


def latitude_by_month(df, species, out):
    data = [df[df.month == m].lat.values for m in range(1, 13)]
    fig, ax = plt.subplots(figsize=(11, 6))
    try:
        ax.boxplot(data, tick_labels=[calendar.month_abbr[m] for m in range(1, 13)],
                   showfliers=False, medianprops=dict(color="crimson"))
    except TypeError:
        ax.boxplot(data, labels=[calendar.month_abbr[m] for m in range(1, 13)],
                   showfliers=False, medianprops=dict(color="crimson"))
    means = [np.mean(d) if len(d) else np.nan for d in data]
    ax.plot(range(1, 13), means, "o-", color="crimson", label="monthly mean latitude")
    ax.set_title(f"{species} — latitude by month (north–south seasonal movement)", fontsize=13)
    ax.set_ylabel("latitude"); ax.grid(axis="y", alpha=0.3); ax.legend()
    fig.tight_layout(); fig.savefig(out, dpi=140); plt.close(fig)


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", required=True)
    ap.add_argument("--avonet", required=True)
    ap.add_argument("--avonet-sheet", default=None)
    ap.add_argument("--avonet-species-col", default=None)
    ap.add_argument("--avonet-migration-col", default=None)
    ap.add_argument("--migration-codes", default="2,3")
    ap.add_argument("--boundaries", default=None)
    ap.add_argument("--species-col", default="species")
    ap.add_argument("--lat", default="decimallatitude")
    ap.add_argument("--lon", default="decimallongitude")
    ap.add_argument("--date-col", default="eventdate")
    ap.add_argument("--min-points", type=int, default=10000)
    ap.add_argument("--out-dir", default="migratory_maps")
    args = ap.parse_args()

    codes = {int(x) for x in args.migration_codes.split(",")}
    os.makedirs(args.out_dir, exist_ok=True)
    mig = load_avonet_migratory(args.avonet, args.avonet_sheet,
                                args.avonet_species_col, args.avonet_migration_col, codes)

    con = duckdb.connect()
    sp, lat, lon, dc = args.species_col, args.lat, args.lon, args.date_col
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{args.data}')").fetchall()]
    for c in (sp, lat, lon, dc):
        if c not in cols:
            raise SystemExit(f"column {c!r} not in GBIF. Columns: {cols[:40]}")
    con.register("mig", mig)
    norm_sql = f"lower(trim(regexp_replace(\"{sp}\", '\\s+', ' ', 'g')))"
    month_sql = (f"COALESCE(month(TRY_CAST(\"{dc}\" AS DATE)), "
                 f"TRY_CAST(regexp_extract(CAST(\"{dc}\" AS VARCHAR),'^\\d{{4}}-(\\d{{2}})',1) AS INTEGER))")

    # pass 1: counts per migratory species
    counts = con.execute(f"""
        WITH g AS (SELECT "{sp}" AS species, {norm_sql} AS norm FROM read_parquet('{args.data}')
                   WHERE "{sp}" IS NOT NULL)
        SELECT min(g.species) AS species, g.norm, count(*) AS n
        FROM g JOIN mig ON g.norm = mig.norm
        GROUP BY g.norm ORDER BY n DESC
    """).fetchdf()
    counts["passes"] = counts["n"] >= args.min_points
    counts.drop(columns=["norm"]).to_csv(os.path.join(args.out_dir, "kept_species.csv"), index=False)
    kept = counts[counts["passes"]]
    print(f"Migratory species in GBIF: {len(counts):,}; "
          f">= {args.min_points:,} records: {len(kept):,}")
    if kept.empty:
        raise SystemExit("none passed the threshold — lower --min-points or check name matching.")

    outline = load_outline(args.boundaries)

    # pass 2: pull lon/lat/month for ONLY the kept species, then split in pandas
    con.register("keep", kept[["norm"]])
    pts = con.execute(f"""
        SELECT {norm_sql} AS norm, "{sp}" AS species,
               "{lon}" AS lon, "{lat}" AS lat, {month_sql} AS month
        FROM read_parquet('{args.data}')
        WHERE "{sp}" IS NOT NULL AND "{lat}" IS NOT NULL AND "{lon}" IS NOT NULL
          AND {norm_sql} IN (SELECT norm FROM keep)
    """).fetchdf()
    pts = pts[pts["month"].between(1, 12)]

    for _, row in kept.iterrows():
        name = row["species"]
        df = pts[pts["norm"] == row["norm"]]
        if df.empty:
            continue
        safe = re.sub(r"[^\w]+", "_", name)
        d = os.path.join(args.out_dir, safe); os.makedirs(d, exist_ok=True)
        monthly_grid(df, outline, name, os.path.join(d, "monthly_grid.png"))
        centroid_track(df, outline, name, os.path.join(d, "centroid_track.png"))
        latitude_by_month(df, name, os.path.join(d, "latitude_by_month.png"))
        print(f"  {name}: {len(df):,} dated records -> {d}/")

    print(f"done. {len(kept)} species mapped under {args.out_dir}/ (list: kept_species.csv)")


if __name__ == "__main__":
    main()
