#!/usr/bin/env python3
"""Tag GBIF occurrences with the CoRE Stack tehsil (and/or MWS) they fall in.

Streaming build: occurrences are read in chunks straight out of the parquet via
DuckDB and spatially joined chunk-by-chunk against the boundary polygons, which
are held in memory once (with a spatial index). Each joined chunk is written to
the output immediately. Peak memory is ~one chunk + the polygons, so this runs
on a pan-India occurrence set (tens of millions of rows) without ever
materialising them all in pandas.

    python build_dataset.py \
        --occurrences 'occurrence.parquet/*' \
        --tehsil static.parquet \
        --tehsil-id-col tehsil_id \
        --out india_biodiversity.parquet

    # tune memory: smaller --chunk = less RAM, more iterations
    python build_dataset.py ... --chunk 250000

No CoRE Stack backend, no Earth Engine, no GeoServer. Just files.

Output is a plain Parquet (geometry dropped — every row keeps its lat/lon, which
is what the DuckDB queries use). Points that fall in no polygon get a NULL id;
pass --only-matched to drop them.
"""

from __future__ import annotations

import argparse
import logging
import sys

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(message)s")
logger = logging.getLogger("build")

# ---------------------------------------------------------------------------
# Column mapping. Adjust the RHS to your GBIF parquet's actual column names.
# ---------------------------------------------------------------------------
COLS = {
    "latitude": "decimallatitude",
    "longitude": "decimallongitude",
    "species": "species",
    "event_date": "eventdate",
}

MWS_ID_CANDIDATES = ("uid", "UID", "mws_uid", "DN")
TEHSIL_ID_CANDIDATES = ("tehsil_id", "TEHSIL", "tehsil_name", "SUB_DIST", "NAME", "pc11_subdistrict_id")


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _parquet_files(path: str) -> list[str]:
    """Resolve a path/glob to a list of real Parquet files, dropping any
    non-parquet markers a '*' glob sweeps in (GBIF ships extensionless marker
    files with no PAR1 footer, e.g. occurrence.parquet/000000)."""
    import os, glob
    if not any(c in path for c in "*?[]"):
        return [path]
    good = []
    for f in sorted(glob.glob(path)):
        try:
            if os.path.getsize(f) > 8 and open(f, "rb").read(4) == b"PAR1":
                good.append(f)
        except OSError:
            pass
    if not good:
        raise ValueError(f"no valid parquet files matched {path!r}")
    return good


def _sql_list(files: list[str]) -> str:
    return "[" + ", ".join(f"'{f}'" for f in files) + "]"


def _pick_id(gdf: gpd.GeoDataFrame, candidates: tuple[str, ...], role: str) -> str:
    for c in candidates:
        if c in gdf.columns:
            return c
    raise ValueError(
        f"No {role} id column found. Looked for {candidates}. "
        f"Columns present: {list(gdf.columns)}. Pass --{role}-id-col to name it."
    )


def load_boundaries(path: str, id_col: str | None, candidates, role: str,
                    layer: str | None) -> tuple[gpd.GeoDataFrame, str]:
    # GeoParquet (e.g. static.parquet) must be read with pyarrow, not GDAL:
    # gpd.read_file routes parquet through GDAL's Arrow driver, which on some
    # installs is linked against a missing libduckdb.so and errors out.
    if str(path).endswith(".parquet"):
        gdf = gpd.read_parquet(path)
    else:
        gdf = gpd.read_file(path, layer=layer)

    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")

    col = id_col or _pick_id(gdf, candidates, role)
    out_col = f"{role}_id"
    gdf = gdf.rename(columns={col: out_col})
    gdf[out_col] = gdf[out_col].astype(str)

    bad = ~gdf.geometry.is_valid
    if bad.any():
        logger.info("Repairing %d invalid %s geometries", int(bad.sum()), role)
        gdf.loc[bad, "geometry"] = gdf.loc[bad, "geometry"].buffer(0)

    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty]
    logger.info("Loaded %d %s polygons (id column: %s)", len(gdf), role, out_col)
    return gdf[[out_col, "geometry"]].reset_index(drop=True), out_col


def _output_schema(sample_file: str, extra_ids: list[str]) -> pa.Schema:
    """Fixed output schema = source parquet's schema (any all-null column forced
    to string so an all-null chunk can't mis-infer) + the id columns we append,
    in the order the joined frame produces them."""
    src = pq.ParquetFile(sample_file).schema_arrow
    fields = [pa.field(f.name, pa.string() if pa.types.is_null(f.type) else f.type)
              for f in src]
    fields += [pa.field(name, pa.string()) for name in extra_ids]
    return pa.schema(fields)


def _iter_chunks(con, sql_src: str, where: str, chunk: int):
    """Yield bbox-filtered occurrence chunks; only one chunk is resident."""
    rel = con.execute(f"SELECT * FROM read_parquet({sql_src}) WHERE {where}")
    while True:
        df = rel.fetch_df_chunk(vectors_per_chunk=max(1, chunk // 2048))
        if df is None or df.empty:
            break
        yield df


def _join_chunk(pts: pd.DataFrame, lat: str, lon: str,
                boundaries: list[tuple[gpd.GeoDataFrame, str]],
                snap_max: float | None = None) -> pd.DataFrame:
    g = gpd.GeoDataFrame(pts, geometry=gpd.points_from_xy(pts[lon], pts[lat]),
                         crs="EPSG:4326")
    for poly, id_col in boundaries:
        g = gpd.sjoin(g, poly[[id_col, "geometry"]], how="left", predicate="within")
        g = g.drop(columns=[c for c in ("index_right",) if c in g.columns])
        # a point on a shared border can match 2 polygons -> keep first
        g = g[~g.index.duplicated(keep="first")]

        # Optional fallback: points that fell in no polygon (offshore, on a
        # border, in a sliver gap) are assigned to the NEAREST polygon within
        # snap_max degrees. Points farther than that stay NULL. Only the
        # unmatched subset is processed, so this stays cheap.
        if snap_max:
            miss = g[id_col].isna()
            if miss.any():
                left = g.loc[miss].drop(columns=[id_col])
                near = gpd.sjoin_nearest(left, poly[[id_col, "geometry"]],
                                         how="left", max_distance=snap_max)
                near = near.drop(columns=[c for c in ("index_right",) if c in near.columns])
                near = near[~near.index.duplicated(keep="first")]
                g.loc[miss, id_col] = near[id_col]
    return g.drop(columns=[c for c in ("geometry",) if c in g.columns])


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--occurrences", required=True, help="GBIF parquet, file or glob")
    p.add_argument("--mws", help="MWS boundary file (gpkg/geojson/parquet/shp)")
    p.add_argument("--tehsil", help="tehsil boundary file (e.g. static.parquet)")
    p.add_argument("--mws-layer", default=None)
    p.add_argument("--tehsil-layer", default=None)
    p.add_argument("--mws-id-col", default=None)
    p.add_argument("--tehsil-id-col", default=None)
    p.add_argument("--out", default="biodiversity.parquet")
    p.add_argument("--chunk", type=int, default=500_000,
                   help="occurrence rows per streamed chunk (lower = less RAM)")
    p.add_argument("--bbox", default=None,
                   help="min_lon,min_lat,max_lon,max_lat pre-filter. Omit to "
                        "auto-derive from the boundaries.")
    p.add_argument("--no-bbox", action="store_true",
                   help="stream every point, no bbox pre-filter")
    p.add_argument("--bbox-pad", type=float, default=0.05)
    p.add_argument("--only-matched", action="store_true",
                   help="drop points that fall in no polygon")
    p.add_argument("--snap-nearest", action="store_true",
                   help="assign unmatched points to the nearest polygon within "
                        "--snap-max-deg (recovers coastal / border / sliver points)")
    p.add_argument("--snap-max-deg", type=float, default=0.02,
                   help="max snap distance in degrees (~0.02 deg ~= 2.2 km)")
    args = p.parse_args()

    if not args.mws and not args.tehsil:
        p.error("give at least one of --mws / --tehsil")

    boundaries: list[tuple[gpd.GeoDataFrame, str]] = []
    if args.tehsil:
        teh, tid = load_boundaries(args.tehsil, args.tehsil_id_col,
                                   TEHSIL_ID_CANDIDATES, "tehsil", args.tehsil_layer)
        boundaries.append((teh, tid))
    if args.mws:
        mws, mid = load_boundaries(args.mws, args.mws_id_col,
                                   MWS_ID_CANDIDATES, "mws", args.mws_layer)
        boundaries.append((mws, mid))
    id_cols = [c for _, c in boundaries]

    # bbox pre-filter (mostly a no-op for a pan-India boundary, but cheap)
    lat, lon = COLS["latitude"], COLS["longitude"]
    where_parts = [f'"{lat}" IS NOT NULL', f'"{lon}" IS NOT NULL']
    if not args.no_bbox:
        if args.bbox:
            minx, miny, maxx, maxy = (float(x) for x in args.bbox.split(","))
        else:
            bounds = [poly.total_bounds for poly, _ in boundaries]
            minx = min(b[0] for b in bounds) - args.bbox_pad
            miny = min(b[1] for b in bounds) - args.bbox_pad
            maxx = max(b[2] for b in bounds) + args.bbox_pad
            maxy = max(b[3] for b in bounds) + args.bbox_pad
        where_parts += [f'"{lon}" BETWEEN {minx} AND {maxx}',
                        f'"{lat}" BETWEEN {miny} AND {maxy}']
    where = " AND ".join(where_parts)

    import duckdb
    con = duckdb.connect()
    files = _parquet_files(args.occurrences)
    sql_src = _sql_list(files)

    schema = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({sql_src})").fetchall()]
    if lat not in schema or lon not in schema:
        raise ValueError(f"lat/lon columns {lat!r}/{lon!r} not in parquet. "
                         f"Edit COLS. Columns: {schema[:40]}")

    out_schema = _output_schema(files[0], id_cols)

    writer = None
    n_in = n_out = n_matched = 0
    logger.info("Streaming %s in chunks of %s (out -> %s)",
                args.occurrences, f"{args.chunk:,}", args.out)
    try:
        for i, pts in enumerate(_iter_chunks(con, sql_src, where, args.chunk), 1):
            n_in += len(pts)
            joined = _join_chunk(pts, lat, lon, boundaries,
                                 snap_max=args.snap_max_deg if args.snap_nearest else None)
            primary = id_cols[0]  # tehsil id if present, else mws id
            if args.only_matched:
                joined = joined[joined[primary].notna()]
            if joined.empty:
                continue
            n_matched += int(joined[primary].notna().sum())
            n_out += len(joined)

            table = pa.Table.from_pandas(joined, preserve_index=False)
            table = table.select(out_schema.names).cast(out_schema, safe=False)
            if writer is None:
                writer = pq.ParquetWriter(args.out, out_schema, compression="zstd")
            writer.write_table(table)
            if i % 10 == 0:
                logger.info("  %d chunks, %s points read, %s written",
                            i, f"{n_in:,}", f"{n_out:,}")
    finally:
        if writer is not None:
            writer.close()

    print("\n" + "=" * 60)
    print(f"Output: {args.out}")
    print(f"Points read (in bbox): {n_in:,}")
    print(f"Points written:        {n_out:,}")
    print(f"  of which tagged with {id_cols[0]}: {n_matched:,}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
