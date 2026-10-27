#!/usr/bin/env python3
"""Automated GBIF biodiversity pipeline.

One cycle = download fresh GBIF data -> clean -> (enrich with IUCN) -> join to
tehsil/MWS boundaries -> generate the urbanization and migration time series ->
delete the old raw data.

    python run_pipeline.py --config config.yaml                 # one full cycle now
    python run_pipeline.py --config config.yaml --every 3600    # loop hourly (testing)
    python run_pipeline.py --config config.yaml --use-existing path/to/occurrence.parquet
    python run_pipeline.py --config config.yaml --dry-run       # print steps, run nothing

Production scheduling: prefer cron over --every, e.g. monthly at 02:00 on the 1st:
    0 2 1 * *  cd /path/to/repo && /usr/bin/python3 run_pipeline.py --config config.yaml >> pipeline.log 2>&1
GBIF credentials come from the environment: GBIF_USER, GBIF_PWD.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import shutil
import subprocess
import sys
import time

import yaml

import gbif_download

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(message)s")
log = logging.getLogger("pipeline")
HERE = os.path.dirname(os.path.abspath(__file__))


def cfg(d, path, default=None):
    cur = d
    for k in path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def run(cmd, dry):
    log.info("$ %s", " ".join(str(c) for c in cmd))
    if dry:
        return
    r = subprocess.run([str(c) for c in cmd])
    if r.returncode != 0:
        raise RuntimeError(f"step failed ({r.returncode}): {' '.join(map(str, cmd))}")


def script(c, name):
    return os.path.join(cfg(c, "paths.scripts_dir", "scripts"), name)


def one_cycle(c, args):
    py = sys.executable
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    work = os.path.join(cfg(c, "paths.work_dir", "data/raw"), f"run_{stamp}")
    out = cfg(c, "paths.out_dir", "outputs")
    os.makedirs(work, exist_ok=True)
    os.makedirs(out, exist_ok=True)
    log.info("=== cycle %s  (work=%s) ===", stamp, work)

    # 1 — DOWNLOAD -------------------------------------------------
    if args.use_existing:
        raw = args.use_existing
        log.info("using existing occurrences: %s", raw)
    else:
        bbox = cfg(c, "gbif.bbox")
        raw = gbif_download.download(
            country=cfg(c, "gbif.country", "IN"),
            bbox=tuple(bbox) if bbox else None,
            out_dir=work,
            fmt=cfg(c, "gbif.format", "SIMPLE_PARQUET"),
            poll_seconds=cfg(c, "gbif.poll_seconds", 30),
            timeout_seconds=cfg(c, "gbif.timeout_seconds"),
            dry_run=args.dry_run,
        )
        raw = raw or os.path.join(work, "occurrence.parquet")
    raw_glob = os.path.join(raw, "*") if os.path.isdir(str(raw)) else raw

    # 2 — CLEAN ---------------------------------------------------
    clean_out = os.path.join(work, "occurrences_clean.parquet")
    cl = [py, os.path.join(HERE, "clean.py"),
          "--src", raw_glob, "--out", clean_out,
          "--max-uncertainty-m", cfg(c, "clean.max_uncertainty_m", 10000),
          "--min-year", cfg(c, "clean.min_year", 1950),
          "--max-year", cfg(c, "clean.max_year", 2100)]
    if not cfg(c, "clean.basis", True): cl.append("--no-basis")
    if not cfg(c, "clean.captive", True): cl.append("--no-captive")
    if not cfg(c, "clean.present_only", True): cl.append("--no-present-only")
    if cfg(c, "clean.drop_null_species", False): cl.append("--drop-null-species")
    blk = cfg(c, "clean.block_institutions") or []
    if blk: cl += ["--block-institutions", ",".join(blk)]
    run(cl, args.dry_run)

    # 2.5 — ENRICH with IUCN (optional) ---------------------------
    join_input = clean_out
    if cfg(c, "enrich.enabled", False):
        lookup = cfg(c, "enrich.lookup", "data/boundaries/iucn_lookup.parquet")
        assessments = cfg(c, "enrich.assessments")
        synonyms = cfg(c, "enrich.synonyms")
        # build the name->category lookup once (it only changes with an IUCN release)
        if not os.path.exists(lookup) and assessments and synonyms \
                and os.path.exists(assessments) and os.path.exists(synonyms):
            log.info("building IUCN lookup (one-time) -> %s", lookup)
            os.makedirs(os.path.dirname(lookup) or ".", exist_ok=True)
            run([py, script(c, "resolve_synonyms.py"),
                 "--assessments", assessments, "--synonyms", synonyms,
                 "--out", lookup], args.dry_run)
        if os.path.exists(lookup) or args.dry_run:
            enriched = os.path.join(work, "occurrences_enriched.parquet")
            run([py, script(c, "add_iucn.py"),
                 "--occurrences", clean_out, "--out", enriched, "--iucn", lookup,
                 "--iucn-name-col", cfg(c, "enrich.name_col", "scientificName"),
                 "--iucn-category-col", cfg(c, "enrich.category_col", "redlistCategory"),
                 "--iucn-id-col", cfg(c, "enrich.id_col", "internalTaxonId"),
                 "--species-col", cfg(c, "enrich.species_col", "species"),
                 "--keep-geometry"], args.dry_run)
            join_input = enriched
        else:
            log.warning("enrich enabled but no IUCN lookup and nothing to build one from; "
                        "threatened metrics will be OFF this run")

    # 3 — JOIN to tehsil (+ MWS) ----------------------------------
    tagged = os.path.join(work, "tagged.parquet")
    jo = [py, script(c, "build_dataset.py"), "--occurrences", join_input,
          "--out", tagged, "--chunk", cfg(c, "boundaries.chunk", 500000)]
    teh = cfg(c, "boundaries.tehsil")
    if teh:
        jo += ["--tehsil", teh]
        if cfg(c, "boundaries.tehsil_id_col"): jo += ["--tehsil-id-col", cfg(c, "boundaries.tehsil_id_col")]
    mws = cfg(c, "boundaries.mws")
    if mws:
        jo += ["--mws", mws]
        if cfg(c, "boundaries.mws_id_col"): jo += ["--mws-id-col", cfg(c, "boundaries.mws_id_col")]
    if cfg(c, "boundaries.snap_nearest", False):
        jo += ["--snap-nearest", "--snap-max-deg", cfg(c, "boundaries.snap_max_deg", 0.02)]
    run(jo, args.dry_run)

    # 4a — URBANIZATION time series (per tehsil / MWS x year) -----
    if cfg(c, "urban.enabled", True):
        root = os.path.join(out, "corelens_data")
        base = [py, script(c, "make_corelens_annual.py"), "--data", tagged,
                "--data-root", root,
                "--min-year", cfg(c, "urban.min_year", 1990),
                "--max-year", cfg(c, "urban.max_year", 2026),
                "--iucn-col", cfg(c, "urban.iucn_col", "iucn_category")]
        if teh:
            run(base + ["--key", cfg(c, "boundaries.tehsil_id_col", "tehsil_id")], args.dry_run)
        if mws:
            run(base + ["--key", cfg(c, "boundaries.mws_id_col", "mws_id"),
                        "--entity", "mws", "--partition-by-year"], args.dry_run)

    # 4b — MIGRATION time series (seasonal maps + phenology) ------
    if cfg(c, "migration.enabled", True):
        avonet = cfg(c, "migration.avonet")
        if avonet and os.path.exists(avonet):
            mm = [py, script(c, "migratory_maps_batch.py"), "--data", tagged,
                  "--avonet", avonet, "--min-points", cfg(c, "migration.min_points", 10000),
                  "--out-dir", os.path.join(out, "migration", "maps")]
            if cfg(c, "migration.avonet_species_col"):
                mm += ["--avonet-species-col", cfg(c, "migration.avonet_species_col")]
            if teh: mm += ["--boundaries", teh]
            run(mm, args.dry_run)
        else:
            log.warning("migration maps skipped: AVONET file not found (%s)", avonet)

        migs = cfg(c, "migration.phenology.migratory") or []
        ress = cfg(c, "migration.phenology.resident") or []
        if migs or ress:
            os.makedirs(os.path.join(out, "migration"), exist_ok=True)
            ph = [py, script(c, "phenology_compare.py"), "--data", tagged,
                  "--migratory", ",".join(migs), "--resident", ",".join(ress),
                  "--normalize", cfg(c, "migration.phenology.normalize", "effort"),
                  "--out", os.path.join(out, "migration", "phenology.png")]
            run(ph, args.dry_run)

    # 5 — DELETE OLD DATA ----------------------------------------
    if cfg(c, "retention.delete_raw_after", True) and not args.dry_run and not args.use_existing:
        log.info("deleting raw/intermediate data for this run: %s", work)
        shutil.rmtree(work, ignore_errors=True)
    prune_runs(cfg(c, "paths.work_dir", "data/raw"), cfg(c, "retention.keep_runs", 1), args)
    log.info("=== cycle %s done ===", stamp)


def prune_runs(work_root, keep, args):
    if not os.path.isdir(work_root):
        return
    runs = sorted(d for d in os.listdir(work_root) if d.startswith("run_"))
    for d in runs[:-keep] if keep > 0 else runs:
        p = os.path.join(work_root, d)
        log.info("pruning old run: %s", p)
        if not args.dry_run:
            shutil.rmtree(p, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--every", type=int, default=None,
                    help="loop forever, sleeping this many seconds between cycles")
    ap.add_argument("--use-existing", default=None,
                    help="skip the GBIF download and use this occurrence parquet/dir")
    ap.add_argument("--dry-run", action="store_true", help="print steps, run nothing")
    args = ap.parse_args()

    with open(args.config) as f:
        c = yaml.safe_load(f) or {}

    def cycle():
        try:
            one_cycle(c, args)
        except Exception as e:
            log.error("cycle failed: %s", e, exc_info=True)

    if args.every:
        log.info("scheduler: a cycle every %ds (Ctrl-C to stop)", args.every)
        while True:
            cycle()
            log.info("sleeping %ds until next cycle…", args.every)
            time.sleep(args.every)
    else:
        cycle()


if __name__ == "__main__":
    main()
