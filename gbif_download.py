#!/usr/bin/env python3
"""Automated GBIF Download API client.

Submits a predicate, polls until GBIF has built the file server-side, downloads
and extracts it. Returns the path to the extracted `occurrence.parquet` directory.

Credentials come from the environment (a free GBIF account):
    export GBIF_USER=...   export GBIF_PWD=...

    python gbif_download.py --country IN \
        --bbox 68.0,6.0,98.0,38.0 --out-dir data/raw/run_2026_10 --format SIMPLE_PARQUET

    python gbif_download.py --dry-run ...   # just print the predicate, submit nothing
"""
from __future__ import annotations

import argparse
import io
import os
import time
import zipfile

import requests

REQUEST_URL = "https://api.gbif.org/v1/occurrence/download/request"
STATUS_URL = "https://api.gbif.org/v1/occurrence/download/{key}"


def build_predicate(country=None, bbox=None, present=True, has_coordinate=True,
                    no_geospatial_issue=True, extra=None):
    preds = []
    if country:
        preds.append({"type": "equals", "key": "COUNTRY", "value": country})
    if has_coordinate:
        preds.append({"type": "equals", "key": "HAS_COORDINATE", "value": "true"})
    if no_geospatial_issue:
        preds.append({"type": "equals", "key": "HAS_GEOSPATIAL_ISSUE", "value": "false"})
    if present:
        preds.append({"type": "equals", "key": "OCCURRENCE_STATUS", "value": "PRESENT"})
    if bbox:
        minx, miny, maxx, maxy = bbox
        preds += [
            {"type": "greaterThanOrEquals", "key": "DECIMAL_LATITUDE", "value": str(miny)},
            {"type": "lessThanOrEquals", "key": "DECIMAL_LATITUDE", "value": str(maxy)},
            {"type": "greaterThanOrEquals", "key": "DECIMAL_LONGITUDE", "value": str(minx)},
            {"type": "lessThanOrEquals", "key": "DECIMAL_LONGITUDE", "value": str(maxx)},
        ]
    if extra:
        preds += list(extra)
    return {"type": "and", "predicates": preds}


def submit(predicate, fmt="SIMPLE_PARQUET", user=None, pwd=None) -> str:
    user = user or os.environ["GBIF_USER"]
    pwd = pwd or os.environ["GBIF_PWD"]
    body = {"creator": user, "notificationAddresses": [], "sendNotification": False,
            "format": fmt, "predicate": predicate}
    r = requests.post(REQUEST_URL, auth=(user, pwd), json=body, timeout=60)
    r.raise_for_status()
    key = r.text.strip().strip('"')
    print(f"download requested: key={key}  (track: https://www.gbif.org/occurrence/download/{key})")
    return key


def wait(key, poll_seconds=30, timeout_seconds=None) -> str:
    url = STATUS_URL.format(key=key)
    start = time.time()
    while True:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        p = r.json()
        status = p["status"]
        print(f"  status: {status}")
        if status == "SUCCEEDED":
            print(f"  ready: {p.get('totalRecords','?')} records, "
                  f"{p.get('size',0)/1e6:.1f} MB compressed")
            return p["downloadLink"]
        if status in ("KILLED", "CANCELLED", "FAILED"):
            raise RuntimeError(f"download {key} ended with status={status}")
        if timeout_seconds and (time.time() - start) > timeout_seconds:
            raise TimeoutError(f"download {key} not ready after {timeout_seconds}s (status={status})")
        time.sleep(poll_seconds)


def fetch(download_link, out_dir) -> str:
    os.makedirs(out_dir, exist_ok=True)
    print(f"downloading {download_link} -> {out_dir}")
    r = requests.get(download_link, timeout=1800)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        zf.extractall(out_dir)
    occ = os.path.join(out_dir, "occurrence.parquet")
    return occ if os.path.exists(occ) else out_dir


def download(country=None, bbox=None, out_dir="data/raw", fmt="SIMPLE_PARQUET",
             poll_seconds=30, timeout_seconds=None, dry_run=False) -> str | None:
    pred = build_predicate(country=country, bbox=bbox)
    if dry_run:
        import json
        print(json.dumps(pred, indent=2))
        return None
    key = submit(pred, fmt=fmt)
    link = wait(key, poll_seconds=poll_seconds, timeout_seconds=timeout_seconds)
    return fetch(link, out_dir)


def _parse_bbox(s):
    if not s:
        return None
    return tuple(float(x) for x in s.split(","))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--country", default="IN")
    ap.add_argument("--bbox", default=None, help="min_lon,min_lat,max_lon,max_lat (omit for whole country)")
    ap.add_argument("--out-dir", default="data/raw")
    ap.add_argument("--format", default="SIMPLE_PARQUET")
    ap.add_argument("--poll-seconds", type=int, default=30)
    ap.add_argument("--timeout-seconds", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true", help="print the predicate and exit")
    args = ap.parse_args()
    path = download(country=args.country, bbox=_parse_bbox(args.bbox), out_dir=args.out_dir,
                    fmt=args.format, poll_seconds=args.poll_seconds,
                    timeout_seconds=args.timeout_seconds, dry_run=args.dry_run)
    if path:
        print(f"occurrences at: {path}")


if __name__ == "__main__":
    main()
