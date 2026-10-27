# GBIF biodiversity pipeline — automation

One cycle: **download** fresh GBIF data → **clean** it → **join** to tehsil/MWS
boundaries → generate the **urbanization** and **migration** time series →
**delete** the old raw data.

## Files
- `run_pipeline.py` — orchestrator (scheduling, chains every step, cleanup)
- `gbif_download.py` — automated GBIF Download API client (submit → poll → fetch)
- `clean.py` — quality/cleaning filters (specimen, captive/zoo, coords, year…)
- `config.yaml` — all paths, the bbox, cleaning toggles, species lists, retention
- (expects your existing scripts in `scripts/`: `build_dataset.py`,
  `make_corelens_annual.py`, `migratory_maps_batch.py`, `phenology_compare.py`)

## Setup
```bash
pip install duckdb pandas pyarrow geopandas shapely matplotlib numpy openpyxl requests polars pyyaml
export GBIF_USER=yourusername
export GBIF_PWD=yourpassword
# put your tehsil boundary at data/boundaries/static.parquet and AVONET at data/AVONET.xlsx
```

## Run
```bash
python run_pipeline.py --config config.yaml                 # one full cycle now
python run_pipeline.py --config config.yaml --every 3600    # loop hourly (for testing)
python run_pipeline.py --config config.yaml --use-existing occurrence.parquet  # skip download
python run_pipeline.py --config config.yaml --dry-run       # print steps, run nothing
```

## Schedule (production: monthly)
Prefer cron over `--every`:
```cron
# 02:00 on the 1st of every month    (hourly test: 0 * * * *)
0 2 1 * *  cd /path/to/repo && GBIF_USER=… GBIF_PWD=… /usr/bin/python3 run_pipeline.py --config config.yaml >> pipeline.log 2>&1
```

## What is kept vs deleted
- **Kept** (`outputs/`): the derived time series — this is the "database".
  - `outputs/corelens_data/tehsil/annual.parquet` (urbanization metrics per tehsil × year)
  - `outputs/migration/maps/<species>/…png` + `kept_species.csv`, `outputs/migration/phenology.png`
- **Deleted** each cycle (`data/raw/run_*`): the big raw download + cleaned +
  tagged intermediates (`retention.delete_raw_after`, `keep_runs`).

## Cleaning rules (all in `config.yaml → clean`)
| rule | drops |
|---|---|
| `basis` | specimen / fossil / **LIVING_SPECIMEN** (zoo/garden captive) |
| `captive` | `establishmentMeans` MANAGED / CULTIVATED / CAPTIVE |
| `block_institutions` | listed zoo/captive `institutionCode`s |
| `max_uncertainty_m` | points with coordinate uncertainty above the limit |
| coords | null-island (0,0) and out-of-range lat/lon |
| `present_only` | `occurrenceStatus` ≠ PRESENT |
| year range | implausible years |
| `drop_null_species` | records not identified to species (optional) |

Any rule whose column is absent from the download is skipped automatically.
Note: `establishmentMeans` needs that column in the download — it comes in
GBIF's `SIMPLE_PARQUET` when available; the cleaner just skips the rule if not.
