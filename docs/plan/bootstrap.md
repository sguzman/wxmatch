# wxmatch Bootstrap and Product Blueprint

## Summary

`wxmatch` is a station-level weather cache and query CLI built around reproducible downloads, canonical weather observations, derived daily/profile datasets, and analog-day analysis.

This document is the working implementation spec for the current repository state and the remaining roadmap work.

## Current State

- The crate builds, formats, and tests.
- CLI shape is stable around `cache`, `source`, `station`, `fetch`, `normalize`, `build`, and `query`.
- `IEM ASOS 1-minute` historical fetch and normalization are implemented.
- `NOAA/NCEI ASOS 5-minute` historical fetch and normalization are implemented.
- `NWS API` station metadata and current/latest fetch are implemented, with repeated snapshots preserved by timestamp.
- `GHCNh` historical fetch and normalization are implemented.
- Normalized observations are written as Parquet datasets keyed by station and year.
- Daily summaries and hourly day profiles are written as Parquet datasets keyed by station and year.
- Query commands support text and JSON output.
- Dataset manifests cover normalized and derived Parquet artifacts.

## Canonical Data Model

Primary normalized observation fields:

- station id
- source
- source station id
- UTC timestamp
- local timestamp
- local date
- minute of day
- temperature in C
- dew point in C
- relative humidity percent
- wind speed in kt
- wind gust in kt
- wind direction in degrees
- wind `u` and `v` in kt
- precipitation in mm when available
- pressure and sea-level pressure in hPa when available
- visibility in km when available
- cloud cover code and derived fraction when available
- raw artifact reference
- quality flags

Derived datasets:

- `DailySummary`
- `DayProfile`
- `ProbabilityBreakdown`
- `AnalogResult`

## Source Precedence

When multiple normalized observations exist for the same station and UTC timestamp, `wxmatch` keeps one canonical record using this priority order:

1. `NOAA/NCEI ASOS 5-minute`
2. `IEM ASOS 1-minute`
3. `NWS API`
4. `GHCNh`

This prevents overlapping source windows from double-counting the same moment in time.

## Source Adapter Interface

Each adapter supports:

- station metadata fetch
- historical fetch when applicable
- current/latest fetch when applicable
- raw normalization into canonical observations
- provenance metadata writes

Stable internal trait:

```rust
trait WeatherSourceAdapter
```

## Storage Contract

Cache root:

```text
.cache/wxmatch/
```

Stable layout:

```text
sources/<source>/raw/...
sources/<source>/normalized/station=<id>/year=<yyyy>.parquet
stations/<station>.json
derived/station=<id>/daily/year=<yyyy>.parquet
derived/station=<id>/profiles/year=<yyyy>.parquet
manifests/*.json
logs/...
```

Rules:

- raw files are immutable and source-specific
- normalized Parquet files are rebuildable from raw
- derived Parquet files are rebuildable from normalized data
- manifests remain JSON and record source, schema version, generation time, row counts, coverage, warnings, and artifact/input paths
- rebuild-from-raw is the preferred migration path from legacy JSON normalized/derived artifacts
- dataset paths are intentionally DuckDB-friendly

## Implemented Runtime Behavior

### Historical path

1. Fetch raw archive artifacts from one or more historical providers.
2. Cache raw artifacts by station and date window.
3. Normalize raw rows into canonical observations.
4. Group normalized observations into yearly Parquet datasets.
5. Build yearly daily summaries and day profiles.

### Current-day path

1. Fetch station metadata from NWS.
2. Fetch latest observation from NWS.
3. Preserve each raw observation snapshot by timestamp.
4. Normalize and merge into yearly Parquet datasets.
5. Use that partial-day data for `today`-style queries.

### Query methods

- `query day`: daily summary and observed-hour coverage
- `query prob`: weighted multi-method output across:
  - `seasonal-climatology`
  - `temperature-trajectory`
  - `partial-profile-analogs`
  - `nearest-neighbor-analogs`
- query outputs carry `quality_state` plus freshness/status notes where applicable
- `query analogs`: same-station nearest-neighbor analog search over hourly profiles with same-cadence preference
- `source list`, `station inspect`, and `cache manifests`: cache/source/provenance inspection in text or JSON
- `query duckdb-paths`: prints normalized/derived Parquet locations and example DuckDB commands

### Combination policy

- Default fixed weights:
  - climatology `0.25`
  - trajectory `0.20`
  - partial-profile analogs `0.30`
  - nearest-neighbor analogs `0.25`
- Combined probability is emitted only when at least two methods are available.
- Weights are renormalized across the available methods only.
- Current-day output includes a freshness or stale-data note based on the newest same-day NWS observation.
- Mixed-cadence candidate pools are explicitly marked when `GHCNh` contributes.
- Sparse current-day target profiles surface explicit unavailability reasons instead of silently omitting methods.

## Validation Rules

- end date must not be earlier than start date
- station ids are normalized to uppercase ICAO-like identifiers
- IEM historical requests use station-local day semantics
- local timestamps are derived from cached NWS station timezone metadata
- overlapping timestamps are resolved by explicit source precedence
- wind similarity uses vector components, not direct direction deltas
- cloud cover fractions are derived from METAR layer codes when available
- partial-day analogs require at least 2 matched hours
- full-profile analogs require at least 6 matched hours
- same-cadence analog candidates rank ahead of mixed-cadence fallback candidates
- if the richer IEM optional-field request is rejected, the adapter retries with the core field set

## Acceptance Criteria

The current bootstrap is considered successful when all of the following work:

- `fetch station ... --source iem-asos-one-minute`
- `fetch station ... --source ncei-asos-five-minute`
- `fetch station ... --source ghcnh`
- `normalize station ... --source iem-asos-one-minute`
- `normalize station ... --source ncei-asos-five-minute`
- `normalize station ... --source ghcnh`
- `build daily ...`
- `build profiles ...`
- `query day ...`
- `query prob ...`
- `fetch current ...`
- `query analogs ...`
- `cache manifests ...`
- `query duckdb-paths ...`

And:

- `cargo check` passes
- `cargo test` passes
- structured logs are emitted
- normalized and derived cache artifacts are written as Parquet
- cache artifacts are directly queryable from DuckDB
- atomic writes prevent torn Parquet/manifest reads during concurrent `today` queries
- the checked-in acceptance matrix succeeds on `KDSM` and `KDEN`

Acceptance runner:

```bash
bash scripts/acceptance_matrix.sh
```

## Remaining Work

- improve calibration and validation against larger historical backfills
- improve current-day modeling quality with denser same-day live snapshots
- increase precip/cloud-layer coverage where upstream archives expose richer detail
- refine provenance detail around overlap reconciliation and dataset aging
