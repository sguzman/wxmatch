# wxmatch Bootstrap and Product Blueprint

## Summary

`wxmatch` is a station-level weather cache and query CLI built around reproducible downloads, normalized observations, derived daily/profile datasets, and analog-day analysis.

This document is the working implementation spec for the current bootstrap and the next major product phases.

## Current State

- The crate builds and tests.
- CLI shape is stable.
- `IEM ASOS 1-minute` historical fetch is implemented.
- `NWS API` station metadata and current/latest fetch are implemented.
- Normalized observations are written as JSON arrays.
- Daily summaries and hourly day profiles are implemented.
- Probability and analog queries are implemented at a practical v1 level.

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

## Source Order

1. `IEM ASOS 1-minute`
2. `NWS API`
3. `NOAA/NCEI ASOS 5-minute`
4. `GHCNh`

## Source Adapter Interface

Each adapter should support:

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
sources/<source>/normalized/...
stations/...
derived/...
manifests/...
logs/...
```

Rules:

- raw files are immutable
- normalized files are rebuildable
- derived files are keyed by station and year
- manifests record source, schema version, generation time, and raw path

## Implemented v1 Behavior

### Historical path

1. Fetch raw minute-level CSV from IEM.
2. Cache raw CSV by station and date window.
3. Normalize into canonical observation records.
4. Build daily summaries and day profiles by year.

### Current-day path

1. Fetch station metadata from NWS.
2. Fetch latest observation from NWS.
3. Normalize and cache that observation.
4. Use it for `today`-style queries when possible.

### Query methods

- `query day`: prints daily summary and profile availability
- `query prob`: seasonal climatology plus analog-based estimate when enough profile data exists
- `query analogs`: same-station nearest-neighbor analog search over hourly profiles

## Validation Rules

- end date must not be earlier than start date
- station ids are normalized to uppercase ICAO-like identifiers
- IEM historical requests use the 3-letter stripped form when appropriate
- local timestamps are derived from cached NWS station timezone metadata
- wind similarity uses vector components, not direct direction deltas
- cloud cover fractions are derived from METAR layer codes when available

## Acceptance Criteria

The bootstrap is considered successful when all of the following work:

- `fetch station ... --source iem-asos-one-minute`
- `normalize station ... --source iem-asos-one-minute`
- `build daily ...`
- `build profiles ...`
- `query day ...`
- `query prob ...`
- `fetch current ...`
- `query analogs ...`

And:

- `cargo check` passes
- `cargo test` passes
- structured logs are emitted
- cache artifacts are written to the documented layout

## Next Phases

### Phase 2

- add `NOAA/NCEI ASOS 5-minute`
- add richer provenance and overlap handling
- improve manifests and source precedence

### Phase 3

- add `GHCNh`
- expand cross-station and broader geography support

### Phase 4

- richer partial-day modeling
- more live observations per day
- JSON output mode
- more probability methods and calibration
