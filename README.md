# wxmatch

`wxmatch` is a Rust CLI for downloading, caching, normalizing, querying, and comparing station-level weather observations.

The project is aimed at a workflow like:

1. Fetch historical observations for a station.
2. Normalize them into one canonical observation model.
3. Build daily summaries and hourly day profiles.
4. Ask questions such as:
   - What happened on this day?
   - How likely is today to reach a target high?
   - Which historical days looked most like this one?

## Status

### Implemented now

- Async Rust CLI built with `clap`
- Structured console and rolling JSON file logging with `tracing`
- Stable cache layout under `.cache/wxmatch/`
- Station metadata fetch via the NWS API
- Historical adapters for `IEM ASOS 1-minute`, `NOAA/NCEI ASOS 5-minute`, and `GHCNh`
- Current observation fetch for `NWS API` with repeated snapshots preserved by timestamp
- Canonical normalized observations stored in Parquet
- Derived daily summaries and hourly day profiles stored in Parquet
- Source precedence for overlapping timestamps: `NCEI 5-minute` -> `IEM 1-minute` -> `NWS API` -> `GHCNh`
- `query day`
- `query prob`
- `query analogs`
- `--format text|json` for `source list`, `station inspect`, and query commands

### Planned next

- Richer cloud/precipitation coverage in historical ingestion
- Additional probability methods and more formal score blending
- More calibration and validation around mixed-cadence analog weighting
- Optional helper commands around DuckDB inspection and migration ergonomics

## Why These Sources

- `IEM ASOS 1-minute`: fastest path to useful high-frequency U.S. station history with a straightforward download interface.
- `NOAA/NCEI ASOS 5-minute`: authoritative U.S. archive for later validation and fallback.
- `NWS API`: current-station metadata and live observation snapshots for `today` workflows.
- `GHCNh`: lower-frequency but broad-coverage fallback for future non-ASOS expansion.

## Cache Philosophy

`wxmatch` keeps raw downloads and normalized outputs separate.

- Raw files are preserved for reproducibility and debugging.
- Normalized Parquet files are idempotent derived artifacts.
- Daily summaries and day profiles are Parquet datasets built from normalized observations.
- Logs are written under the cache so command runs are inspectable after the fact.

Default cache root:

```text
~/.cache/wxmatch/
```

Layout:

```text
.cache/wxmatch/
  sources/
    iem-asos-1min/
      raw/
      normalized/
    ncei-asos-5min/
      raw/
      normalized/
    nws-api/
      raw/
      normalized/
    ghcnh/
      raw/
      normalized/
  stations/
  derived/
  manifests/
  logs/
```

DuckDB-friendly dataset paths:

```text
sources/<source>/normalized/station=<id>/year=<yyyy>.parquet
derived/station=<id>/daily/year=<yyyy>.parquet
derived/station=<id>/profiles/year=<yyyy>.parquet
```

## CLI Shape

Top-level commands:

- `cache`
- `source`
- `station`
- `fetch`
- `normalize`
- `build`
- `query`

Examples that work today:

```bash
cargo run -- cache show
cargo run -- source list
cargo run -- source list --format json
cargo run -- station inspect KDSM
cargo run -- station inspect KDSM --format json
cargo run -- fetch station KDSM --source iem-asos-one-minute --start 2026-05-14 --end 2026-05-14
cargo run -- fetch station KDSM --source ncei-asos-five-minute --start 2026-05-14 --end 2026-05-14
cargo run -- fetch station KDSM --source ghcnh --start 2026-05-14 --end 2026-05-14
cargo run -- normalize station KDSM --source iem-asos-one-minute
cargo run -- normalize station KDSM --source ncei-asos-five-minute
cargo run -- build daily KDSM --year 2026
cargo run -- build profiles KDSM --year 2026
cargo run -- query day KDSM 2026-05-14
cargo run -- query day KDSM 2026-05-14 --format json
cargo run -- query prob KDSM --date 2026-05-14 --threshold-high 75
cargo run -- fetch current KDSM
cargo run -- query analogs KDSM --date 2026-05-14 --top 10
```

## Logging

Console output is human-readable by default, while file logs are written as JSON.

Useful flags:

```bash
cargo run -- -v source list
cargo run -- --log-format json source list
cargo run -- --log-filter 'wxmatch=debug,reqwest=info' source list
```

## Developer Quickstart

Build:

```bash
cargo check
```

Test:

```bash
cargo test
```

Run formatting:

```bash
cargo fmt
```

Inspect cached Parquet with DuckDB:

```bash
duckdb -c "select local_date, high_temp_c from '~/.cache/wxmatch/derived/station=KDSM/daily/year=2026.parquet' limit 5"
duckdb -c "select source, observed_at_utc, temperature_c from '~/.cache/wxmatch/sources/ncei-asos-5min/normalized/station=KDSM/year=2026.parquet' limit 5"
```

## Notes on v1 Behavior

- The IEM path still targets the fields reliably exposed by the 1-minute endpoint: temperature, dew point, wind direction, wind speed, and pressure.
- The NCEI 5-minute adapter parses METAR-style tokens from the NOAA archive and currently emphasizes temperature, dew point, wind, visibility, cloud code, and altimeter-derived pressure.
- The GHCNh adapter normalizes the stable hourly fields exposed by the PSV export and acts as a lower-resolution fallback.
- Relative humidity is derived during normalization when temperature and dew point are present.
- Current NWS observations add cloud cover, visibility, pressure, and gust fields when available, and repeated fetches keep multiple same-day raw snapshots.
- Analog matching is same-station only in the current implementation and uses hourly profiles built from normalized observations.

## Project Plan

The implementation blueprint lives in [docs/plan/bootstrap.md](docs/plan/bootstrap.md).
