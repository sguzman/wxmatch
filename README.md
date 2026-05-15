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
- Historical minute-level fetch for `IEM ASOS 1-minute`
- Current observation fetch for `NWS API`
- Canonical normalized observation records stored in JSON
- Daily summary builder
- Hourly day-profile builder
- `query day`
- `query prob`
- `query analogs`

### Planned next

- `NOAA/NCEI ASOS 5-minute` historical adapter
- `GHCNh` fallback adapter
- Richer cloud/precipitation coverage in historical ingestion
- Better current-day partial profile support from multiple live observations
- Additional probability methods and more formal score blending
- Machine-readable CLI output modes

## Why These Sources

- `IEM ASOS 1-minute`: fastest path to useful high-frequency U.S. station history with a straightforward download interface.
- `NOAA/NCEI ASOS 5-minute`: authoritative U.S. archive for later validation and fallback.
- `NWS API`: easy current-station metadata and latest observation access.
- `GHCNh`: lower-frequency but broad-coverage fallback for future non-ASOS expansion.

## Cache Philosophy

`wxmatch` keeps raw downloads and normalized outputs separate.

- Raw files are preserved for reproducibility and debugging.
- Normalized files are idempotent derived artifacts.
- Daily summaries and day profiles are built from normalized observations.
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
    nws-api/
      raw/
      normalized/
  stations/
  derived/
  manifests/
  logs/
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
cargo run -- station inspect KDSM
cargo run -- fetch station KDSM --source iem-asos-one-minute --start 2026-05-14 --end 2026-05-14
cargo run -- normalize station KDSM --source iem-asos-one-minute
cargo run -- build daily KDSM --year 2026
cargo run -- build profiles KDSM --year 2026
cargo run -- query day KDSM 2026-05-14
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

## Notes on v1 Behavior

- The first historical implementation targets the fields reliably exposed by the IEM 1-minute endpoint: temperature, dew point, wind direction, wind speed, and pressure.
- Relative humidity is derived during normalization when temperature and dew point are present.
- Current NWS observations add cloud cover, visibility, pressure, and gust fields when available.
- Analog matching is same-station only in v1 and uses hourly profiles built from normalized observations.

## Project Plan

The implementation blueprint lives in [docs/plan/bootstrap.md](docs/plan/bootstrap.md).
