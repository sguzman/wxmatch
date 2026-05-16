#!/usr/bin/env bash
set -euo pipefail

PRIMARY_STATION="${PRIMARY_STATION:-KDSM}"
SECONDARY_STATION="${SECONDARY_STATION:-KDEN}"
HISTORICAL_DATE="${HISTORICAL_DATE:-2026-05-14}"
THRESHOLD_HIGH_F="${THRESHOLD_HIGH_F:-75}"

run() {
  echo
  echo "+ $*"
  "$@"
}

run cargo test

for station in "$PRIMARY_STATION" "$SECONDARY_STATION"; do
  run cargo run -- station inspect "$station" --format json
  run cargo run -- fetch station "$station" --source iem-asos-one-minute --start "$HISTORICAL_DATE" --end "$HISTORICAL_DATE"
  run cargo run -- fetch station "$station" --source ncei-asos-five-minute --start "$HISTORICAL_DATE" --end "$HISTORICAL_DATE"
  run cargo run -- fetch station "$station" --source ghcnh --start "$HISTORICAL_DATE" --end "$HISTORICAL_DATE"
  run cargo run -- normalize station "$station" --source iem-asos-one-minute
  run cargo run -- normalize station "$station" --source ncei-asos-five-minute
  run cargo run -- normalize station "$station" --source ghcnh
  run cargo run -- build daily "$station" --year 2026
  run cargo run -- build profiles "$station" --year 2026
  run cargo run -- query day "$station" "$HISTORICAL_DATE" --format json
  run cargo run -- query prob "$station" --date "$HISTORICAL_DATE" --threshold-high "$THRESHOLD_HIGH_F" --format json
  run cargo run -- query analogs "$station" --date "$HISTORICAL_DATE" --top 5 --format json
  run cargo run -- query prob "$station" --today --threshold-high "$THRESHOLD_HIGH_F" --format json
  run cargo run -- query analogs "$station" --today --top 3 --format json
done

run cargo run -- source list --format json
run cargo run -- station inspect "$PRIMARY_STATION" --format json
run cargo run -- station inspect "$SECONDARY_STATION" --format json
run cargo run -- cache doctor
