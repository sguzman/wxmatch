use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use chrono::{Datelike, Local, NaiveDate, Timelike};
use serde::Serialize;
use serde_json::json;
use tracing::{debug, info, instrument, warn};

use crate::app::App;
use crate::cache::CacheLayout;
use crate::cli::{
    AnalogsArgs, BuildCommand, BuildSubcommand, CacheCommand, CacheSubcommand, Cli, Command,
    FetchCommand, FetchCurrentArgs, FetchStationArgs, NormalizeCommand, NormalizeSubcommand,
    OutputFormat, ProbabilityArgs, QueryCommand, QuerySubcommand, SourceCommand, SourceSubcommand,
    StationCommand, StationSubcommand,
};
use crate::domain::{
    DailySummary, DailySummaryRow, DatasetManifest, DayProfile, DayProfileRow, ObservationRecord,
    ObservationRow, SCHEMA_VERSION, StationId, celsius_to_fahrenheit,
};
use crate::engine::{
    DailySummaryBuilder, DayProfileBuilder, DerivedDatasetBuilder, build_probability_breakdown,
    dedupe_observations, minimum_analog_hours, probability_quality_state, target_date_or_today,
    top_analogs,
};
use crate::source::{DataSource, SourceDescriptor, all_sources};
use crate::sources::WeatherSourceAdapter;
use crate::storage::{list_files_recursive, read_json, read_parquet, write_json, write_parquet};

pub async fn dispatch(app: &App, cli: Cli) -> Result<()> {
    let format = cli.format;
    match cli.command {
        Command::Cache(command) => handle_cache(format, app, command).await,
        Command::Source(command) => handle_source(format, app, command).await,
        Command::Station(command) => handle_station(format, app, command).await,
        Command::Fetch(command) => handle_fetch(app, command).await,
        Command::Normalize(command) => handle_normalize(app, command).await,
        Command::Build(command) => handle_build(app, command).await,
        Command::Query(command) => handle_query(format, app, command).await,
    }
}

fn print_json<T: Serialize>(value: &T) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).context("failed to serialize JSON output")?
    );
    Ok(())
}

#[instrument(skip(app))]
async fn handle_cache(format: OutputFormat, app: &App, command: CacheCommand) -> Result<()> {
    match command.command {
        CacheSubcommand::Init => {
            app.cache.ensure_exists()?;
            println!("cache initialized at {}", app.cache.root.display());
        }
        CacheSubcommand::Show => print_cache_layout(&app.cache),
        CacheSubcommand::Doctor => run_cache_doctor(&app.cache)?,
        CacheSubcommand::Manifests { station } => {
            list_manifests(format, app, station.as_deref()).await?;
        }
    }

    Ok(())
}

#[instrument(skip(app))]
async fn handle_source(format: OutputFormat, app: &App, command: SourceCommand) -> Result<()> {
    backfill_cached_manifests(app)?;
    match command.command {
        SourceSubcommand::List => {
            let descriptors = all_sources()
                .into_iter()
                .map(SourceDescriptor::from_source)
                .map(|descriptor| {
                    let normalized_root = app.cache.source_root(descriptor.source).join("normalized");
                    let raw_root = app.cache.source_root(descriptor.source).join("raw");
                    let manifest_root = app.cache.manifests_dir.as_path();
                    let normalized_manifest_count = count_matching_files(
                        manifest_root,
                        "json",
                        &format!("normalized-{}-", descriptor.source.slug()),
                    )
                    .unwrap_or(0);
                    let normalized_manifests =
                        source_normalized_manifests(app, descriptor.source).unwrap_or_default();
                    let coverage_years = manifest_years(&normalized_manifests);
                    let manifest_warnings = manifest_warnings(&normalized_manifests);
                    let role = source_role(descriptor.source);
                    json!({
                        "source": descriptor.source.slug(),
                        "slug": descriptor.slug,
                        "cadence": descriptor.cadence,
                        "scope": descriptor.scope,
                        "summary": descriptor.summary,
                        "role": role,
                        "raw_files": count_files(&raw_root, source_raw_extension(descriptor.source)).unwrap_or(0),
                        "normalized_parquet_files": count_files(&normalized_root, "parquet").unwrap_or(0),
                        "normalized_manifests": normalized_manifest_count,
                        "coverage_years": coverage_years,
                        "warnings": manifest_warnings,
                    })
                })
                .collect::<Vec<_>>();
            if format == OutputFormat::Json {
                print_json(&descriptors)?;
            } else {
                for descriptor in descriptors {
                    println!(
                        "{slug:16}  role={role:20} cadence={cadence:12} scope={scope:18} raw={raw_files:4} normalized={normalized_files:4} manifests={manifest_files:4} years={years} {summary}",
                        slug = descriptor["slug"].as_str().unwrap_or_default(),
                        role = descriptor["role"].as_str().unwrap_or_default(),
                        cadence = descriptor["cadence"].as_str().unwrap_or_default(),
                        scope = descriptor["scope"].as_str().unwrap_or_default(),
                        raw_files = descriptor["raw_files"].as_u64().unwrap_or_default(),
                        normalized_files = descriptor["normalized_parquet_files"]
                            .as_u64()
                            .unwrap_or_default(),
                        manifest_files = descriptor["normalized_manifests"]
                            .as_u64()
                            .unwrap_or_default(),
                        years = format_years_json(&descriptor["coverage_years"]),
                        summary = descriptor["summary"].as_str().unwrap_or_default(),
                    );
                    if let Some(warnings) = descriptor["warnings"].as_array() {
                        for warning in warnings {
                            println!("  warning: {}", warning.as_str().unwrap_or_default());
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

#[instrument(skip(app))]
async fn handle_station(format: OutputFormat, app: &App, command: StationCommand) -> Result<()> {
    backfill_cached_manifests(app)?;
    match command.command {
        StationSubcommand::Inspect { station } => {
            let station_id = StationId::new(&station);
            let station_record = app.sources.nws.fetch_station_metadata(&station_id).await?;
            let raw_iem = count_files(
                &app.cache
                    .source_root(DataSource::IemAsosOneMinute)
                    .join(format!("raw/station={station_id}")),
                "csv",
            )?;
            let normalized_iem = count_files(
                &app.cache
                    .source_root(DataSource::IemAsosOneMinute)
                    .join(format!("normalized/station={station_id}")),
                "parquet",
            )?;
            let normalized_nws = count_files(
                &app.cache
                    .source_root(DataSource::NwsApi)
                    .join(format!("normalized/station={station_id}")),
                "parquet",
            )?;
            let normalized_ncei = count_files(
                &app.cache
                    .source_root(DataSource::NceiAsosFiveMinute)
                    .join(format!("normalized/station={station_id}")),
                "parquet",
            )?;
            let normalized_ghcnh = count_files(
                &app.cache
                    .source_root(DataSource::Ghcnh)
                    .join(format!("normalized/station={station_id}")),
                "parquet",
            )?;
            let daily_years = count_files(
                &app.cache
                    .derived_dir
                    .join(format!("station={station_id}/daily")),
                "parquet",
            )?;
            let profile_years = count_files(
                &app.cache
                    .derived_dir
                    .join(format!("station={station_id}/profiles")),
                "parquet",
            )?;
            let normalized_manifests = count_manifest_files_for_station(
                &app.cache.manifests_dir,
                "normalized-",
                &station_id,
            )?;
            let derived_manifests = count_manifest_files_for_station(
                &app.cache.manifests_dir,
                "derived-",
                &station_id,
            )?;
            let normalized_manifest_payloads = station_normalized_manifests(app, &station_id)?;
            let derived_manifest_payloads = station_derived_manifests(app, &station_id)?;
            let freshness_note =
                current_day_freshness_note(app, &station_id, Local::now().date_naive())?;
            let output = json!({
                "station": station_record.station_id,
                "source_station_id": station_record.source_station_id,
                "name": station_record.name,
                "timezone": station_record.timezone,
                "latitude": station_record.latitude,
                "longitude": station_record.longitude,
                "elevation_m": station_record.elevation_m,
                "provider": station_record.provider.unwrap_or_else(|| "unknown".to_owned()),
                "station_cache": app.cache.station_metadata_path(&station_id).display().to_string(),
                "current_day_status": {
                    "freshness_note": freshness_note,
                },
                "source_coverage": source_coverage_summary(app, &station_id)?,
                "cache_status": {
                    "iem_raw_files": raw_iem,
                    "iem_normalized_files": normalized_iem,
                    "nws_normalized_files": normalized_nws,
                    "ncei_normalized_files": normalized_ncei,
                    "ghcnh_normalized_files": normalized_ghcnh,
                    "daily_years_built": daily_years,
                    "profile_years_built": profile_years,
                    "normalized_manifests": normalized_manifests,
                    "derived_manifests": derived_manifests,
                    "normalized_manifest_warnings": manifest_warnings(&normalized_manifest_payloads),
                    "derived_manifest_years": manifest_years(&derived_manifest_payloads),
                }
            });
            if format == OutputFormat::Json {
                print_json(&output)?;
            } else {
                println!(
                    "station: {}",
                    output["station"].as_str().unwrap_or_default()
                );
                println!(
                    "source station id: {}",
                    output["source_station_id"].as_str().unwrap_or_default()
                );
                println!("name: {}", output["name"].as_str().unwrap_or_default());
                println!(
                    "timezone: {}",
                    output["timezone"].as_str().unwrap_or_default()
                );
                println!(
                    "location: {}, {}",
                    output["latitude"].as_f64().unwrap_or_default(),
                    output["longitude"].as_f64().unwrap_or_default()
                );
                if let Some(elevation) = output["elevation_m"].as_f64() {
                    println!("elevation_m: {:.1}", elevation);
                }
                println!(
                    "provider: {}",
                    output["provider"].as_str().unwrap_or_default()
                );
                println!(
                    "station cache: {}",
                    output["station_cache"].as_str().unwrap_or_default()
                );
                println!("iem raw files: {raw_iem}");
                println!("iem normalized files: {normalized_iem}");
                println!("nws normalized files: {normalized_nws}");
                println!("ncei normalized files: {normalized_ncei}");
                println!("ghcnh normalized files: {normalized_ghcnh}");
                println!("daily years built: {daily_years}");
                println!("profile years built: {profile_years}");
                println!("normalized manifests: {normalized_manifests}");
                println!("derived manifests: {derived_manifests}");
                if let Some(note) = output["current_day_status"]["freshness_note"].as_str() {
                    println!("current-day status: {note}");
                }
                if let Some(coverage) = output["source_coverage"].as_array() {
                    for source in coverage {
                        println!(
                            "{}: years={} warnings={}",
                            source["source"].as_str().unwrap_or_default(),
                            format_years_json(&source["coverage_years"]),
                            source["warnings"]
                                .as_array()
                                .map(|warnings| warnings.len())
                                .unwrap_or_default()
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

#[instrument(skip(app))]
async fn handle_fetch(app: &App, command: FetchCommand) -> Result<()> {
    match command.command {
        crate::cli::FetchSubcommand::Station(args) => fetch_station(app, args).await,
        crate::cli::FetchSubcommand::Current(args) => fetch_current(app, args).await,
    }
}

#[instrument(skip(app))]
async fn handle_normalize(app: &App, command: NormalizeCommand) -> Result<()> {
    match command.command {
        NormalizeSubcommand::Station { station, source } => {
            normalize_station(app, &StationId::new(&station), source).await
        }
    }
}

#[instrument(skip(app))]
async fn handle_build(app: &App, command: BuildCommand) -> Result<()> {
    match command.command {
        BuildSubcommand::Daily { station, year } => {
            build_daily(app, &StationId::new(&station), year).await
        }
        BuildSubcommand::Profiles { station, year } => {
            build_profiles(app, &StationId::new(&station), year).await
        }
    }
}

#[instrument(skip(app))]
async fn handle_query(format: OutputFormat, app: &App, command: QueryCommand) -> Result<()> {
    match command.command {
        QuerySubcommand::Day { station, date } => {
            query_day(format, app, &StationId::new(&station), date).await
        }
        QuerySubcommand::Today { station } => {
            query_day(
                format,
                app,
                &StationId::new(&station),
                Local::now().date_naive(),
            )
            .await
        }
        QuerySubcommand::Prob(args) => query_probability(format, app, args).await,
        QuerySubcommand::Analogs(args) => query_analogs(format, app, args).await,
        QuerySubcommand::DuckdbPaths { station, year } => {
            query_duckdb_paths(format, app, station.as_deref(), year).await
        }
    }
}

#[instrument(skip(app))]
async fn fetch_station(app: &App, args: FetchStationArgs) -> Result<()> {
    if args.end < args.start {
        bail!("--end must not be earlier than --start");
    }

    let station_id = StationId::new(&args.station);
    let adapter = app.sources.adapter(args.source);
    let station = adapter.fetch_station_metadata(&station_id).await?;
    let result = adapter
        .fetch_historical(&station_id, args.start, args.end, args.refresh)
        .await?;

    println!("station: {}", station.station_id);
    println!("source: {}", args.source.slug());
    println!("window: {} -> {}", args.start, args.end);
    println!("raw path: {}", result.path);
    println!("bytes: {}", result.byte_count);
    println!(
        "cache: {}",
        if result.reused {
            "reused"
        } else {
            "downloaded"
        }
    );
    Ok(())
}

#[instrument(skip(app))]
async fn fetch_current(app: &App, args: FetchCurrentArgs) -> Result<()> {
    let station_id = StationId::new(&args.station);
    let station = app.sources.nws.fetch_station_metadata(&station_id).await?;
    let result = app.sources.nws.fetch_current(&station_id, false).await?;
    let normalized = app
        .sources
        .nws
        .normalize_raw_file(Path::new(&result.path), &station)?;
    let current_year = Local::now().year();
    let normalized_path = app
        .cache
        .normalized_path(DataSource::NwsApi, &station_id, current_year);
    let added = merge_observations_into_year(&normalized_path, normalized)?;

    println!("station: {}", station.station_id);
    println!("source: {}", DataSource::NwsApi.slug());
    println!("raw path: {}", result.path);
    println!("normalized path: {}", normalized_path.display());
    println!("observations added: {added}");
    Ok(())
}

#[instrument(skip(app))]
async fn normalize_station(app: &App, station_id: &StationId, source: DataSource) -> Result<()> {
    let adapter = app.sources.adapter(source);
    let station = adapter.fetch_station_metadata(station_id).await?;
    let raw_root = app
        .cache
        .source_root(source)
        .join(format!("raw/station={station_id}"));
    let raw_ext = match source {
        DataSource::IemAsosOneMinute => "csv",
        DataSource::NwsApi => "json",
        DataSource::NceiAsosFiveMinute => "dat",
        DataSource::Ghcnh => "psv",
    };
    let raw_files = list_files_recursive(&raw_root, raw_ext)?;
    if raw_files.is_empty() {
        bail!("no raw files found under {}", raw_root.display());
    }

    let mut grouped_by_year: BTreeMap<i32, Vec<ObservationRecord>> = BTreeMap::new();
    for raw_path in raw_files {
        let observations = adapter.normalize_raw_file(&raw_path, &station)?;
        for observation in observations {
            grouped_by_year
                .entry(observation.local_date.year())
                .or_default()
                .push(observation);
        }
        info!(
            source = source.slug(),
            station = %station_id,
            raw = %raw_path.display(),
            observations = grouped_by_year.values().map(Vec::len).sum::<usize>(),
            "normalized raw file"
        );
    }

    let mut normalized_total = 0usize;
    let years = collect_years(&grouped_by_year, None)?;
    for year in years {
        let normalized_path = app.cache.normalized_path(source, station_id, year);
        let observations = grouped_by_year.remove(&year).unwrap_or_default();
        normalized_total += observations.len();
        let _added = merge_observations_into_year(&normalized_path, observations)?;
        let merged_observations = read_observation_records(&normalized_path)?;
        write_normalized_manifest(
            app,
            source,
            station_id,
            year,
            &normalized_path,
            &merged_observations,
            input_paths_for_year(source, station_id, year, app)?,
        )?;
        info!(
            source = source.slug(),
            station = %station_id,
            year,
            normalized = %normalized_path.display(),
            "wrote normalized parquet year"
        );
    }

    println!("station: {station_id}");
    println!("source: {}", source.slug());
    println!("normalized observations: {normalized_total}");
    println!(
        "normalized root: {}",
        app.cache
            .source_root(source)
            .join(format!("normalized/station={station_id}"))
            .display()
    );
    Ok(())
}

#[instrument(skip(app))]
async fn build_daily(app: &App, station_id: &StationId, year: Option<i32>) -> Result<()> {
    ensure_normalized_station(app, station_id).await?;
    let observations = load_station_observations(app, station_id)?;
    let builder = DailySummaryBuilder;
    let summaries = builder.build(&observations)?;
    let grouped = group_daily_by_year(&summaries);
    write_grouped_summaries(app, station_id, grouped, year)?;
    Ok(())
}

#[instrument(skip(app))]
async fn build_profiles(app: &App, station_id: &StationId, year: Option<i32>) -> Result<()> {
    ensure_normalized_station(app, station_id).await?;
    let observations = load_station_observations(app, station_id)?;
    let builder = DayProfileBuilder;
    let profiles = builder.build(&observations)?;
    let grouped = group_profiles_by_year(&profiles);
    write_grouped_profiles(app, station_id, grouped, year)?;
    Ok(())
}

#[instrument(skip(app))]
async fn query_day(
    format: OutputFormat,
    app: &App,
    station_id: &StationId,
    date: NaiveDate,
) -> Result<()> {
    if date == Local::now().date_naive() {
        ensure_today_current_data(app, station_id).await?;
    }
    ensure_derived(app, station_id, date.year()).await?;
    let daily = load_daily_for_year(app, station_id, date.year())?;
    let profiles = load_profiles_for_year(app, station_id, date.year())?;
    let summary = daily
        .iter()
        .find(|summary| summary.local_date == date)
        .context("no daily summary found for date")?;
    let profile = profiles
        .iter()
        .find(|profile| profile.local_date == date)
        .context("no day profile found for date")?;
    let freshness_note = current_day_freshness_note(app, station_id, date)?;
    let quality_state = quality_state_for_day(profile.observed_hour_count, &summary.source_slugs);
    let quality_note = quality_note_for_day(profile.observed_hour_count, &summary.source_slugs);

    let output = json!({
        "station": station_id.to_string(),
        "date": date,
        "observations": summary.observation_count,
        "high_f": summary.high_temp_c.map(celsius_to_fahrenheit),
        "low_f": summary.low_temp_c.map(celsius_to_fahrenheit),
        "mean_f": summary.mean_temp_c.map(celsius_to_fahrenheit),
        "observed_hours": profile.observed_hour_count,
        "sources": summary.source_slugs,
        "freshness_note": freshness_note,
        "quality_state": quality_state,
        "quality_note": quality_note,
    });
    if format == OutputFormat::Json {
        print_json(&output)?;
    } else {
        println!("station: {station_id}");
        println!("date: {date}");
        println!("observations: {}", summary.observation_count);
        if let Some(high) = summary.high_temp_c {
            println!("high: {:.1}F", celsius_to_fahrenheit(high));
        }
        if let Some(low) = summary.low_temp_c {
            println!("low: {:.1}F", celsius_to_fahrenheit(low));
        }
        if let Some(mean) = summary.mean_temp_c {
            println!("mean: {:.1}F", celsius_to_fahrenheit(mean));
        }
        println!("observed hours: {}", profile.observed_hour_count);
        println!(
            "quality state: {}",
            output["quality_state"].as_str().unwrap_or_default()
        );
        if let Some(sources) = output["sources"].as_array() {
            let sources = sources
                .iter()
                .filter_map(|value| value.as_str())
                .collect::<Vec<_>>()
                .join(",");
            if !sources.is_empty() {
                println!("sources: {sources}");
            }
        }
        if let Some(freshness_note) = output["freshness_note"].as_str() {
            println!("freshness: {freshness_note}");
        }
        if let Some(quality_note) = output["quality_note"].as_str() {
            println!("quality: {quality_note}");
        }
    }
    Ok(())
}

#[instrument(skip(app))]
async fn query_probability(format: OutputFormat, app: &App, args: ProbabilityArgs) -> Result<()> {
    let target_date = target_date_or_today(args.date, args.today)?;
    let station_id = StationId::new(&args.station);
    if target_date == Local::now().date_naive() {
        ensure_today_current_data(app, &station_id).await?;
    }
    ensure_derived(app, &station_id, target_date.year()).await?;
    let daily = load_all_daily(app, &station_id)?;
    let profiles = load_all_profiles(app, &station_id)?;
    let target_profile = resolve_target_profile(app, &station_id, target_date, &profiles)?;
    let as_of_hour = args.as_of.map(|time| time.hour() as u8);
    let breakdown = build_probability_breakdown(
        station_id.clone(),
        target_date,
        crate::domain::fahrenheit_to_celsius(f64::from(args.threshold_high)),
        &daily,
        &profiles,
        target_profile.as_ref(),
        as_of_hour,
    );
    let freshness_note = current_day_freshness_note(app, &station_id, target_date)?;
    let quality_note = target_profile.as_ref().and_then(|profile| {
        quality_note_for_day(profile.observed_hour_count, &profile.source_slugs)
    });

    if format == OutputFormat::Json {
        let output = json!({
            "breakdown": breakdown,
            "freshness_note": freshness_note,
            "quality_note": quality_note,
        });
        print_json(&output)?;
    } else {
        println!("station: {}", breakdown.station_id);
        println!("date: {}", breakdown.target_date);
        println!("threshold high: {:.1}F", args.threshold_high);
        println!("quality state: {}", breakdown.quality_state);
        for method in &breakdown.methods {
            println!(
                "{}: {:.1}% (n={}){}{}{}",
                method.method,
                method.probability * 100.0,
                method.sample_size,
                method
                    .weight_used
                    .map(|weight| format!(" weight={weight:.2}"))
                    .unwrap_or_default(),
                method
                    .confidence_note
                    .as_ref()
                    .map(|note| format!(" confidence={note}"))
                    .unwrap_or_default(),
                method
                    .note
                    .as_ref()
                    .map(|note| format!(" [{note}]"))
                    .unwrap_or_default()
            );
        }
        for unavailable in &breakdown.unavailable_methods {
            println!(
                "{}: unavailable [{}]",
                unavailable.method, unavailable.reason
            );
        }
        if let Some(combined) = &breakdown.combined {
            println!(
                "combined: {:.1}% (methods={} [{}])",
                combined.probability * 100.0,
                combined.method_count,
                combined.combination_note
            );
        } else {
            println!("combined: unavailable (need at least two methods)");
        }
        if let Some(freshness_note) = freshness_note {
            println!("freshness: {freshness_note}");
        }
        if let Some(quality_note) = quality_note {
            println!("quality: {quality_note}");
        }
    }
    Ok(())
}

#[instrument(skip(app))]
async fn query_analogs(format: OutputFormat, app: &App, args: AnalogsArgs) -> Result<()> {
    let target_date = target_date_or_today(args.date, args.today)?;
    let station_id = StationId::new(&args.station);
    if target_date == Local::now().date_naive() {
        ensure_today_current_data(app, &station_id).await?;
    }
    ensure_derived(app, &station_id, target_date.year()).await?;
    let daily = load_all_daily(app, &station_id)?;
    let profiles = load_all_profiles(app, &station_id)?;
    let target_profile = resolve_target_profile(app, &station_id, target_date, &profiles)?
        .context("no target profile available for analog search")?;
    let as_of_hour = args.as_of.map(|time| time.hour() as u8);
    let analogs = top_analogs(
        &station_id,
        target_date,
        &daily,
        &profiles,
        &target_profile,
        as_of_hour,
        args.top,
    );
    let freshness_note = current_day_freshness_note(app, &station_id, target_date)?;
    let quality_state = probability_quality_state(
        target_date,
        Some(&target_profile),
        &daily,
        &profiles,
        as_of_hour,
    );
    let quality_note = quality_note_for_day(
        target_profile.observed_hour_count,
        &target_profile.source_slugs,
    );
    let status_note = if analogs.is_empty() {
        if target_profile.observed_hour_count < minimum_analog_hours(as_of_hour) {
            Some(format!(
                "insufficient observed hours for analog search: {} observed, {} required",
                target_profile.observed_hour_count,
                minimum_analog_hours(as_of_hour)
            ))
        } else {
            Some(format!(
                "no comparable analogs found for target profile with {} observed hours",
                target_profile.observed_hour_count
            ))
        }
    } else {
        None
    };

    if format == OutputFormat::Json {
        let output = json!({
            "analogs": analogs,
            "freshness_note": freshness_note,
            "status_note": status_note,
            "target_observed_hours": target_profile.observed_hour_count,
            "quality_state": quality_state,
            "quality_note": quality_note,
        });
        print_json(&output)?;
    } else {
        println!("station: {station_id}");
        println!("date: {target_date}");
        println!("top analogs: {}", analogs.len());
        println!(
            "target observed hours: {}",
            target_profile.observed_hour_count
        );
        println!("quality state: {quality_state}");
        if let Some(freshness_note) = freshness_note {
            println!("freshness: {freshness_note}");
        }
        if let Some(quality_note) = quality_note {
            println!("quality: {quality_note}");
        }
        if let Some(status_note) = status_note {
            println!("status: {status_note}");
        }
        for analog in analogs {
            let high = analog
                .observed_high_c
                .map(celsius_to_fahrenheit)
                .map(|value| format!("{value:.1}F"))
                .unwrap_or_else(|| "n/a".to_owned());
            println!(
                "{}  distance={:.3} high={} compared_hours={} sources={}{}",
                analog.analog_date,
                analog.distance,
                high,
                analog.compared_hours,
                analog.candidate_source_summary,
                analog
                    .source_mix_note
                    .as_ref()
                    .map(|note| format!(" [{note}]"))
                    .unwrap_or_default()
            );
        }
    }
    Ok(())
}

fn run_cache_doctor(cache: &CacheLayout) -> Result<()> {
    println!("cache root: {}", cache.root.display());
    for dir in cache.directories() {
        let exists = dir.exists();
        println!(
            "{:>10}  {}",
            if exists { "ok" } else { "missing" },
            dir.display()
        );
    }

    let manifest = cache.bootstrap_manifest_path();
    println!(
        "{:>10}  {}",
        if manifest.exists() { "ok" } else { "missing" },
        manifest.display()
    );
    for path in list_files_recursive(&cache.sources_dir, "parquet")? {
        let source = source_from_artifact_path(&path);
        let station_id = station_id_from_artifact_path(&path);
        let year = year_from_artifact_path(&path);
        if let (Some(source), Some(station_id), Some(year)) = (source, station_id, year) {
            let manifest = cache.normalized_manifest_path(source, &station_id, year);
            if !manifest.exists() {
                println!(
                    "   warning  missing normalized manifest for {}",
                    path.display()
                );
            }
        }
    }
    for path in list_files_recursive(&cache.derived_dir, "parquet")? {
        let station_id = station_id_from_artifact_path(&path);
        let year = year_from_artifact_path(&path);
        if let (Some(station_id), Some(year)) = (station_id, year) {
            let kind = if path.to_string_lossy().contains("/daily/") {
                "daily"
            } else {
                "profiles"
            };
            let manifest = cache.derived_manifest_path(kind, &station_id, year);
            if !manifest.exists() {
                println!(
                    "   warning  missing derived manifest for {}",
                    path.display()
                );
                continue;
            }
            let manifest_data: DatasetManifest = read_json(&manifest)?;
            let manifest_time = std::fs::metadata(&manifest)?.modified().ok();
            for input in &manifest_data.input_paths {
                if let Ok(metadata) = std::fs::metadata(input) {
                    if metadata
                        .modified()
                        .ok()
                        .zip(manifest_time)
                        .is_some_and(|(input_time, output_time)| input_time > output_time)
                    {
                        println!(
                            "   warning  derived manifest {} is older than input {}",
                            manifest.display(),
                            input
                        );
                    }
                }
                let input_path = Path::new(input);
                let source = source_from_artifact_path(input_path);
                let input_station_id = station_id_from_artifact_path(input_path);
                let input_year = year_from_artifact_path(input_path);
                if let (Some(source), Some(input_station_id), Some(input_year), Some(output_time)) =
                    (source, input_station_id, input_year, manifest_time)
                {
                    let input_manifest =
                        cache.normalized_manifest_path(source, &input_station_id, input_year);
                    if let Ok(metadata) = std::fs::metadata(&input_manifest) {
                        if metadata
                            .modified()
                            .ok()
                            .is_some_and(|time| time > output_time)
                        {
                            println!(
                                "   warning  derived manifest {} is older than normalized manifest {}",
                                manifest.display(),
                                input_manifest.display()
                            );
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn print_cache_layout(cache: &CacheLayout) {
    println!("root: {}", cache.root.display());
    println!("sources: {}", cache.sources_dir.display());
    println!("stations: {}", cache.stations_dir.display());
    println!("derived: {}", cache.derived_dir.display());
    println!("manifests: {}", cache.manifests_dir.display());
    println!("logs: {}", cache.logs_dir.display());
}

fn load_station_observations(app: &App, station_id: &StationId) -> Result<Vec<ObservationRecord>> {
    let mut observations = Vec::new();
    for source in [
        DataSource::NceiAsosFiveMinute,
        DataSource::IemAsosOneMinute,
        DataSource::NwsApi,
        DataSource::Ghcnh,
    ] {
        let root = app
            .cache
            .source_root(source)
            .join(format!("normalized/station={station_id}"));
        for path in list_files_recursive(&root, "parquet")? {
            let mut file_observations = read_observation_records(&path)?;
            debug!(path = %path.display(), count = file_observations.len(), "loaded normalized observation file");
            observations.append(&mut file_observations);
        }
    }
    observations = dedupe_observations(observations);
    if observations.is_empty() {
        bail!("no normalized observations found for station {station_id}");
    }
    Ok(observations)
}

fn load_daily_for_year(
    app: &App,
    station_id: &StationId,
    year: i32,
) -> Result<Vec<crate::domain::DailySummary>> {
    read_daily_summaries(&app.cache.daily_summary_path(station_id, year))
}

fn load_profiles_for_year(
    app: &App,
    station_id: &StationId,
    year: i32,
) -> Result<Vec<crate::domain::DayProfile>> {
    read_day_profiles(&app.cache.day_profile_path(station_id, year))
}

fn load_all_daily(app: &App, station_id: &StationId) -> Result<Vec<crate::domain::DailySummary>> {
    let root = app
        .cache
        .derived_dir
        .join(format!("station={station_id}/daily"));
    let mut all = Vec::new();
    for path in list_files_recursive(&root, "parquet")? {
        let mut values = read_daily_summaries(&path)?;
        all.append(&mut values);
    }
    if all.is_empty() {
        bail!("no daily summaries found for station {station_id}");
    }
    all.sort_by_key(|summary| summary.local_date);
    Ok(all)
}

fn load_all_profiles(app: &App, station_id: &StationId) -> Result<Vec<crate::domain::DayProfile>> {
    let root = app
        .cache
        .derived_dir
        .join(format!("station={station_id}/profiles"));
    let mut all = Vec::new();
    for path in list_files_recursive(&root, "parquet")? {
        let mut values = read_day_profiles(&path)?;
        all.append(&mut values);
    }
    if all.is_empty() {
        bail!("no day profiles found for station {station_id}");
    }
    all.sort_by_key(|profile| profile.local_date);
    Ok(all)
}

fn group_daily_by_year(
    summaries: &[crate::domain::DailySummary],
) -> BTreeMap<i32, Vec<crate::domain::DailySummary>> {
    let mut grouped = BTreeMap::new();
    for summary in summaries {
        grouped
            .entry(summary.local_date.year())
            .or_insert_with(Vec::new)
            .push(summary.clone());
    }
    grouped
}

fn group_profiles_by_year(
    profiles: &[crate::domain::DayProfile],
) -> BTreeMap<i32, Vec<crate::domain::DayProfile>> {
    let mut grouped = BTreeMap::new();
    for profile in profiles {
        grouped
            .entry(profile.local_date.year())
            .or_insert_with(Vec::new)
            .push(profile.clone());
    }
    grouped
}

fn write_grouped_summaries(
    app: &App,
    station_id: &StationId,
    grouped: BTreeMap<i32, Vec<crate::domain::DailySummary>>,
    year: Option<i32>,
) -> Result<()> {
    let years = collect_years(&grouped, year)?;
    for year in years {
        let path = app.cache.daily_summary_path(station_id, year);
        let values = grouped.get(&year).cloned().unwrap_or_default();
        write_daily_summaries(&path, &values)?;
        write_derived_manifest(
            app,
            "daily",
            station_id,
            year,
            &path,
            values.len(),
            values.iter().map(|row| row.local_date).min(),
            values.iter().map(|row| row.local_date).max(),
        )?;
        println!("daily summaries {} -> {}", year, path.display());
    }
    Ok(())
}

fn write_grouped_profiles(
    app: &App,
    station_id: &StationId,
    grouped: BTreeMap<i32, Vec<crate::domain::DayProfile>>,
    year: Option<i32>,
) -> Result<()> {
    let years = collect_years(&grouped, year)?;
    for year in years {
        let path = app.cache.day_profile_path(station_id, year);
        let values = grouped.get(&year).cloned().unwrap_or_default();
        write_day_profiles(&path, &values)?;
        write_profile_manifest(app, station_id, year, &path, &values)?;
        println!("day profiles {} -> {}", year, path.display());
    }
    Ok(())
}

fn collect_years<T>(grouped: &BTreeMap<i32, Vec<T>>, year: Option<i32>) -> Result<Vec<i32>> {
    match year {
        Some(year) => Ok(vec![year]),
        None => {
            let years = grouped.keys().copied().collect::<Vec<_>>();
            if years.is_empty() {
                bail!("no derived data was produced");
            }
            Ok(years)
        }
    }
}

async fn ensure_derived(app: &App, station_id: &StationId, year: i32) -> Result<()> {
    ensure_normalized_station(app, station_id).await?;
    backfill_derived_manifests(app, station_id, year)?;
    if !app.cache.daily_summary_path(station_id, year).exists() {
        build_daily(app, station_id, Some(year)).await?;
    }
    if !app.cache.day_profile_path(station_id, year).exists() {
        build_profiles(app, station_id, Some(year)).await?;
    }
    Ok(())
}

async fn ensure_normalized_station(app: &App, station_id: &StationId) -> Result<()> {
    for source in all_sources() {
        let normalized_root = app
            .cache
            .source_root(source)
            .join(format!("normalized/station={station_id}"));
        if count_files(&normalized_root, "parquet")? > 0 {
            backfill_normalized_manifests(app, source, station_id)?;
            continue;
        }

        let raw_root = app
            .cache
            .source_root(source)
            .join(format!("raw/station={station_id}"));
        if count_files(&raw_root, source_raw_extension(source))? == 0 {
            continue;
        }

        info!(station = %station_id, source = source.slug(), "rebuilding parquet datasets from cached raw source files");
        normalize_station(app, station_id, source).await?;
    }
    Ok(())
}

async fn ensure_today_current_data(app: &App, station_id: &StationId) -> Result<()> {
    let today = Local::now().date_naive();
    let normalized_path = app
        .cache
        .normalized_path(DataSource::NwsApi, station_id, today.year());

    info!(station = %station_id, date = %today, "refreshing current-day observation for today query");
    let station = app.sources.nws.fetch_station_metadata(station_id).await?;
    let result = app.sources.nws.fetch_current(station_id, false).await?;
    let normalized = app
        .sources
        .nws
        .normalize_raw_file(Path::new(&result.path), &station)?;
    let _lock = acquire_path_lock(&normalized_path)?;
    let added = merge_observations_into_year(&normalized_path, normalized)?;
    let existing_today_count = if normalized_path.exists() {
        read_observations_for_date(&normalized_path, today)?.len()
    } else {
        0
    };
    if added > 0
        || !app
            .cache
            .daily_summary_path(station_id, today.year())
            .exists()
        || !app
            .cache
            .day_profile_path(station_id, today.year())
            .exists()
    {
        info!(
            station = %station_id,
            date = %today,
            added,
            observations_for_day = existing_today_count,
            "rebuilding derived datasets after current-day refresh"
        );
        build_daily(app, station_id, Some(today.year())).await?;
        build_profiles(app, station_id, Some(today.year())).await?;
    }
    Ok(())
}

fn count_files(root: &Path, ext: &str) -> Result<usize> {
    Ok(list_files_recursive(root, ext)?.len())
}

fn count_matching_files(root: &Path, ext: &str, needle: &str) -> Result<usize> {
    Ok(list_files_recursive(root, ext)?
        .into_iter()
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.contains(needle))
        })
        .count())
}

fn count_manifest_files_for_station(
    root: &Path,
    prefix: &str,
    station_id: &StationId,
) -> Result<usize> {
    Ok(list_files_recursive(root, "json")?
        .into_iter()
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with(prefix) && name.contains(&format!("-{station_id}-"))
                })
        })
        .count())
}

fn resolve_target_profile(
    app: &App,
    station_id: &StationId,
    target_date: NaiveDate,
    profiles: &[crate::domain::DayProfile],
) -> Result<Option<crate::domain::DayProfile>> {
    if let Some(profile) = profiles
        .iter()
        .find(|profile| profile.local_date == target_date)
    {
        return Ok(Some(profile.clone()));
    }

    if target_date == Local::now().date_naive() {
        let current_path =
            app.cache
                .normalized_path(DataSource::NwsApi, station_id, target_date.year());
        if current_path.exists() {
            let observations = read_observations_for_date(&current_path, target_date)?;
            let builder = DayProfileBuilder;
            let mut profiles = builder.build(&observations)?;
            return Ok(profiles.pop());
        }
    }

    Ok(None)
}

fn source_raw_extension(source: DataSource) -> &'static str {
    match source {
        DataSource::IemAsosOneMinute => "csv",
        DataSource::NceiAsosFiveMinute => "dat",
        DataSource::NwsApi => "json",
        DataSource::Ghcnh => "psv",
    }
}

fn write_observation_records(path: &Path, observations: &[ObservationRecord]) -> Result<()> {
    let rows = observations
        .iter()
        .map(ObservationRow::from_observation)
        .collect::<Result<Vec<_>>>()?;
    write_parquet(path, &rows)
}

fn read_observation_records(path: &Path) -> Result<Vec<ObservationRecord>> {
    let rows: Vec<ObservationRow> = read_parquet(path)?;
    rows.into_iter()
        .map(ObservationRow::into_observation)
        .collect::<Result<Vec<_>>>()
}

fn merge_observations_into_year(
    path: &Path,
    observations: Vec<ObservationRecord>,
) -> Result<usize> {
    let existing = if path.exists() {
        match read_observation_records(path) {
            Ok(rows) => rows,
            Err(error) => {
                let quarantine_path = quarantine_corrupt_artifact(path)?;
                warn!(
                    path = %path.display(),
                    quarantine_path = %quarantine_path.display(),
                    error = %error,
                    "existing normalized parquet was unreadable; quarantined and rebuilding from new observations"
                );
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };
    let existing_count = existing.len();
    let merged = dedupe_observations(existing.into_iter().chain(observations).collect());
    let merged_count = merged.len();
    write_observation_records(path, &merged)?;
    Ok(merged_count.saturating_sub(existing_count))
}

fn read_observations_for_date(
    path: &Path,
    target_date: NaiveDate,
) -> Result<Vec<ObservationRecord>> {
    Ok(read_observation_records(path)?
        .into_iter()
        .filter(|observation| observation.local_date == target_date)
        .collect())
}

fn write_daily_summaries(path: &Path, summaries: &[DailySummary]) -> Result<()> {
    let rows = summaries
        .iter()
        .map(DailySummaryRow::from_summary)
        .collect::<Vec<_>>();
    write_parquet(path, &rows)
}

fn read_daily_summaries(path: &Path) -> Result<Vec<DailySummary>> {
    let rows: Vec<DailySummaryRow> = read_parquet(path)?;
    rows.into_iter()
        .map(DailySummaryRow::into_summary)
        .collect::<Result<Vec<_>>>()
}

fn write_day_profiles(path: &Path, profiles: &[DayProfile]) -> Result<()> {
    let rows = profiles
        .iter()
        .flat_map(DayProfileRow::from_profile)
        .collect::<Vec<_>>();
    write_parquet(path, &rows)
}

fn read_day_profiles(path: &Path) -> Result<Vec<DayProfile>> {
    let rows: Vec<DayProfileRow> = read_parquet(path)?;
    DayProfileRow::into_profiles(rows)
}

async fn list_manifests(format: OutputFormat, app: &App, station: Option<&str>) -> Result<()> {
    backfill_cached_manifests(app)?;
    let station_filter = station.map(StationId::new);
    let mut manifests = list_files_recursive(&app.cache.manifests_dir, "json")?;
    manifests.retain(|path| {
        station_filter.as_ref().is_none_or(|station_id| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.contains(&station_id.to_string()))
        })
    });
    manifests.sort();
    let entries = manifests
        .into_iter()
        .map(|path| {
            let name = path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or_default()
                .to_owned();
            let kind = manifest_kind(&name);
            if kind == "bootstrap" {
                Ok(json!({
                    "kind": kind,
                    "path": path.display().to_string(),
                }))
            } else {
                let payload = read_json::<serde_json::Value>(&path)?;
                Ok(json!({
                    "kind": kind,
                    "path": path.display().to_string(),
                    "manifest": payload,
                }))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    if format == OutputFormat::Json {
        print_json(&entries)?;
    } else {
        for entry in entries {
            let kind = entry["kind"].as_str().unwrap_or_default();
            let path = entry["path"].as_str().unwrap_or_default();
            if let Some(manifest) = entry.get("manifest") {
                println!(
                    "{kind:18} station={} year={} rows={} path={path}",
                    manifest["station_id"].as_str().unwrap_or("-"),
                    manifest["year"].as_i64().unwrap_or_default(),
                    manifest["row_count"].as_u64().unwrap_or_default(),
                );
            } else {
                println!("{kind:18} path={path}");
            }
        }
    }
    Ok(())
}

async fn query_duckdb_paths(
    format: OutputFormat,
    app: &App,
    station: Option<&str>,
    year: Option<i32>,
) -> Result<()> {
    backfill_cached_manifests(app)?;
    let station_filter = station.map(StationId::new);
    let mut normalized = Vec::new();
    let mut derived = Vec::new();
    for source in all_sources() {
        let root = app.cache.source_root(source).join("normalized");
        for path in list_files_recursive(&root, "parquet")? {
            if station_filter.as_ref().is_some_and(|station_id| {
                station_id_from_artifact_path(&path).as_ref() != Some(station_id)
            }) {
                continue;
            }
            if year.is_some_and(|year| year_from_artifact_path(&path) != Some(year)) {
                continue;
            }
            normalized.push(json!({
                "source": source.slug(),
                "path": path.display().to_string(),
            }));
        }
    }
    for path in list_files_recursive(&app.cache.derived_dir, "parquet")? {
        if station_filter.as_ref().is_some_and(|station_id| {
            station_id_from_artifact_path(&path).as_ref() != Some(station_id)
        }) {
            continue;
        }
        if year.is_some_and(|year| year_from_artifact_path(&path) != Some(year)) {
            continue;
        }
        derived.push(json!({
            "kind": if path.to_string_lossy().contains("/daily/") { "daily" } else { "profiles" },
            "path": path.display().to_string(),
        }));
    }
    let examples = normalized
        .first()
        .map(|entry| {
            vec![
                format!(
                    "duckdb -c \"select * from '{}' limit 5\"",
                    entry["path"].as_str().unwrap_or_default()
                ),
                format!(
                    "duckdb -c \"describe select * from '{}'\"",
                    entry["path"].as_str().unwrap_or_default()
                ),
            ]
        })
        .unwrap_or_default();
    let output = json!({
        "normalized": normalized,
        "derived": derived,
        "examples": examples,
    });
    if format == OutputFormat::Json {
        print_json(&output)?;
    } else {
        println!("normalized:");
        for entry in output["normalized"].as_array().unwrap_or(&Vec::new()) {
            println!(
                "{}  {}",
                entry["source"].as_str().unwrap_or_default(),
                entry["path"].as_str().unwrap_or_default()
            );
        }
        println!("derived:");
        for entry in output["derived"].as_array().unwrap_or(&Vec::new()) {
            println!(
                "{}  {}",
                entry["kind"].as_str().unwrap_or_default(),
                entry["path"].as_str().unwrap_or_default()
            );
        }
        for example in output["examples"].as_array().unwrap_or(&Vec::new()) {
            println!("{}", example.as_str().unwrap_or_default());
        }
    }
    Ok(())
}

fn input_paths_for_year(
    source: DataSource,
    station_id: &StationId,
    year: i32,
    app: &App,
) -> Result<Vec<String>> {
    let root = app
        .cache
        .source_root(source)
        .join(format!("raw/station={station_id}"));
    let year_prefix = year.to_string();
    Ok(list_files_recursive(&root, source_raw_extension(source))?
        .into_iter()
        .filter(|path| path.to_string_lossy().contains(&year_prefix))
        .map(|path| path.display().to_string())
        .collect())
}

fn backfill_normalized_manifests(
    app: &App,
    source: DataSource,
    station_id: &StationId,
) -> Result<()> {
    let root = app
        .cache
        .source_root(source)
        .join(format!("normalized/station={station_id}"));
    for path in list_files_recursive(&root, "parquet")? {
        let Some(year) = year_from_artifact_path(&path) else {
            continue;
        };
        let manifest_path = app.cache.normalized_manifest_path(source, station_id, year);
        if manifest_path.exists() && !dataset_manifest_needs_refresh(&manifest_path)? {
            continue;
        }
        let observations = read_observation_records(&path)?;
        write_normalized_manifest(
            app,
            source,
            station_id,
            year,
            &path,
            &observations,
            input_paths_for_year(source, station_id, year, app)?,
        )?;
    }
    Ok(())
}

fn backfill_cached_manifests(app: &App) -> Result<()> {
    for source in all_sources() {
        let normalized_root = app.cache.source_root(source).join("normalized");
        for path in list_files_recursive(&normalized_root, "parquet")? {
            let Some(station_id) = station_id_from_artifact_path(&path) else {
                continue;
            };
            let Some(year) = year_from_artifact_path(&path) else {
                continue;
            };
            let manifest_path = app
                .cache
                .normalized_manifest_path(source, &station_id, year);
            if manifest_path.exists() && !dataset_manifest_needs_refresh(&manifest_path)? {
                continue;
            }
            let observations = read_observation_records(&path)?;
            write_normalized_manifest(
                app,
                source,
                &station_id,
                year,
                &path,
                &observations,
                input_paths_for_year(source, &station_id, year, app)?,
            )?;
        }
    }

    let derived_root = app.cache.derived_dir.clone();
    for path in list_files_recursive(&derived_root, "parquet")? {
        let Some(station_id) = station_id_from_artifact_path(&path) else {
            continue;
        };
        let Some(year) = year_from_artifact_path(&path) else {
            continue;
        };
        if path.to_string_lossy().contains("/daily/") {
            let manifest_path = app.cache.derived_manifest_path("daily", &station_id, year);
            if !manifest_path.exists() || dataset_manifest_needs_refresh(&manifest_path)? {
                let values = read_daily_summaries(&path)?;
                write_derived_manifest(
                    app,
                    "daily",
                    &station_id,
                    year,
                    &path,
                    values.len(),
                    values.iter().map(|row| row.local_date).min(),
                    values.iter().map(|row| row.local_date).max(),
                )?;
            }
        } else if path.to_string_lossy().contains("/profiles/") {
            let manifest_path = app
                .cache
                .derived_manifest_path("profiles", &station_id, year);
            if !manifest_path.exists() || dataset_manifest_needs_refresh(&manifest_path)? {
                let profiles = read_day_profiles(&path)?;
                write_profile_manifest(app, &station_id, year, &path, &profiles)?;
            }
        }
    }

    Ok(())
}

fn backfill_derived_manifests(app: &App, station_id: &StationId, year: i32) -> Result<()> {
    let daily_path = app.cache.daily_summary_path(station_id, year);
    let daily_manifest = app.cache.derived_manifest_path("daily", station_id, year);
    if daily_path.exists()
        && (!daily_manifest.exists() || dataset_manifest_needs_refresh(&daily_manifest)?)
    {
        let values = read_daily_summaries(&daily_path)?;
        write_derived_manifest(
            app,
            "daily",
            station_id,
            year,
            &daily_path,
            values.len(),
            values.iter().map(|row| row.local_date).min(),
            values.iter().map(|row| row.local_date).max(),
        )?;
    }

    let profile_path = app.cache.day_profile_path(station_id, year);
    let profile_manifest = app
        .cache
        .derived_manifest_path("profiles", station_id, year);
    if profile_path.exists()
        && (!profile_manifest.exists() || dataset_manifest_needs_refresh(&profile_manifest)?)
    {
        let profiles = read_day_profiles(&profile_path)?;
        write_profile_manifest(app, station_id, year, &profile_path, &profiles)?;
    }
    Ok(())
}

fn year_from_artifact_path(path: &Path) -> Option<i32> {
    path.file_stem()
        .and_then(|value| value.to_str())
        .and_then(|value| value.strip_prefix("year="))
        .and_then(|value| value.parse::<i32>().ok())
}

fn station_id_from_artifact_path(path: &Path) -> Option<StationId> {
    path.ancestors()
        .filter_map(|ancestor| ancestor.file_name().and_then(|value| value.to_str()))
        .find_map(|value| value.strip_prefix("station=").map(StationId::new))
}

fn source_from_artifact_path(path: &Path) -> Option<DataSource> {
    path.ancestors()
        .filter_map(|ancestor| ancestor.file_name().and_then(|value| value.to_str()))
        .find_map(DataSource::from_slug)
}

fn manifest_kind(file_name: &str) -> &'static str {
    if file_name.starts_with("normalized-") {
        "normalized"
    } else if file_name.starts_with("derived-") {
        "derived"
    } else if file_name.starts_with("fetch-") {
        "fetch"
    } else if file_name == "bootstrap.json" {
        "bootstrap"
    } else {
        "unknown"
    }
}

fn dataset_manifest_needs_refresh(path: &Path) -> Result<bool> {
    let value = read_json::<serde_json::Value>(path)?;
    Ok(value.get("warnings").is_none() || value.get("source_count").is_none())
}

fn all_normalized_inputs_for_year(app: &App, station_id: &StationId, year: i32) -> Vec<String> {
    all_sources()
        .into_iter()
        .map(|source| app.cache.normalized_path(source, station_id, year))
        .filter(|path| path.exists())
        .map(|path| path.display().to_string())
        .collect()
}

fn write_normalized_manifest(
    app: &App,
    source: DataSource,
    station_id: &StationId,
    year: i32,
    artifact_path: &Path,
    observations: &[ObservationRecord],
    input_paths: Vec<String>,
) -> Result<()> {
    let start_date = observations.iter().map(|row| row.local_date).min();
    let end_date = observations.iter().map(|row| row.local_date).max();
    let manifest = DatasetManifest {
        dataset_kind: "normalized-observations".to_owned(),
        source: Some(source),
        station_id: station_id.to_string(),
        year,
        schema_version: SCHEMA_VERSION.to_owned(),
        generated_at_utc: chrono::Utc::now(),
        row_count: observations.len(),
        start_date,
        end_date,
        artifact_path: artifact_path.display().to_string(),
        input_paths,
        warnings: normalized_manifest_warnings(source, observations),
        source_count: observations
            .iter()
            .map(|row| row.source.slug())
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
    };
    write_json(
        &app.cache.normalized_manifest_path(source, station_id, year),
        &manifest,
    )
}

fn write_derived_manifest(
    app: &App,
    dataset_kind: &str,
    station_id: &StationId,
    year: i32,
    artifact_path: &Path,
    row_count: usize,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
) -> Result<()> {
    let manifest = DatasetManifest {
        dataset_kind: dataset_kind.to_owned(),
        source: None,
        station_id: station_id.to_string(),
        year,
        schema_version: SCHEMA_VERSION.to_owned(),
        generated_at_utc: chrono::Utc::now(),
        row_count,
        start_date,
        end_date,
        artifact_path: artifact_path.display().to_string(),
        input_paths: all_normalized_inputs_for_year(app, station_id, year),
        warnings: Vec::new(),
        source_count: all_sources()
            .into_iter()
            .filter(|source| {
                app.cache
                    .normalized_path(*source, station_id, year)
                    .exists()
            })
            .count(),
    };
    write_json(
        &app.cache
            .derived_manifest_path(dataset_kind, station_id, year),
        &manifest,
    )
}

fn write_profile_manifest(
    app: &App,
    station_id: &StationId,
    year: i32,
    artifact_path: &Path,
    profiles: &[DayProfile],
) -> Result<()> {
    let start_date = profiles.iter().map(|row| row.local_date).min();
    let end_date = profiles.iter().map(|row| row.local_date).max();
    let row_count = profiles.iter().map(|profile| profile.hours.len()).sum();
    write_derived_manifest(
        app,
        "profiles",
        station_id,
        year,
        artifact_path,
        row_count,
        start_date,
        end_date,
    )
}

fn normalized_manifest_warnings(
    source: DataSource,
    observations: &[ObservationRecord],
) -> Vec<String> {
    let mut warnings = Vec::new();
    if source == DataSource::Ghcnh {
        warnings.push(
            "hourly fallback cadence; analog and probability methods may be lower confidence"
                .to_owned(),
        );
    }
    if observations.iter().any(|row| {
        row.quality_flags
            .iter()
            .any(|flag| matches!(flag, crate::domain::QualityFlag::SourceFieldMissing(_)))
    }) {
        warnings.push(
            "one or more optional normalized fields were limited by source availability or parser coverage".to_owned(),
        );
    }
    warnings
}

fn current_day_freshness_note(
    app: &App,
    station_id: &StationId,
    target_date: NaiveDate,
) -> Result<Option<String>> {
    if target_date != Local::now().date_naive() {
        return Ok(None);
    }
    let path = app
        .cache
        .normalized_path(DataSource::NwsApi, station_id, target_date.year());
    if !path.exists() {
        return Ok(Some(
            "no same-day NWS observation has been normalized yet".to_owned(),
        ));
    }
    let latest = read_observations_for_date(&path, target_date)?
        .into_iter()
        .map(|row| row.observed_at_utc)
        .max();
    let Some(latest) = latest else {
        return Ok(Some(
            "no same-day NWS observation has been normalized yet".to_owned(),
        ));
    };
    let age = chrono::Utc::now() - latest;
    let minutes = age.num_minutes();
    if minutes > 90 {
        Ok(Some(format!(
            "stale current-day data: latest NWS observation is {minutes} minutes old ({latest})"
        )))
    } else {
        Ok(Some(format!(
            "latest NWS observation age: {minutes} minutes ({latest})"
        )))
    }
}

fn quality_state_for_day(observed_hours: usize, source_slugs: &[String]) -> String {
    if observed_hours < minimum_analog_hours(None) {
        "sparse-current-day".to_owned()
    } else if source_slugs.iter().any(|slug| slug == "ghcnh") {
        "mixed-cadence".to_owned()
    } else {
        "normal".to_owned()
    }
}

fn quality_note_for_day(observed_hours: usize, source_slugs: &[String]) -> Option<String> {
    if source_slugs.len() == 1 && source_slugs.first().is_some_and(|slug| slug == "nws-api") {
        return Some(
            "low-history / partial-day target currently relies only on NWS API observations"
                .to_owned(),
        );
    }
    if observed_hours < minimum_analog_hours(None) {
        return Some(format!(
            "partial-day target has only {observed_hours} observed hours"
        ));
    }
    None
}

fn source_role(source: DataSource) -> &'static str {
    match source {
        DataSource::IemAsosOneMinute => "historical-fast-path",
        DataSource::NceiAsosFiveMinute => "historical-authoritative",
        DataSource::NwsApi => "current-day-live",
        DataSource::Ghcnh => "fallback-hourly",
    }
}

fn source_normalized_manifests(app: &App, source: DataSource) -> Result<Vec<DatasetManifest>> {
    list_files_recursive(&app.cache.manifests_dir, "json")?
        .into_iter()
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(&format!("normalized-{}-", source.slug())))
        })
        .map(|path| read_json(&path))
        .collect()
}

fn station_normalized_manifests(app: &App, station_id: &StationId) -> Result<Vec<DatasetManifest>> {
    list_files_recursive(&app.cache.manifests_dir, "json")?
        .into_iter()
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with("normalized-") && name.contains(&format!("-{station_id}-"))
                })
        })
        .map(|path| read_json(&path))
        .collect()
}

fn station_derived_manifests(app: &App, station_id: &StationId) -> Result<Vec<DatasetManifest>> {
    list_files_recursive(&app.cache.manifests_dir, "json")?
        .into_iter()
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with("derived-") && name.contains(&format!("-{station_id}-"))
                })
        })
        .map(|path| read_json(&path))
        .collect()
}

fn manifest_years(manifests: &[DatasetManifest]) -> Vec<i32> {
    let mut years = manifests
        .iter()
        .map(|manifest| manifest.year)
        .collect::<Vec<_>>();
    years.sort();
    years.dedup();
    years
}

fn manifest_warnings(manifests: &[DatasetManifest]) -> Vec<String> {
    let mut warnings = manifests
        .iter()
        .flat_map(|manifest| manifest.warnings.iter().cloned())
        .collect::<Vec<_>>();
    warnings.sort();
    warnings.dedup();
    warnings
}

fn format_years_json(value: &serde_json::Value) -> String {
    value
        .as_array()
        .map(|years| {
            years
                .iter()
                .filter_map(|year| year.as_i64())
                .map(|year| year.to_string())
                .collect::<Vec<_>>()
                .join(",")
        })
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "-".to_owned())
}

fn source_coverage_summary(app: &App, station_id: &StationId) -> Result<Vec<serde_json::Value>> {
    let mut coverage = Vec::new();
    for source in all_sources() {
        let manifests = station_normalized_manifests_for_source(app, station_id, source)?;
        coverage.push(json!({
            "source": source.slug(),
            "role": source_role(source),
            "coverage_years": manifest_years(&manifests),
            "warnings": manifest_warnings(&manifests),
            "manifest_count": manifests.len(),
        }));
    }
    Ok(coverage)
}

fn station_normalized_manifests_for_source(
    app: &App,
    station_id: &StationId,
    source: DataSource,
) -> Result<Vec<DatasetManifest>> {
    list_files_recursive(&app.cache.manifests_dir, "json")?
        .into_iter()
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with(&format!("normalized-{}-", source.slug()))
                        && name.contains(&format!("-{station_id}-"))
                })
        })
        .map(|path| read_json(&path))
        .collect()
}

#[cfg(test)]
mod tests {
    use chrono::{FixedOffset, NaiveDateTime, TimeZone};

    use crate::domain::{ObservationRecord, StationId};
    use crate::source::DataSource;

    use super::{normalized_manifest_warnings, quality_note_for_day, quality_state_for_day};

    fn observation(source: DataSource) -> ObservationRecord {
        let offset = FixedOffset::west_opt(6 * 3600).unwrap();
        let naive = NaiveDateTime::parse_from_str("2026-05-14 00:00", "%Y-%m-%d %H:%M").unwrap();
        let dt = offset.from_local_datetime(&naive).single().unwrap();
        ObservationRecord::from_parts(
            StationId::new("KDSM"),
            source,
            "KDSM".to_owned(),
            dt,
            "raw".to_owned(),
        )
    }

    #[test]
    fn query_quality_helpers_mark_sparse_and_low_history_days() {
        assert_eq!(
            quality_state_for_day(1, &["nws-api".to_owned()]),
            "sparse-current-day"
        );
        assert_eq!(
            quality_note_for_day(1, &["nws-api".to_owned()]).as_deref(),
            Some("low-history / partial-day target currently relies only on NWS API observations")
        );
        assert_eq!(
            quality_state_for_day(12, &["ghcnh".to_owned()]),
            "mixed-cadence"
        );
        assert_eq!(
            quality_state_for_day(12, &["iem-asos-1min".to_owned()]),
            "normal"
        );
    }

    #[test]
    fn manifest_warnings_distinguish_cadence_and_field_coverage() {
        let mut ghcnh = observation(DataSource::Ghcnh);
        ghcnh
            .quality_flags
            .push(crate::domain::QualityFlag::SourceFieldMissing(
                "relative_humidity".to_owned(),
            ));
        let warnings = normalized_manifest_warnings(DataSource::Ghcnh, &[ghcnh]);
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("hourly fallback cadence"))
        );
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("source availability or parser coverage"))
        );
    }
}

struct PathLock {
    path: PathBuf,
}

impl Drop for PathLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn acquire_path_lock(target: &Path) -> Result<PathLock> {
    let lock_path = target.with_extension(format!(
        "{}.lock",
        target
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or("artifact")
    ));
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create lock parent {}", parent.display()))?;
    }
    for _ in 0..200 {
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
        {
            Ok(_) => return Ok(PathLock { path: lock_path }),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                thread::sleep(Duration::from_millis(100));
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to acquire lock {}", lock_path.display()));
            }
        }
    }
    bail!("timed out waiting for lock {}", lock_path.display())
}

fn quarantine_corrupt_artifact(path: &Path) -> Result<PathBuf> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let quarantine_path = path.with_extension(format!(
        "{}.corrupt-{nanos}",
        path.extension()
            .and_then(|value| value.to_str())
            .unwrap_or("artifact")
    ));
    fs::rename(path, &quarantine_path).with_context(|| {
        format!(
            "failed to quarantine unreadable artifact {} to {}",
            path.display(),
            quarantine_path.display()
        )
    })?;
    Ok(quarantine_path)
}
