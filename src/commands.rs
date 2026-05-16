use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, bail};
use chrono::{Datelike, Local, NaiveDate, Timelike};
use serde::Serialize;
use serde_json::json;
use tracing::{debug, info, instrument};

use crate::app::App;
use crate::cache::CacheLayout;
use crate::cli::{
    AnalogsArgs, BuildCommand, BuildSubcommand, CacheCommand, CacheSubcommand, Cli, Command,
    FetchCommand, FetchCurrentArgs, FetchStationArgs, NormalizeCommand, NormalizeSubcommand,
    OutputFormat, ProbabilityArgs, QueryCommand, QuerySubcommand, SourceCommand, SourceSubcommand,
    StationCommand, StationSubcommand,
};
use crate::domain::{
    DailySummary, DailySummaryRow, DayProfile, DayProfileRow, ObservationRecord, ObservationRow,
    StationId, celsius_to_fahrenheit,
};
use crate::engine::{
    DailySummaryBuilder, DayProfileBuilder, DerivedDatasetBuilder, build_probability_breakdown,
    dedupe_observations, target_date_or_today, top_analogs,
};
use crate::source::{DataSource, SourceDescriptor, all_sources};
use crate::sources::WeatherSourceAdapter;
use crate::storage::{list_files_recursive, read_parquet, write_parquet};

pub async fn dispatch(app: &App, cli: Cli) -> Result<()> {
    let format = cli.format;
    match cli.command {
        Command::Cache(command) => handle_cache(app, command).await,
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
async fn handle_cache(app: &App, command: CacheCommand) -> Result<()> {
    match command.command {
        CacheSubcommand::Init => {
            app.cache.ensure_exists()?;
            println!("cache initialized at {}", app.cache.root.display());
        }
        CacheSubcommand::Show => print_cache_layout(&app.cache),
        CacheSubcommand::Doctor => run_cache_doctor(&app.cache)?,
    }

    Ok(())
}

#[instrument(skip(app))]
async fn handle_source(format: OutputFormat, app: &App, command: SourceCommand) -> Result<()> {
    match command.command {
        SourceSubcommand::List => {
            let descriptors = all_sources()
                .into_iter()
                .map(SourceDescriptor::from_source)
                .map(|descriptor| {
                    let normalized_root = app.cache.source_root(descriptor.source).join("normalized");
                    let raw_root = app.cache.source_root(descriptor.source).join("raw");
                    json!({
                        "source": descriptor.source.slug(),
                        "slug": descriptor.slug,
                        "cadence": descriptor.cadence,
                        "scope": descriptor.scope,
                        "summary": descriptor.summary,
                        "raw_files": count_files(&raw_root, source_raw_extension(descriptor.source)).unwrap_or(0),
                        "normalized_parquet_files": count_files(&normalized_root, "parquet").unwrap_or(0),
                    })
                })
                .collect::<Vec<_>>();
            if format == OutputFormat::Json {
                print_json(&descriptors)?;
            } else {
                for descriptor in descriptors {
                    println!(
                        "{slug:16}  cadence={cadence:12} scope={scope:18} raw={raw_files:4} normalized={normalized_files:4} {summary}",
                        slug = descriptor["slug"].as_str().unwrap_or_default(),
                        cadence = descriptor["cadence"].as_str().unwrap_or_default(),
                        scope = descriptor["scope"].as_str().unwrap_or_default(),
                        raw_files = descriptor["raw_files"].as_u64().unwrap_or_default(),
                        normalized_files = descriptor["normalized_parquet_files"]
                            .as_u64()
                            .unwrap_or_default(),
                        summary = descriptor["summary"].as_str().unwrap_or_default(),
                    );
                }
            }
        }
    }

    Ok(())
}

#[instrument(skip(app))]
async fn handle_station(format: OutputFormat, app: &App, command: StationCommand) -> Result<()> {
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
                "cache_status": {
                    "iem_raw_files": raw_iem,
                    "iem_normalized_files": normalized_iem,
                    "nws_normalized_files": normalized_nws,
                    "ncei_normalized_files": normalized_ncei,
                    "ghcnh_normalized_files": normalized_ghcnh,
                    "daily_years_built": daily_years,
                    "profile_years_built": profile_years,
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
    merge_observations_into_year(&normalized_path, normalized)?;

    println!("station: {}", station.station_id);
    println!("source: {}", DataSource::NwsApi.slug());
    println!("raw path: {}", result.path);
    println!("normalized path: {}", normalized_path.display());
    println!("observations written: 1");
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
        merge_observations_into_year(&normalized_path, observations)?;
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

    let output = json!({
        "station": station_id.to_string(),
        "date": date,
        "observations": summary.observation_count,
        "high_f": summary.high_temp_c.map(celsius_to_fahrenheit),
        "low_f": summary.low_temp_c.map(celsius_to_fahrenheit),
        "mean_f": summary.mean_temp_c.map(celsius_to_fahrenheit),
        "observed_hours": profile.observed_hour_count,
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

    if format == OutputFormat::Json {
        print_json(&breakdown)?;
    } else {
        println!("station: {}", breakdown.station_id);
        println!("date: {}", breakdown.target_date);
        println!("threshold high: {:.1}F", args.threshold_high);
        for method in &breakdown.methods {
            println!(
                "{}: {:.1}% (n={}){}",
                method.method,
                method.probability * 100.0,
                method.sample_size,
                method
                    .note
                    .as_ref()
                    .map(|note| format!(" [{note}]"))
                .unwrap_or_default()
            );
        }
        for unavailable in &breakdown.unavailable_methods {
            println!("{}: unavailable [{}]", unavailable.method, unavailable.reason);
        }
        if let Some(combined) = breakdown.combined_probability {
            println!("combined: {:.1}%", combined * 100.0);
        } else {
            println!("combined: unavailable (need at least two methods)");
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

    if format == OutputFormat::Json {
        print_json(&analogs)?;
    } else {
        println!("station: {station_id}");
        println!("date: {target_date}");
        println!("top analogs: {}", analogs.len());
        for analog in analogs {
            let high = analog
                .observed_high_c
                .map(celsius_to_fahrenheit)
                .map(|value| format!("{value:.1}F"))
                .unwrap_or_else(|| "n/a".to_owned());
            println!(
                "{}  distance={:.3} high={} compared_hours={}",
                analog.analog_date, analog.distance, high, analog.compared_hours
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
    if normalized_path.exists() && read_observations_for_date(&normalized_path, today)?.len() > 0 {
        return Ok(());
    }

    info!(station = %station_id, date = %today, "hydrating current-day observation for today query");
    let station = app.sources.nws.fetch_station_metadata(station_id).await?;
    let result = app.sources.nws.fetch_current(station_id, false).await?;
    let normalized = app
        .sources
        .nws
        .normalize_raw_file(Path::new(&result.path), &station)?;
    merge_observations_into_year(&normalized_path, normalized)?;
    build_daily(app, station_id, Some(today.year())).await?;
    build_profiles(app, station_id, Some(today.year())).await?;
    Ok(())
}

fn count_files(root: &Path, ext: &str) -> Result<usize> {
    Ok(list_files_recursive(root, ext)?.len())
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

fn merge_observations_into_year(path: &Path, observations: Vec<ObservationRecord>) -> Result<()> {
    let existing = if path.exists() {
        read_observation_records(path)?
    } else {
        Vec::new()
    };
    let merged = dedupe_observations(existing.into_iter().chain(observations).collect());
    write_observation_records(path, &merged)
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
