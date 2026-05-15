use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, bail};
use chrono::{Datelike, Local, NaiveDate, Timelike};
use tracing::{debug, info, instrument};

use crate::app::App;
use crate::cache::CacheLayout;
use crate::cli::{
    AnalogsArgs, BuildCommand, BuildSubcommand, CacheCommand, CacheSubcommand, Cli, Command,
    FetchCommand, FetchCurrentArgs, FetchStationArgs, NormalizeCommand, NormalizeSubcommand,
    ProbabilityArgs, QueryCommand, QuerySubcommand, SourceCommand, SourceSubcommand,
    StationCommand, StationSubcommand,
};
use crate::domain::{ObservationRecord, StationId, celsius_to_fahrenheit};
use crate::engine::{
    DailySummaryBuilder, DayProfileBuilder, DerivedDatasetBuilder, build_probability_breakdown,
    target_date_or_today, top_analogs,
};
use crate::source::{DataSource, SourceDescriptor, all_sources};
use crate::sources::WeatherSourceAdapter;
use crate::storage::{list_files_recursive, read_json, write_json};

pub async fn dispatch(app: &App, cli: Cli) -> Result<()> {
    match cli.command {
        Command::Cache(command) => handle_cache(app, command).await,
        Command::Source(command) => handle_source(command).await,
        Command::Station(command) => handle_station(app, command).await,
        Command::Fetch(command) => handle_fetch(app, command).await,
        Command::Normalize(command) => handle_normalize(app, command).await,
        Command::Build(command) => handle_build(app, command).await,
        Command::Query(command) => handle_query(app, command).await,
    }
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

#[instrument]
async fn handle_source(command: SourceCommand) -> Result<()> {
    match command.command {
        SourceSubcommand::List => {
            for descriptor in all_sources().into_iter().map(SourceDescriptor::from_source) {
                println!(
                    "{slug:16}  cadence={cadence:12} scope={scope:18} {summary}",
                    slug = descriptor.slug,
                    cadence = descriptor.cadence,
                    scope = descriptor.scope,
                    summary = descriptor.summary,
                );
            }
        }
    }

    Ok(())
}

#[instrument(skip(app))]
async fn handle_station(app: &App, command: StationCommand) -> Result<()> {
    match command.command {
        StationSubcommand::Inspect { station } => {
            let station_id = StationId::new(&station);
            let station_record = app.sources.nws.fetch_station_metadata(&station_id).await?;
            println!("station: {}", station_record.station_id);
            println!("source station id: {}", station_record.source_station_id);
            println!("name: {}", station_record.name);
            println!("timezone: {}", station_record.timezone);
            println!(
                "location: {}, {}",
                station_record.latitude, station_record.longitude
            );
            if let Some(elevation) = station_record.elevation_m {
                println!("elevation_m: {:.1}", elevation);
            }
            println!(
                "provider: {}",
                station_record
                    .provider
                    .unwrap_or_else(|| "unknown".to_owned())
            );
            println!(
                "station cache: {}",
                app.cache.station_metadata_path(&station_id).display()
            );
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
async fn handle_query(app: &App, command: QueryCommand) -> Result<()> {
    match command.command {
        QuerySubcommand::Day { station, date } => {
            query_day(app, &StationId::new(&station), date).await
        }
        QuerySubcommand::Today { station } => {
            query_day(app, &StationId::new(&station), Local::now().date_naive()).await
        }
        QuerySubcommand::Prob(args) => query_probability(app, args).await,
        QuerySubcommand::Analogs(args) => query_analogs(app, args).await,
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
    let normalized_path = app.cache.normalized_path(
        DataSource::NwsApi,
        &station_id,
        &format!("current-{}", Local::now().date_naive().format("%Y%m%d")),
    );
    write_json(&normalized_path, &normalized)?;

    println!("station: {}", station.station_id);
    println!("source: {}", DataSource::NwsApi.slug());
    println!("raw path: {}", result.path);
    println!("normalized path: {}", normalized_path.display());
    println!("observations: {}", normalized.len());
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
        DataSource::NceiAsosFiveMinute | DataSource::Ghcnh => {
            bail!("source {} is not implemented", source.slug())
        }
    };
    let raw_files = list_files_recursive(&raw_root, raw_ext)?;
    if raw_files.is_empty() {
        bail!("no raw files found under {}", raw_root.display());
    }

    let mut normalized_total = 0usize;
    for raw_path in raw_files {
        let observations = adapter.normalize_raw_file(&raw_path, &station)?;
        normalized_total += observations.len();
        let normalized_name = raw_path
            .file_stem()
            .and_then(|v| v.to_str())
            .unwrap_or("normalized");
        let normalized_path = app
            .cache
            .normalized_path(source, station_id, normalized_name);
        write_json(&normalized_path, &observations)?;
        info!(
            source = source.slug(),
            station = %station_id,
            raw = %raw_path.display(),
            normalized = %normalized_path.display(),
            observations = observations.len(),
            "normalized raw file"
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
    let observations = load_station_observations(app, station_id)?;
    let builder = DailySummaryBuilder;
    let summaries = builder.build(&observations)?;
    let grouped = group_daily_by_year(&summaries);
    write_grouped_summaries(app, station_id, grouped, year)?;
    Ok(())
}

#[instrument(skip(app))]
async fn build_profiles(app: &App, station_id: &StationId, year: Option<i32>) -> Result<()> {
    let observations = load_station_observations(app, station_id)?;
    let builder = DayProfileBuilder;
    let profiles = builder.build(&observations)?;
    let grouped = group_profiles_by_year(&profiles);
    write_grouped_profiles(app, station_id, grouped, year)?;
    Ok(())
}

#[instrument(skip(app))]
async fn query_day(app: &App, station_id: &StationId, date: NaiveDate) -> Result<()> {
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
    Ok(())
}

#[instrument(skip(app))]
async fn query_probability(app: &App, args: ProbabilityArgs) -> Result<()> {
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
    if let Some(combined) = breakdown.combined_probability {
        println!("combined: {:.1}%", combined * 100.0);
    } else {
        println!("combined: unavailable (need at least two methods)");
    }
    Ok(())
}

#[instrument(skip(app))]
async fn query_analogs(app: &App, args: AnalogsArgs) -> Result<()> {
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
    for source in [DataSource::IemAsosOneMinute, DataSource::NwsApi] {
        let root = app
            .cache
            .source_root(source)
            .join(format!("normalized/station={station_id}"));
        for path in list_files_recursive(&root, "json")? {
            let mut file_observations = read_json::<Vec<ObservationRecord>>(&path)?;
            debug!(path = %path.display(), count = file_observations.len(), "loaded normalized observation file");
            observations.append(&mut file_observations);
        }
    }
    observations.sort_by_key(|observation| observation.observed_at_utc);
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
    read_json(&app.cache.daily_summary_path(station_id, year))
}

fn load_profiles_for_year(
    app: &App,
    station_id: &StationId,
    year: i32,
) -> Result<Vec<crate::domain::DayProfile>> {
    read_json(&app.cache.day_profile_path(station_id, year))
}

fn load_all_daily(app: &App, station_id: &StationId) -> Result<Vec<crate::domain::DailySummary>> {
    let root = app
        .cache
        .derived_dir
        .join(format!("station={station_id}/daily"));
    let mut all = Vec::new();
    for path in list_files_recursive(&root, "json")? {
        let mut values = read_json::<Vec<crate::domain::DailySummary>>(&path)?;
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
    for path in list_files_recursive(&root, "json")? {
        let mut values = read_json::<Vec<crate::domain::DayProfile>>(&path)?;
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
        write_json(&path, &values)?;
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
        write_json(&path, &values)?;
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
    if !app.cache.daily_summary_path(station_id, year).exists() {
        build_daily(app, station_id, Some(year)).await?;
    }
    if !app.cache.day_profile_path(station_id, year).exists() {
        build_profiles(app, station_id, Some(year)).await?;
    }
    Ok(())
}

async fn ensure_today_current_data(app: &App, station_id: &StationId) -> Result<()> {
    let today = Local::now().date_naive();
    let normalized_path = app.cache.normalized_path(
        DataSource::NwsApi,
        station_id,
        &format!("current-{}", today.format("%Y%m%d")),
    );
    if normalized_path.exists() {
        return Ok(());
    }

    info!(station = %station_id, date = %today, "hydrating current-day observation for today query");
    let station = app.sources.nws.fetch_station_metadata(station_id).await?;
    let result = app.sources.nws.fetch_current(station_id, false).await?;
    let normalized = app
        .sources
        .nws
        .normalize_raw_file(Path::new(&result.path), &station)?;
    write_json(&normalized_path, &normalized)?;
    build_daily(app, station_id, Some(today.year())).await?;
    build_profiles(app, station_id, Some(today.year())).await?;
    Ok(())
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
        let current_path = app.cache.normalized_path(
            DataSource::NwsApi,
            station_id,
            &format!("current-{}", target_date.format("%Y%m%d")),
        );
        if current_path.exists() {
            let observations = read_json::<Vec<ObservationRecord>>(&current_path)?;
            let builder = DayProfileBuilder;
            let mut profiles = builder.build(&observations)?;
            return Ok(profiles.pop());
        }
    }

    Ok(None)
}
