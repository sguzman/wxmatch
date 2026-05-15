use anyhow::{Result, bail};
use chrono::{Local, NaiveDate};
use tracing::{debug, info, instrument, warn};

use crate::app::App;
use crate::cache::CacheLayout;
use crate::cli::{
    AnalogsArgs, BuildCommand, BuildSubcommand, CacheCommand, CacheSubcommand, Cli, Command,
    FetchCommand, FetchCurrentArgs, FetchStationArgs, NormalizeCommand, NormalizeSubcommand,
    ProbabilityArgs, QueryCommand, QuerySubcommand, SourceCommand, SourceSubcommand,
    StationCommand, StationSubcommand,
};
use crate::source::{DataSource, SourceDescriptor, all_sources};

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
                    "{slug:16}  cadence={cadence:10} scope={scope:18} {summary}",
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
            let station = canonical_station_id(&station);
            println!("station: {station}");
            println!("stations cache: {}", app.cache.stations_dir.display());
            for source in all_sources() {
                println!(
                    "source {}: {}",
                    source.slug(),
                    app.cache
                        .source_root(source)
                        .join(format!("station={station}"))
                        .display()
                );
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
            let station = canonical_station_id(&station);
            let raw_root = app
                .cache
                .source_root(source)
                .join(format!("station={station}/raw"));
            let normalized_root = app
                .cache
                .source_root(source)
                .join(format!("station={station}/normalized"));

            info!(
                station,
                source = source.slug(),
                "planned normalization roots"
            );
            println!("station: {station}");
            println!("source: {}", source.slug());
            println!("raw root: {}", raw_root.display());
            println!("normalized root: {}", normalized_root.display());
            println!("status: normalization adapter not implemented yet");
        }
    }

    Ok(())
}

#[instrument(skip(app))]
async fn handle_build(app: &App, command: BuildCommand) -> Result<()> {
    match command.command {
        BuildSubcommand::Daily { station, year } => {
            let station = canonical_station_id(&station);
            print_derived_target(&app.cache, &station, "daily", year);
        }
        BuildSubcommand::Profiles { station, year } => {
            let station = canonical_station_id(&station);
            print_derived_target(&app.cache, &station, "profiles", year);
        }
    }

    Ok(())
}

#[instrument(skip(app))]
async fn handle_query(app: &App, command: QueryCommand) -> Result<()> {
    match command.command {
        QuerySubcommand::Day { station, date } => {
            let station = canonical_station_id(&station);
            print_query_day(app, &station, date);
        }
        QuerySubcommand::Today { station } => {
            let station = canonical_station_id(&station);
            print_query_day(app, &station, Local::now().date_naive());
        }
        QuerySubcommand::Prob(args) => print_probability_plan(app, args)?,
        QuerySubcommand::Analogs(args) => print_analog_plan(app, args)?,
    }

    Ok(())
}

#[instrument(skip(app))]
async fn fetch_station(app: &App, args: FetchStationArgs) -> Result<()> {
    if args.end < args.start {
        bail!("--end must not be earlier than --start");
    }

    let station = canonical_station_id(&args.station);
    let source_root = app.cache.source_root(args.source);
    let raw_target = source_root.join(format!(
        "raw/station={station}/year={}",
        args.start.format("%Y")
    ));

    debug!(http_client = ?app.http, "HTTP client ready for fetch planning");
    info!(
        station,
        source = args.source.slug(),
        start = %args.start,
        end = %args.end,
        refresh = args.refresh,
        "prepared historical fetch plan"
    );

    println!("station: {station}");
    println!("source: {}", args.source.slug());
    println!("window: {} -> {}", args.start, args.end);
    println!("raw cache target: {}", raw_target.display());
    println!("status: downloader adapter not implemented yet");
    Ok(())
}

#[instrument(skip(app))]
async fn fetch_current(app: &App, args: FetchCurrentArgs) -> Result<()> {
    let station = canonical_station_id(&args.station);
    let today = Local::now().date_naive();
    let raw_target = app.cache.source_root(args.source).join(format!(
        "raw/station={station}/date={today}/observation.json"
    ));

    debug!(http_client = ?app.http, "HTTP client ready for live fetch planning");
    info!(
        station,
        source = args.source.slug(),
        "prepared current-day fetch plan"
    );

    println!("station: {station}");
    println!("source: {}", args.source.slug());
    println!("date: {today}");
    println!("raw cache target: {}", raw_target.display());
    println!("status: live fetch adapter not implemented yet");
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
    if manifest.exists() {
        println!("{:>10}  {}", "ok", manifest.display());
    } else {
        warn!(path = %manifest.display(), "bootstrap manifest has not been written yet");
        println!("{:>10}  {}", "missing", manifest.display());
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

fn print_derived_target(cache: &CacheLayout, station: &str, dataset: &str, year: Option<i32>) {
    let target = match year {
        Some(year) => cache
            .derived_dir
            .join(format!("station={station}/{dataset}/year={year}.parquet")),
        None => cache
            .derived_dir
            .join(format!("station={station}/{dataset}/")),
    };

    println!("station: {station}");
    println!("dataset: {dataset}");
    println!("target: {}", target.display());
    println!("status: derived dataset builder not implemented yet");
}

fn print_query_day(app: &App, station: &str, date: NaiveDate) {
    let daily = app.cache.derived_dir.join(format!(
        "station={station}/daily/year={}.parquet",
        date.format("%Y")
    ));
    let profiles = app.cache.derived_dir.join(format!(
        "station={station}/profiles/year={}.parquet",
        date.format("%Y")
    ));

    println!("station: {station}");
    println!("date: {date}");
    println!("daily dataset: {}", daily.display());
    println!("profiles dataset: {}", profiles.display());
    println!("status: query adapter not implemented yet");
}

fn print_probability_plan(app: &App, args: ProbabilityArgs) -> Result<()> {
    let target_date = resolve_target_date(args.date, args.today)?;
    let station = canonical_station_id(&args.station);
    let as_of = args
        .as_of
        .map_or_else(|| "full-day".to_owned(), |time| time.to_string());

    println!("station: {station}");
    println!("date: {target_date}");
    println!("threshold high: {:.1}F", args.threshold_high);
    println!("as-of: {as_of}");
    println!(
        "daily dataset: {}",
        app.cache
            .derived_dir
            .join(format!(
                "station={station}/daily/year={}.parquet",
                target_date.format("%Y")
            ))
            .display()
    );
    println!(
        "profiles dataset: {}",
        app.cache
            .derived_dir
            .join(format!(
                "station={station}/profiles/year={}.parquet",
                target_date.format("%Y")
            ))
            .display()
    );
    println!("methods queued: climatology, partial-profile analogs, nearest-neighbor analogs");
    println!("status: probability engine not implemented yet");

    Ok(())
}

fn print_analog_plan(app: &App, args: AnalogsArgs) -> Result<()> {
    let target_date = resolve_target_date(args.date, args.today)?;
    let station = canonical_station_id(&args.station);
    let as_of = args
        .as_of
        .map_or_else(|| "full-day".to_owned(), |time| time.to_string());

    println!("station: {station}");
    println!("date: {target_date}");
    println!("as-of: {as_of}");
    println!("top: {}", args.top);
    println!(
        "profiles dataset: {}",
        app.cache
            .derived_dir
            .join(format!(
                "station={station}/profiles/year={}.parquet",
                target_date.format("%Y")
            ))
            .display()
    );
    println!("status: analog search engine not implemented yet");

    Ok(())
}

fn resolve_target_date(date: Option<NaiveDate>, today: bool) -> Result<NaiveDate> {
    match (date, today) {
        (Some(date), false) => Ok(date),
        (None, true) => Ok(Local::now().date_naive()),
        (None, false) => bail!("pass either --date YYYY-MM-DD or --today"),
        (Some(_), true) => bail!("--date and --today are mutually exclusive"),
    }
}

fn canonical_station_id(station: &str) -> String {
    station.trim().to_ascii_uppercase()
}

#[allow(dead_code)]
fn _source_cache_root(cache: &CacheLayout, source: DataSource, station: &str) -> String {
    cache
        .source_root(source)
        .join(format!("station={}", canonical_station_id(station)))
        .display()
        .to_string()
}
