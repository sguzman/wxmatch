use std::path::PathBuf;

use chrono::{NaiveDate, NaiveTime};
use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};

use crate::source::DataSource;

#[derive(Debug, Clone, Parser)]
#[command(
    name = "wxmatch",
    version,
    about = "Station-level weather cache, query, and forecast CLI",
    long_about = "wxmatch bootstraps a weather data cache and query engine around reproducible downloads, normalized observations, and analog-day forecasting workflows."
)]
pub struct Cli {
    #[arg(long, global = true, env = "WXMATCH_CACHE_DIR")]
    pub cache_dir: Option<PathBuf>,

    #[arg(long, global = true, default_value = "auto", env = "WXMATCH_COLOR")]
    pub color: clap::ColorChoice,

    #[arg(long, global = true, value_enum, default_value_t = LogFormat::Pretty, env = "WXMATCH_LOG_FORMAT")]
    pub log_format: LogFormat,

    #[arg(long, global = true, env = "WXMATCH_LOG")]
    pub log_filter: Option<String>,

    #[arg(short = 'v', long = "verbose", global = true, action = ArgAction::Count)]
    pub verbose: u8,

    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Text, env = "WXMATCH_FORMAT")]
    pub format: OutputFormat,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum LogFormat {
    Pretty,
    Compact,
    Json,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum OutputFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    Cache(CacheCommand),
    Source(SourceCommand),
    Station(StationCommand),
    Fetch(FetchCommand),
    Normalize(NormalizeCommand),
    Build(BuildCommand),
    Query(QueryCommand),
}

#[derive(Debug, Clone, Args)]
pub struct CacheCommand {
    #[command(subcommand)]
    pub command: CacheSubcommand,
}

#[derive(Debug, Clone, Subcommand)]
pub enum CacheSubcommand {
    Init,
    Show,
    Doctor,
    Manifests {
        #[arg(long)]
        station: Option<String>,
    },
}

#[derive(Debug, Clone, Args)]
pub struct SourceCommand {
    #[command(subcommand)]
    pub command: SourceSubcommand,
}

#[derive(Debug, Clone, Subcommand)]
pub enum SourceSubcommand {
    List,
}

#[derive(Debug, Clone, Args)]
pub struct StationCommand {
    #[command(subcommand)]
    pub command: StationSubcommand,
}

#[derive(Debug, Clone, Subcommand)]
pub enum StationSubcommand {
    Inspect { station: String },
}

#[derive(Debug, Clone, Args)]
pub struct FetchCommand {
    #[command(subcommand)]
    pub command: FetchSubcommand,
}

#[derive(Debug, Clone, Subcommand)]
pub enum FetchSubcommand {
    Station(FetchStationArgs),
    Current(FetchCurrentArgs),
}

#[derive(Debug, Clone, Args)]
pub struct FetchStationArgs {
    pub station: String,

    #[arg(long, value_enum, default_value_t = DataSource::IemAsosOneMinute)]
    pub source: DataSource,

    #[arg(long)]
    pub start: NaiveDate,

    #[arg(long)]
    pub end: NaiveDate,

    #[arg(long)]
    pub refresh: bool,
}

#[derive(Debug, Clone, Args)]
pub struct FetchCurrentArgs {
    pub station: String,

    #[arg(long, value_enum, default_value_t = DataSource::NwsApi)]
    pub source: DataSource,
}

#[derive(Debug, Clone, Args)]
pub struct NormalizeCommand {
    #[command(subcommand)]
    pub command: NormalizeSubcommand,
}

#[derive(Debug, Clone, Subcommand)]
pub enum NormalizeSubcommand {
    Station {
        station: String,
        #[arg(long, value_enum)]
        source: DataSource,
    },
}

#[derive(Debug, Clone, Args)]
pub struct BuildCommand {
    #[command(subcommand)]
    pub command: BuildSubcommand,
}

#[derive(Debug, Clone, Subcommand)]
pub enum BuildSubcommand {
    Daily {
        station: String,
        #[arg(long)]
        year: Option<i32>,
    },
    Profiles {
        station: String,
        #[arg(long)]
        year: Option<i32>,
    },
}

#[derive(Debug, Clone, Args)]
pub struct QueryCommand {
    #[command(subcommand)]
    pub command: QuerySubcommand,
}

#[derive(Debug, Clone, Subcommand)]
pub enum QuerySubcommand {
    Day {
        station: String,
        date: NaiveDate,
    },
    Today {
        station: String,
    },
    Prob(ProbabilityArgs),
    LikelyHigh(LikelyHighArgs),
    Analogs(AnalogsArgs),
    Hypothesis(HypothesisArgs),
    DuckdbPaths {
        #[arg(long)]
        station: Option<String>,
        #[arg(long)]
        year: Option<i32>,
    },
}

#[derive(Debug, Clone, Args)]
pub struct ProbabilityArgs {
    pub station: String,

    #[arg(long, conflicts_with = "today")]
    pub date: Option<NaiveDate>,

    #[arg(long, default_value_t = false)]
    pub today: bool,

    #[arg(long)]
    pub threshold_high: f32,

    #[arg(long)]
    pub as_of: Option<NaiveTime>,
}

#[derive(Debug, Clone, Args)]
pub struct LikelyHighArgs {
    pub station: String,

    #[arg(long, conflicts_with = "today")]
    pub date: Option<NaiveDate>,

    #[arg(long, default_value_t = false)]
    pub today: bool,

    #[arg(long)]
    pub as_of: Option<NaiveTime>,

    #[arg(long, default_value_t = 40)]
    pub min_high: i32,

    #[arg(long, default_value_t = 100)]
    pub max_high: i32,

    #[arg(long, default_value_t = 5)]
    pub top: usize,
}

#[derive(Debug, Clone, Args)]
pub struct AnalogsArgs {
    pub station: String,

    #[arg(long, conflicts_with = "today")]
    pub date: Option<NaiveDate>,

    #[arg(long, default_value_t = false)]
    pub today: bool,

    #[arg(long)]
    pub as_of: Option<NaiveTime>,

    #[arg(long, default_value_t = 25)]
    pub top: usize,
}

#[derive(Debug, Clone, Args)]
pub struct HypothesisArgs {
    pub station: String,

    #[arg(long, conflicts_with = "today")]
    pub date: Option<NaiveDate>,

    #[arg(long, default_value_t = false)]
    pub today: bool,

    #[arg(long)]
    pub as_of: NaiveTime,

    #[arg(long)]
    pub assume_temp: f32,

    #[arg(long)]
    pub max_high: f32,

    #[arg(long, default_value_t = 25)]
    pub top: usize,
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{CacheSubcommand, Cli, Command, QuerySubcommand};

    #[test]
    fn parses_probability_query() {
        let cli = Cli::parse_from([
            "wxmatch",
            "query",
            "prob",
            "KDEN",
            "--today",
            "--threshold-high",
            "77.5",
        ]);

        match cli.command {
            Command::Query(query) => match query.command {
                QuerySubcommand::Prob(args) => {
                    assert_eq!(args.station, "KDEN");
                    assert!(args.today);
                    assert_eq!(args.threshold_high, 77.5);
                }
                other => panic!("unexpected subcommand: {other:?}"),
            },
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parses_likely_high_query() {
        let cli = Cli::parse_from([
            "wxmatch",
            "query",
            "likely-high",
            "KLAX",
            "--today",
            "--min-high",
            "55",
            "--max-high",
            "85",
            "--top",
            "7",
        ]);

        match cli.command {
            Command::Query(query) => match query.command {
                QuerySubcommand::LikelyHigh(args) => {
                    assert_eq!(args.station, "KLAX");
                    assert!(args.today);
                    assert_eq!(args.min_high, 55);
                    assert_eq!(args.max_high, 85);
                    assert_eq!(args.top, 7);
                }
                other => panic!("unexpected subcommand: {other:?}"),
            },
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parses_cache_manifests_subcommand() {
        let cli = Cli::parse_from(["wxmatch", "cache", "manifests", "--station", "KDSM"]);
        match cli.command {
            Command::Cache(command) => match command.command {
                CacheSubcommand::Manifests { station } => {
                    assert_eq!(station.as_deref(), Some("KDSM"));
                }
                other => panic!("unexpected subcommand: {other:?}"),
            },
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parses_query_duckdb_paths_subcommand() {
        let cli = Cli::parse_from([
            "wxmatch",
            "query",
            "duckdb-paths",
            "--station",
            "KDSM",
            "--year",
            "2026",
        ]);
        match cli.command {
            Command::Query(query) => match query.command {
                QuerySubcommand::DuckdbPaths { station, year } => {
                    assert_eq!(station.as_deref(), Some("KDSM"));
                    assert_eq!(year, Some(2026));
                }
                other => panic!("unexpected subcommand: {other:?}"),
            },
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parses_query_hypothesis_subcommand() {
        let cli = Cli::parse_from([
            "wxmatch",
            "query",
            "hypothesis",
            "KLAX",
            "--date",
            "2026-05-15",
            "--as-of",
            "12:00",
            "--assume-temp",
            "66",
            "--max-high",
            "68",
        ]);
        match cli.command {
            Command::Query(query) => match query.command {
                QuerySubcommand::Hypothesis(args) => {
                    assert_eq!(args.station, "KLAX");
                    assert_eq!(args.assume_temp, 66.0);
                    assert_eq!(args.max_high, 68.0);
                }
                other => panic!("unexpected subcommand: {other:?}"),
            },
            other => panic!("unexpected command: {other:?}"),
        }
    }
}
