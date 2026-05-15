use anyhow::{Context, Result};
use tracing::Level;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::ChronoLocal;
use tracing_subscriber::layer::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::cache::CacheLayout;
use crate::cli::{Cli, LogFormat};

pub struct LoggingGuards {
    _file_guard: WorkerGuard,
}

pub fn init(cli: &Cli, cache: &CacheLayout) -> Result<LoggingGuards> {
    cache
        .ensure_exists()
        .context("failed to prepare cache before logging initialization")?;

    let filter = if let Some(filter) = &cli.log_filter {
        EnvFilter::try_new(filter).context("invalid log filter passed to --log-filter")?
    } else {
        EnvFilter::new(match cli.verbose {
            0 => "wxmatch=info,reqwest=warn",
            1 => "wxmatch=debug,reqwest=info",
            _ => "wxmatch=trace,reqwest=debug",
        })
    };

    let log_file = tracing_appender::rolling::daily(&cache.logs_dir, "wxmatch.log");
    let (file_writer, file_guard) = tracing_appender::non_blocking(log_file);

    let console_layer = match cli.log_format {
        LogFormat::Pretty => tracing_subscriber::fmt::layer()
            .with_ansi(!matches!(cli.color, clap::ColorChoice::Never))
            .with_timer(ChronoLocal::rfc_3339())
            .with_target(true)
            .pretty()
            .boxed(),
        LogFormat::Compact => tracing_subscriber::fmt::layer()
            .with_ansi(!matches!(cli.color, clap::ColorChoice::Never))
            .with_timer(ChronoLocal::rfc_3339())
            .with_target(true)
            .compact()
            .boxed(),
        LogFormat::Json => tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_timer(ChronoLocal::rfc_3339())
            .with_target(true)
            .json()
            .boxed(),
    };

    let file_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_timer(ChronoLocal::rfc_3339())
        .with_writer(file_writer)
        .with_target(true)
        .json()
        .boxed();

    tracing_subscriber::registry()
        .with(filter)
        .with(console_layer)
        .with(file_layer)
        .init();

    tracing::event!(
        Level::DEBUG,
        log_file = %cache.logs_dir.join("wxmatch.log").display(),
        "tracing initialized"
    );

    Ok(LoggingGuards {
        _file_guard: file_guard,
    })
}
