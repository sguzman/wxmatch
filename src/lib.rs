pub mod app;
pub mod cache;
pub mod cli;
pub mod commands;
pub mod domain;
pub mod engine;
pub mod logging;
pub mod source;
pub mod sources;
pub mod storage;

use anyhow::Result;
use clap::Parser;
use tracing::info;

use crate::app::App;
use crate::cache::CacheLayout;
use crate::cli::Cli;

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    let cache = CacheLayout::resolve(cli.cache_dir.as_deref())?;
    let _logging = logging::init(&cli, &cache)?;

    info!(
        command = ?cli.command,
        cache_root = %cache.root.display(),
        "starting wxmatch"
    );

    let app = App::bootstrap(cache).await?;
    commands::dispatch(&app, cli).await
}
