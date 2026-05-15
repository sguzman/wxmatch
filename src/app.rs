use anyhow::{Context, Result};
use reqwest::Client;
use tracing::{info, instrument};

use crate::cache::{CacheLayout, write_bootstrap_manifest};

pub struct App {
    pub cache: CacheLayout,
    pub http: Client,
}

impl App {
    #[instrument(skip(cache))]
    pub async fn bootstrap(cache: CacheLayout) -> Result<Self> {
        cache.ensure_exists()?;
        write_bootstrap_manifest(&cache)?;

        let http = Client::builder()
            .user_agent(concat!("wxmatch/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("failed to build HTTP client")?;

        info!("application bootstrap complete");
        Ok(Self { cache, http })
    }
}
