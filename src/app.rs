use anyhow::{Context, Result};
use reqwest::Client;
use tracing::{info, instrument};

use crate::cache::{CacheLayout, write_bootstrap_manifest};
use crate::sources::SourceRegistry;

pub struct App {
    pub cache: CacheLayout,
    pub http: Client,
    pub sources: SourceRegistry,
}

impl App {
    #[instrument(skip(cache))]
    pub async fn bootstrap(cache: CacheLayout) -> Result<Self> {
        cache.ensure_exists()?;
        write_bootstrap_manifest(&cache)?;

        let http = Client::builder()
            .user_agent("wxmatch/0.1.0 (contact: wxmatch@example.invalid)")
            .build()
            .context("failed to build HTTP client")?;
        let sources = SourceRegistry::new(cache.clone(), http.clone());

        info!("application bootstrap complete");
        Ok(Self {
            cache,
            http,
            sources,
        })
    }
}
