use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use directories::BaseDirs;
use serde::Serialize;
use tracing::{debug, info, instrument};

use crate::domain::StationId;
use crate::source::{DataSource, all_sources};

pub const APP_NAME: &str = "wxmatch";

#[derive(Debug, Clone)]
pub struct CacheLayout {
    pub root: PathBuf,
    pub sources_dir: PathBuf,
    pub stations_dir: PathBuf,
    pub derived_dir: PathBuf,
    pub manifests_dir: PathBuf,
    pub logs_dir: PathBuf,
}

#[derive(Debug, Serialize)]
pub struct BootstrapManifest {
    pub app_name: &'static str,
    pub created_at_utc: DateTime<Utc>,
    pub sources: Vec<BootstrapSource>,
}

#[derive(Debug, Serialize)]
pub struct BootstrapSource {
    pub slug: String,
    pub summary: &'static str,
    pub cadence: &'static str,
}

impl CacheLayout {
    pub fn resolve(override_dir: Option<&Path>) -> Result<Self> {
        let root = match override_dir {
            Some(path) => path.to_path_buf(),
            None => {
                let base = BaseDirs::new().context("unable to resolve user cache directory")?;
                base.cache_dir().join(APP_NAME)
            }
        };

        Ok(Self {
            sources_dir: root.join("sources"),
            stations_dir: root.join("stations"),
            derived_dir: root.join("derived"),
            manifests_dir: root.join("manifests"),
            logs_dir: root.join("logs"),
            root,
        })
    }

    #[instrument(skip(self))]
    pub fn ensure_exists(&self) -> Result<()> {
        for dir in self.directories() {
            fs::create_dir_all(dir)
                .with_context(|| format!("failed to create cache directory {}", dir.display()))?;
            debug!(path = %dir.display(), "ensured cache directory exists");
        }
        Ok(())
    }

    pub fn directories(&self) -> [&Path; 6] {
        [
            &self.root,
            &self.sources_dir,
            &self.stations_dir,
            &self.derived_dir,
            &self.manifests_dir,
            &self.logs_dir,
        ]
    }

    pub fn bootstrap_manifest_path(&self) -> PathBuf {
        self.manifests_dir.join("bootstrap.json")
    }

    pub fn source_root(&self, source: DataSource) -> PathBuf {
        self.sources_dir.join(source.slug())
    }

    pub fn station_metadata_path(&self, station_id: &StationId) -> PathBuf {
        self.stations_dir.join(format!("{station_id}.json"))
    }

    pub fn historical_raw_path(
        &self,
        source: DataSource,
        station_id: &StationId,
        start: chrono::NaiveDate,
        end: chrono::NaiveDate,
        extension: &str,
    ) -> PathBuf {
        self.source_root(source).join(format!(
            "raw/station={station_id}/window={}__{}.{}",
            start.format("%Y%m%d"),
            end.format("%Y%m%d"),
            extension
        ))
    }

    pub fn current_raw_path(
        &self,
        source: DataSource,
        station_id: &StationId,
        date: chrono::NaiveDate,
        extension: &str,
    ) -> PathBuf {
        self.source_root(source).join(format!(
            "raw/station={station_id}/date={date}/latest.{extension}"
        ))
    }

    pub fn normalized_path(
        &self,
        source: DataSource,
        station_id: &StationId,
        name: &str,
    ) -> PathBuf {
        self.source_root(source)
            .join(format!("normalized/station={station_id}/{name}.json"))
    }

    pub fn daily_summary_path(&self, station_id: &StationId, year: i32) -> PathBuf {
        self.derived_dir
            .join(format!("station={station_id}/daily/year={year}.json"))
    }

    pub fn day_profile_path(&self, station_id: &StationId, year: i32) -> PathBuf {
        self.derived_dir
            .join(format!("station={station_id}/profiles/year={year}.json"))
    }

    pub fn fetch_manifest_path(
        &self,
        source: DataSource,
        station_id: &StationId,
        start: chrono::NaiveDate,
        end: Option<chrono::NaiveDate>,
    ) -> PathBuf {
        let suffix = end.map_or_else(
            || start.format("%Y%m%d").to_string(),
            |end| format!("{}__{}", start.format("%Y%m%d"), end.format("%Y%m%d")),
        );
        self.manifests_dir.join(format!(
            "fetch-{}-{}-{suffix}.json",
            source.slug(),
            station_id
        ))
    }
}

#[instrument(skip(layout))]
pub fn write_bootstrap_manifest(layout: &CacheLayout) -> Result<()> {
    let manifest = BootstrapManifest {
        app_name: APP_NAME,
        created_at_utc: Utc::now(),
        sources: all_sources()
            .into_iter()
            .map(|source| BootstrapSource {
                slug: source.slug().to_owned(),
                summary: source.summary(),
                cadence: source.cadence(),
            })
            .collect(),
    };

    let body =
        serde_json::to_vec_pretty(&manifest).context("failed to serialize bootstrap manifest")?;
    let path = layout.bootstrap_manifest_path();
    fs::write(&path, body).with_context(|| format!("failed to write {}", path.display()))?;
    info!(path = %path.display(), "wrote bootstrap manifest");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::CacheLayout;

    #[test]
    fn uses_explicit_cache_dir() {
        let layout = CacheLayout::resolve(Some(Path::new("/tmp/wxmatch-test"))).unwrap();
        assert_eq!(layout.root, Path::new("/tmp/wxmatch-test"));
        assert_eq!(layout.logs_dir, Path::new("/tmp/wxmatch-test/logs"));
    }
}
