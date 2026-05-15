use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;

pub fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create parent directory {}", parent.display()))?;
    }
    Ok(())
}

pub fn write_text(path: &Path, body: &str) -> Result<()> {
    ensure_parent(path)?;
    fs::write(path, body).with_context(|| format!("failed to write {}", path.display()))
}

pub fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    ensure_parent(path)?;
    let body = serde_json::to_vec_pretty(value).context("failed to serialize JSON")?;
    fs::write(path, body).with_context(|| format!("failed to write {}", path.display()))
}

pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let body = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_slice(&body).with_context(|| format!("failed to parse {}", path.display()))
}

pub fn list_files_recursive(root: &Path, ext: &str) -> Result<Vec<PathBuf>> {
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut files = Vec::new();
    for entry in fs::read_dir(root).with_context(|| format!("failed to list {}", root.display()))? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            files.extend(list_files_recursive(&path, ext)?);
        } else if path.extension().and_then(|v| v.to_str()) == Some(ext) {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}
