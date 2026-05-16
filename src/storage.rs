use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use arrow::datatypes::FieldRef;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_arrow::schema::{SchemaLike, TracingOptions};

pub fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create parent directory {}", parent.display()))?;
    }
    Ok(())
}

pub fn write_text(path: &Path, body: &str) -> Result<()> {
    ensure_parent(path)?;
    atomic_write_bytes(path, body.as_bytes())
}

pub fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    ensure_parent(path)?;
    let body = serde_json::to_vec_pretty(value).context("failed to serialize JSON")?;
    atomic_write_bytes(path, &body)
}

pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let body = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_slice(&body).with_context(|| format!("failed to parse {}", path.display()))
}

pub fn write_parquet<T>(path: &Path, rows: &[T]) -> Result<()>
where
    T: Serialize + DeserializeOwned,
{
    ensure_parent(path)?;
    let fields = Vec::<FieldRef>::from_type::<T>(TracingOptions::default())
        .context("failed to derive Arrow schema from type")?;
    let batch = serde_arrow::to_record_batch(&fields, &rows)
        .context("failed to build Arrow record batch")?;
    let temp_path = atomic_temp_path(path);
    let file = fs::File::create(&temp_path)
        .with_context(|| format!("failed to create parquet file {}", temp_path.display()))?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)
        .context("failed to create parquet writer")?;
    writer
        .write(&batch)
        .with_context(|| format!("failed to write parquet batch {}", temp_path.display()))?;
    writer
        .close()
        .with_context(|| format!("failed to finalize parquet file {}", temp_path.display()))?;
    atomic_rename(&temp_path, path)?;
    Ok(())
}

pub fn read_parquet<T>(path: &Path) -> Result<Vec<T>>
where
    T: DeserializeOwned,
{
    let file = fs::File::open(path)
        .with_context(|| format!("failed to open parquet file {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("failed to open parquet reader {}", path.display()))?;
    let reader = builder
        .build()
        .with_context(|| format!("failed to build parquet reader {}", path.display()))?;
    let mut rows = Vec::new();
    for batch in reader {
        let batch =
            batch.with_context(|| format!("failed to read record batch {}", path.display()))?;
        let mut decoded = serde_arrow::from_record_batch::<Vec<T>>(&batch)
            .with_context(|| format!("failed to decode parquet batch {}", path.display()))?;
        rows.append(&mut decoded);
    }
    Ok(rows)
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

fn atomic_write_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    let temp_path = atomic_temp_path(path);
    fs::write(&temp_path, bytes)
        .with_context(|| format!("failed to write {}", temp_path.display()))?;
    atomic_rename(&temp_path, path)
}

fn atomic_temp_path(path: &Path) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("artifact");
    path.with_file_name(format!(".{file_name}.tmp-{}-{nanos}", std::process::id()))
}

fn atomic_rename(temp_path: &Path, target_path: &Path) -> Result<()> {
    fs::rename(temp_path, target_path).with_context(|| {
        format!(
            "failed to move temporary file {} into place at {}",
            temp_path.display(),
            target_path.display()
        )
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use chrono::{FixedOffset, NaiveDateTime, TimeZone};

    use crate::domain::{
        DailySummaryRow, DayProfileRow, ObservationRecord, ObservationRow, StationId,
    };
    use crate::source::DataSource;

    use super::{read_parquet, write_parquet};

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("wxmatch-{name}-{}.parquet", std::process::id()))
    }

    #[test]
    fn round_trips_observation_rows_in_parquet() {
        let offset = FixedOffset::west_opt(6 * 3600).unwrap();
        let dt = offset
            .from_local_datetime(
                &NaiveDateTime::parse_from_str("2026-05-14 12:00", "%Y-%m-%d %H:%M").unwrap(),
            )
            .single()
            .unwrap();
        let mut observation = ObservationRecord::from_parts(
            StationId::new("KDSM"),
            DataSource::IemAsosOneMinute,
            "DSM".to_owned(),
            dt,
            "raw.csv".to_owned(),
        );
        observation.temperature_c = Some(20.5);
        let row = ObservationRow::from_observation(&observation).unwrap();
        let path = temp_path("observations");
        write_parquet(&path, &[row.clone()]).unwrap();
        let rows: Vec<ObservationRow> = read_parquet(&path).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].station_id, row.station_id);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn round_trips_daily_and_profile_rows_in_parquet() {
        let summary = DailySummaryRow {
            schema_version: "v1".to_owned(),
            station_id: "KDSM".to_owned(),
            local_date: "2026-05-14".to_owned(),
            observation_count: 24,
            source_slugs_json: "[\"iem-asos-1min\"]".to_owned(),
            high_temp_c: Some(30.0),
            low_temp_c: Some(20.0),
            mean_temp_c: Some(25.0),
            mean_dewpoint_c: Some(15.0),
            mean_relative_humidity_pct: Some(45.0),
            max_wind_speed_kt: Some(12.0),
            mean_wind_u_kt: Some(-1.0),
            mean_wind_v_kt: Some(2.0),
            total_precipitation_mm: Some(1.0),
            mean_cloud_cover_fraction: Some(0.25),
        };
        let profile = DayProfileRow {
            schema_version: "v1".to_owned(),
            station_id: "KDSM".to_owned(),
            local_date: "2026-05-14".to_owned(),
            observed_hour_count: 24,
            source_slugs_json: "[\"iem-asos-1min\"]".to_owned(),
            hour: 0,
            sample_count: 1,
            temperature_c: Some(21.0),
            dewpoint_c: Some(11.0),
            relative_humidity_pct: Some(50.0),
            wind_u_kt: Some(-2.0),
            wind_v_kt: Some(3.0),
            cloud_cover_fraction: Some(0.0),
            precipitation_mm: Some(0.0),
        };
        let daily_path = temp_path("daily");
        let profile_path = temp_path("profiles");
        write_parquet(&daily_path, &[summary.clone()]).unwrap();
        write_parquet(&profile_path, &[profile.clone()]).unwrap();
        let daily_rows: Vec<DailySummaryRow> = read_parquet(&daily_path).unwrap();
        let profile_rows: Vec<DayProfileRow> = read_parquet(&profile_path).unwrap();
        assert_eq!(daily_rows[0].station_id, summary.station_id);
        assert_eq!(profile_rows[0].hour, profile.hour);
        let _ = std::fs::remove_file(daily_path);
        let _ = std::fs::remove_file(profile_path);
    }
}
