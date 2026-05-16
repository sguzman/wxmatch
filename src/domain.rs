use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

use anyhow::{Context, Result};
use chrono::{DateTime, FixedOffset, NaiveDate, Timelike, Utc};
use serde::{Deserialize, Serialize};

use crate::source::DataSource;

pub const SCHEMA_VERSION: &str = "v1";

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StationId(String);

impl StationId {
    pub fn new(input: &str) -> Self {
        let trimmed = input.trim().to_ascii_uppercase();
        let normalized = if trimmed.len() == 3 && trimmed.chars().all(|c| c.is_ascii_alphabetic()) {
            format!("K{trimmed}")
        } else {
            trimmed
        };
        Self(normalized)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_nws_id(&self) -> &str {
        &self.0
    }

    pub fn as_iem_id(&self) -> &str {
        if self.0.len() == 4 && self.0.starts_with('K') {
            &self.0[1..]
        } else {
            &self.0
        }
    }
}

impl Display for StationId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationRecord {
    pub station_id: StationId,
    pub source_station_id: String,
    pub name: String,
    pub timezone: String,
    pub latitude: f64,
    pub longitude: f64,
    pub elevation_m: Option<f64>,
    pub provider: Option<String>,
    pub fetched_at_utc: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceMetadata {
    pub source: DataSource,
    pub schema_version: String,
    pub generated_at_utc: DateTime<Utc>,
    pub raw_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetManifest {
    pub dataset_kind: String,
    pub source: Option<DataSource>,
    pub station_id: String,
    pub year: i32,
    pub schema_version: String,
    pub generated_at_utc: DateTime<Utc>,
    pub row_count: usize,
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
    pub artifact_path: String,
    pub input_paths: Vec<String>,
    pub warnings: Vec<String>,
    pub source_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum QualityFlag {
    MissingTemperature,
    MissingDewpoint,
    MissingWind,
    MissingPressure,
    MissingCloudCover,
    DerivedRelativeHumidity,
    SourceFieldMissing(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservationRecord {
    pub station_id: StationId,
    pub source: DataSource,
    pub source_station_id: String,
    pub observed_at_utc: DateTime<Utc>,
    pub observed_at_local: DateTime<FixedOffset>,
    pub local_date: NaiveDate,
    pub minute_of_day: u16,
    pub temperature_c: Option<f64>,
    pub dewpoint_c: Option<f64>,
    pub relative_humidity_pct: Option<f64>,
    pub wind_speed_kt: Option<f64>,
    pub wind_gust_kt: Option<f64>,
    pub wind_direction_deg: Option<f64>,
    pub wind_u_kt: Option<f64>,
    pub wind_v_kt: Option<f64>,
    pub precipitation_mm: Option<f64>,
    pub pressure_hpa: Option<f64>,
    pub sea_level_pressure_hpa: Option<f64>,
    pub visibility_km: Option<f64>,
    pub cloud_cover_code: Option<String>,
    pub cloud_cover_fraction: Option<f64>,
    pub raw_ref: String,
    pub text_description: Option<String>,
    pub quality_flags: Vec<QualityFlag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailySummary {
    pub station_id: StationId,
    pub local_date: NaiveDate,
    pub observation_count: usize,
    pub source_slugs: Vec<String>,
    pub high_temp_c: Option<f64>,
    pub low_temp_c: Option<f64>,
    pub mean_temp_c: Option<f64>,
    pub mean_dewpoint_c: Option<f64>,
    pub mean_relative_humidity_pct: Option<f64>,
    pub max_wind_speed_kt: Option<f64>,
    pub mean_wind_u_kt: Option<f64>,
    pub mean_wind_v_kt: Option<f64>,
    pub total_precipitation_mm: Option<f64>,
    pub mean_cloud_cover_fraction: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HourlyProfilePoint {
    pub hour: u8,
    pub sample_count: usize,
    pub temperature_c: Option<f64>,
    pub dewpoint_c: Option<f64>,
    pub relative_humidity_pct: Option<f64>,
    pub wind_u_kt: Option<f64>,
    pub wind_v_kt: Option<f64>,
    pub cloud_cover_fraction: Option<f64>,
    pub precipitation_mm: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DayProfile {
    pub station_id: StationId,
    pub local_date: NaiveDate,
    pub observed_hour_count: usize,
    pub source_slugs: Vec<String>,
    pub hours: Vec<HourlyProfilePoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbabilityEstimate {
    pub method: String,
    pub probability: f64,
    pub sample_size: usize,
    pub weight_used: Option<f64>,
    pub confidence_note: Option<String>,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MethodAvailability {
    pub method: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbabilityBreakdown {
    pub station_id: StationId,
    pub target_date: NaiveDate,
    pub threshold_high_c: f64,
    pub methods: Vec<ProbabilityEstimate>,
    pub unavailable_methods: Vec<MethodAvailability>,
    pub combined: Option<CombinedProbability>,
    pub combined_probability: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CombinedProbability {
    pub probability: f64,
    pub method_count: usize,
    pub combination_note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalogResult {
    pub station_id: StationId,
    pub target_date: NaiveDate,
    pub analog_date: NaiveDate,
    pub distance: f64,
    pub observed_high_c: Option<f64>,
    pub compared_hours: usize,
    pub candidate_source_summary: String,
    pub source_mix_note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservationRow {
    pub schema_version: String,
    pub station_id: String,
    pub source: String,
    pub source_station_id: String,
    pub observed_at_utc: String,
    pub observed_at_local: String,
    pub local_date: String,
    pub minute_of_day: u16,
    pub temperature_c: Option<f64>,
    pub dewpoint_c: Option<f64>,
    pub relative_humidity_pct: Option<f64>,
    pub wind_speed_kt: Option<f64>,
    pub wind_gust_kt: Option<f64>,
    pub wind_direction_deg: Option<f64>,
    pub wind_u_kt: Option<f64>,
    pub wind_v_kt: Option<f64>,
    pub precipitation_mm: Option<f64>,
    pub pressure_hpa: Option<f64>,
    pub sea_level_pressure_hpa: Option<f64>,
    pub visibility_km: Option<f64>,
    pub cloud_cover_code: Option<String>,
    pub cloud_cover_fraction: Option<f64>,
    pub raw_ref: String,
    pub text_description: Option<String>,
    pub quality_flags_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailySummaryRow {
    pub schema_version: String,
    pub station_id: String,
    pub local_date: String,
    pub observation_count: u64,
    pub source_slugs_json: String,
    pub high_temp_c: Option<f64>,
    pub low_temp_c: Option<f64>,
    pub mean_temp_c: Option<f64>,
    pub mean_dewpoint_c: Option<f64>,
    pub mean_relative_humidity_pct: Option<f64>,
    pub max_wind_speed_kt: Option<f64>,
    pub mean_wind_u_kt: Option<f64>,
    pub mean_wind_v_kt: Option<f64>,
    pub total_precipitation_mm: Option<f64>,
    pub mean_cloud_cover_fraction: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DayProfileRow {
    pub schema_version: String,
    pub station_id: String,
    pub local_date: String,
    pub observed_hour_count: u64,
    pub source_slugs_json: String,
    pub hour: u8,
    pub sample_count: u64,
    pub temperature_c: Option<f64>,
    pub dewpoint_c: Option<f64>,
    pub relative_humidity_pct: Option<f64>,
    pub wind_u_kt: Option<f64>,
    pub wind_v_kt: Option<f64>,
    pub cloud_cover_fraction: Option<f64>,
    pub precipitation_mm: Option<f64>,
}

impl ObservationRecord {
    pub fn from_parts(
        station_id: StationId,
        source: DataSource,
        source_station_id: String,
        observed_at_local: DateTime<FixedOffset>,
        raw_ref: String,
    ) -> Self {
        let observed_at_utc = observed_at_local.with_timezone(&Utc);
        let local_date = observed_at_local.date_naive();
        let minute_of_day = (observed_at_local.hour() * 60 + observed_at_local.minute()) as u16;

        Self {
            station_id,
            source,
            source_station_id,
            observed_at_utc,
            observed_at_local,
            local_date,
            minute_of_day,
            temperature_c: None,
            dewpoint_c: None,
            relative_humidity_pct: None,
            wind_speed_kt: None,
            wind_gust_kt: None,
            wind_direction_deg: None,
            wind_u_kt: None,
            wind_v_kt: None,
            precipitation_mm: None,
            pressure_hpa: None,
            sea_level_pressure_hpa: None,
            visibility_km: None,
            cloud_cover_code: None,
            cloud_cover_fraction: None,
            raw_ref,
            text_description: None,
            quality_flags: Vec::new(),
        }
    }
}

impl ObservationRow {
    pub fn from_observation(value: &ObservationRecord) -> Result<Self> {
        Ok(Self {
            schema_version: SCHEMA_VERSION.to_owned(),
            station_id: value.station_id.to_string(),
            source: value.source.slug().to_owned(),
            source_station_id: value.source_station_id.clone(),
            observed_at_utc: value.observed_at_utc.to_rfc3339(),
            observed_at_local: value.observed_at_local.to_rfc3339(),
            local_date: value.local_date.format("%Y-%m-%d").to_string(),
            minute_of_day: value.minute_of_day,
            temperature_c: value.temperature_c,
            dewpoint_c: value.dewpoint_c,
            relative_humidity_pct: value.relative_humidity_pct,
            wind_speed_kt: value.wind_speed_kt,
            wind_gust_kt: value.wind_gust_kt,
            wind_direction_deg: value.wind_direction_deg,
            wind_u_kt: value.wind_u_kt,
            wind_v_kt: value.wind_v_kt,
            precipitation_mm: value.precipitation_mm,
            pressure_hpa: value.pressure_hpa,
            sea_level_pressure_hpa: value.sea_level_pressure_hpa,
            visibility_km: value.visibility_km,
            cloud_cover_code: value.cloud_cover_code.clone(),
            cloud_cover_fraction: value.cloud_cover_fraction,
            raw_ref: value.raw_ref.clone(),
            text_description: value.text_description.clone(),
            quality_flags_json: serde_json::to_string(&value.quality_flags)
                .context("failed to serialize quality flags")?,
        })
    }

    pub fn into_observation(self) -> Result<ObservationRecord> {
        Ok(ObservationRecord {
            station_id: StationId::new(&self.station_id),
            source: DataSource::from_slug(&self.source)
                .context("failed to parse observation source")?,
            source_station_id: self.source_station_id,
            observed_at_utc: DateTime::parse_from_rfc3339(&self.observed_at_utc)
                .context("failed to parse observed_at_utc")?
                .with_timezone(&Utc),
            observed_at_local: DateTime::parse_from_rfc3339(&self.observed_at_local)
                .context("failed to parse observed_at_local")?,
            local_date: NaiveDate::parse_from_str(&self.local_date, "%Y-%m-%d")
                .context("failed to parse local_date")?,
            minute_of_day: self.minute_of_day,
            temperature_c: self.temperature_c,
            dewpoint_c: self.dewpoint_c,
            relative_humidity_pct: self.relative_humidity_pct,
            wind_speed_kt: self.wind_speed_kt,
            wind_gust_kt: self.wind_gust_kt,
            wind_direction_deg: self.wind_direction_deg,
            wind_u_kt: self.wind_u_kt,
            wind_v_kt: self.wind_v_kt,
            precipitation_mm: self.precipitation_mm,
            pressure_hpa: self.pressure_hpa,
            sea_level_pressure_hpa: self.sea_level_pressure_hpa,
            visibility_km: self.visibility_km,
            cloud_cover_code: self.cloud_cover_code,
            cloud_cover_fraction: self.cloud_cover_fraction,
            raw_ref: self.raw_ref,
            text_description: self.text_description,
            quality_flags: serde_json::from_str(&self.quality_flags_json)
                .context("failed to parse quality flags")?,
        })
    }
}

impl DailySummaryRow {
    pub fn from_summary(value: &DailySummary) -> Self {
        Self {
            schema_version: SCHEMA_VERSION.to_owned(),
            station_id: value.station_id.to_string(),
            local_date: value.local_date.format("%Y-%m-%d").to_string(),
            observation_count: value.observation_count as u64,
            source_slugs_json: serde_json::to_string(&value.source_slugs)
                .unwrap_or_else(|_| "[]".to_owned()),
            high_temp_c: value.high_temp_c,
            low_temp_c: value.low_temp_c,
            mean_temp_c: value.mean_temp_c,
            mean_dewpoint_c: value.mean_dewpoint_c,
            mean_relative_humidity_pct: value.mean_relative_humidity_pct,
            max_wind_speed_kt: value.max_wind_speed_kt,
            mean_wind_u_kt: value.mean_wind_u_kt,
            mean_wind_v_kt: value.mean_wind_v_kt,
            total_precipitation_mm: value.total_precipitation_mm,
            mean_cloud_cover_fraction: value.mean_cloud_cover_fraction,
        }
    }

    pub fn into_summary(self) -> Result<DailySummary> {
        Ok(DailySummary {
            station_id: StationId::new(&self.station_id),
            local_date: NaiveDate::parse_from_str(&self.local_date, "%Y-%m-%d")
                .context("failed to parse summary local_date")?,
            observation_count: self.observation_count as usize,
            source_slugs: serde_json::from_str(&self.source_slugs_json)
                .context("failed to parse summary source slugs")?,
            high_temp_c: self.high_temp_c,
            low_temp_c: self.low_temp_c,
            mean_temp_c: self.mean_temp_c,
            mean_dewpoint_c: self.mean_dewpoint_c,
            mean_relative_humidity_pct: self.mean_relative_humidity_pct,
            max_wind_speed_kt: self.max_wind_speed_kt,
            mean_wind_u_kt: self.mean_wind_u_kt,
            mean_wind_v_kt: self.mean_wind_v_kt,
            total_precipitation_mm: self.total_precipitation_mm,
            mean_cloud_cover_fraction: self.mean_cloud_cover_fraction,
        })
    }
}

impl DayProfileRow {
    pub fn from_profile(value: &DayProfile) -> Vec<Self> {
        value
            .hours
            .iter()
            .map(|hour| Self {
                schema_version: SCHEMA_VERSION.to_owned(),
                station_id: value.station_id.to_string(),
                local_date: value.local_date.format("%Y-%m-%d").to_string(),
                observed_hour_count: value.observed_hour_count as u64,
                source_slugs_json: serde_json::to_string(&value.source_slugs)
                    .unwrap_or_else(|_| "[]".to_owned()),
                hour: hour.hour,
                sample_count: hour.sample_count as u64,
                temperature_c: hour.temperature_c,
                dewpoint_c: hour.dewpoint_c,
                relative_humidity_pct: hour.relative_humidity_pct,
                wind_u_kt: hour.wind_u_kt,
                wind_v_kt: hour.wind_v_kt,
                cloud_cover_fraction: hour.cloud_cover_fraction,
                precipitation_mm: hour.precipitation_mm,
            })
            .collect()
    }

    pub fn into_profiles(rows: Vec<Self>) -> Result<Vec<DayProfile>> {
        let mut grouped: BTreeMap<(String, String), Vec<Self>> = BTreeMap::new();
        for row in rows {
            grouped
                .entry((row.station_id.clone(), row.local_date.clone()))
                .or_default()
                .push(row);
        }

        let mut profiles = Vec::new();
        for ((station_id, local_date), mut rows) in grouped {
            rows.sort_by_key(|row| row.hour);
            let observed_hour_count = rows.first().map(|row| row.observed_hour_count).unwrap_or(0);
            let source_slugs = rows
                .first()
                .map(|row| serde_json::from_str(&row.source_slugs_json))
                .transpose()
                .context("failed to parse profile source slugs")?
                .unwrap_or_default();
            let hours = rows
                .into_iter()
                .map(|row| HourlyProfilePoint {
                    hour: row.hour,
                    sample_count: row.sample_count as usize,
                    temperature_c: row.temperature_c,
                    dewpoint_c: row.dewpoint_c,
                    relative_humidity_pct: row.relative_humidity_pct,
                    wind_u_kt: row.wind_u_kt,
                    wind_v_kt: row.wind_v_kt,
                    cloud_cover_fraction: row.cloud_cover_fraction,
                    precipitation_mm: row.precipitation_mm,
                })
                .collect();
            profiles.push(DayProfile {
                station_id: StationId::new(&station_id),
                local_date: NaiveDate::parse_from_str(&local_date, "%Y-%m-%d")
                    .context("failed to parse profile local_date")?,
                observed_hour_count: observed_hour_count as usize,
                source_slugs,
                hours,
            });
        }

        Ok(profiles)
    }
}

pub fn fahrenheit_to_celsius(value: f64) -> f64 {
    (value - 32.0) * 5.0 / 9.0
}

pub fn celsius_to_fahrenheit(value: f64) -> f64 {
    value * 9.0 / 5.0 + 32.0
}

pub fn knots_to_kmh(value: f64) -> f64 {
    value * 1.852
}

pub fn kmh_to_knots(value: f64) -> f64 {
    value / 1.852
}

pub fn inches_hg_to_hpa(value: f64) -> f64 {
    value * 33.8638866667
}

pub fn pa_to_hpa(value: f64) -> f64 {
    value / 100.0
}

pub fn meters_to_km(value: f64) -> f64 {
    value / 1000.0
}

pub fn relative_humidity_from_celsius(temp_c: f64, dewpoint_c: f64) -> f64 {
    let a = 17.625;
    let b = 243.04;
    let saturation = ((a * temp_c) / (b + temp_c)).exp();
    let actual = ((a * dewpoint_c) / (b + dewpoint_c)).exp();
    (100.0 * actual / saturation).clamp(0.0, 100.0)
}

pub fn wind_components_knots(direction_deg: f64, speed_kt: f64) -> (f64, f64) {
    let theta = direction_deg.to_radians();
    let u = -speed_kt * theta.sin();
    let v = -speed_kt * theta.cos();
    (u, v)
}

pub fn cloud_fraction_from_code(code: &str) -> Option<f64> {
    match code.trim().to_ascii_uppercase().as_str() {
        "CLR" | "SKC" => Some(0.0),
        "FEW" => Some(0.125),
        "SCT" => Some(0.375),
        "BKN" => Some(0.75),
        "OVC" | "VV" => Some(1.0),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        StationId, cloud_fraction_from_code, fahrenheit_to_celsius, relative_humidity_from_celsius,
        wind_components_knots,
    };

    #[test]
    fn normalizes_station_ids() {
        assert_eq!(StationId::new("dsm").as_nws_id(), "KDSM");
        assert_eq!(StationId::new("kdsm").as_iem_id(), "DSM");
    }

    #[test]
    fn computes_relative_humidity() {
        let rh = relative_humidity_from_celsius(
            fahrenheit_to_celsius(74.0),
            fahrenheit_to_celsius(32.0),
        );
        assert!(rh > 20.0 && rh < 30.0);
    }

    #[test]
    fn computes_wind_vector() {
        let (u, v) = wind_components_knots(0.0, 10.0);
        assert!(u.abs() < 1e-9);
        assert!((v + 10.0).abs() < 1e-9);
    }

    #[test]
    fn maps_cloud_cover() {
        assert_eq!(cloud_fraction_from_code("BKN"), Some(0.75));
    }
}
