use std::fmt::{Display, Formatter};

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
    pub hours: Vec<HourlyProfilePoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbabilityEstimate {
    pub method: String,
    pub probability: f64,
    pub sample_size: usize,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbabilityBreakdown {
    pub station_id: StationId,
    pub target_date: NaiveDate,
    pub threshold_high_c: f64,
    pub methods: Vec<ProbabilityEstimate>,
    pub combined_probability: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalogResult {
    pub station_id: StationId,
    pub target_date: NaiveDate,
    pub analog_date: NaiveDate,
    pub distance: f64,
    pub observed_high_c: Option<f64>,
    pub compared_hours: usize,
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
