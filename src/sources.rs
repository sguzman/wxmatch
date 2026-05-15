use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;
use csv::{ReaderBuilder, StringRecord};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, instrument};

use crate::cache::CacheLayout;
use crate::domain::{
    ObservationRecord, QualityFlag, SCHEMA_VERSION, SourceMetadata, StationId, StationRecord,
    cloud_fraction_from_code, fahrenheit_to_celsius, inches_hg_to_hpa, kmh_to_knots, meters_to_km,
    pa_to_hpa, relative_humidity_from_celsius, wind_components_knots,
};
use crate::source::DataSource;
use crate::storage::{read_json, write_json, write_text};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoricalFetchResult {
    pub path: String,
    pub byte_count: usize,
    pub reused: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentFetchResult {
    pub path: String,
    pub byte_count: usize,
    pub reused: bool,
}

#[async_trait]
pub trait WeatherSourceAdapter: Send + Sync {
    fn source(&self) -> DataSource;
    async fn fetch_station_metadata(&self, station_id: &StationId) -> Result<StationRecord>;
    async fn fetch_historical(
        &self,
        station_id: &StationId,
        start: NaiveDate,
        end: NaiveDate,
        refresh: bool,
    ) -> Result<HistoricalFetchResult>;
    async fn fetch_current(
        &self,
        station_id: &StationId,
        refresh: bool,
    ) -> Result<CurrentFetchResult>;
    fn normalize_raw_file(
        &self,
        path: &Path,
        station: &StationRecord,
    ) -> Result<Vec<ObservationRecord>>;
}

pub struct SourceRegistry {
    pub iem: IemAsosOneMinuteAdapter,
    pub nws: NwsApiAdapter,
}

impl SourceRegistry {
    pub fn new(cache: CacheLayout, http: Client) -> Self {
        Self {
            iem: IemAsosOneMinuteAdapter::new(cache.clone(), http.clone()),
            nws: NwsApiAdapter::new(cache, http),
        }
    }

    pub fn adapter(&self, source: DataSource) -> &dyn WeatherSourceAdapter {
        match source {
            DataSource::IemAsosOneMinute => &self.iem,
            DataSource::NwsApi => &self.nws,
            DataSource::NceiAsosFiveMinute | DataSource::Ghcnh => &self.nws,
        }
    }
}

pub struct IemAsosOneMinuteAdapter {
    cache: CacheLayout,
    http: Client,
}

impl IemAsosOneMinuteAdapter {
    pub fn new(cache: CacheLayout, http: Client) -> Self {
        Self { cache, http }
    }

    fn request_url(
        &self,
        station_id: &StationId,
        timezone: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> String {
        format!(
            "https://mesonet.agron.iastate.edu/cgi-bin/request/asos1min.py?station={station}&vars=tmpf&vars=dwpf&vars=drct&vars=sknt&vars=pres1&year1={year1}&month1={month1}&day1={day1}&hour1=0&minute1=0&year2={year2}&month2={month2}&day2={day2}&hour2=23&minute2=59&what=download&tz={timezone}",
            station = station_id.as_iem_id(),
            year1 = start.format("%Y"),
            month1 = start.format("%m"),
            day1 = start.format("%d"),
            year2 = end.format("%Y"),
            month2 = end.format("%m"),
            day2 = end.format("%d"),
            timezone = timezone.replace('/', "%2F"),
        )
    }
}

#[async_trait]
impl WeatherSourceAdapter for IemAsosOneMinuteAdapter {
    fn source(&self) -> DataSource {
        DataSource::IemAsosOneMinute
    }

    #[instrument(skip(self))]
    async fn fetch_station_metadata(&self, station_id: &StationId) -> Result<StationRecord> {
        let nws = NwsApiAdapter::new(self.cache.clone(), self.http.clone());
        nws.fetch_station_metadata(station_id).await
    }

    #[instrument(skip(self))]
    async fn fetch_historical(
        &self,
        station_id: &StationId,
        start: NaiveDate,
        end: NaiveDate,
        refresh: bool,
    ) -> Result<HistoricalFetchResult> {
        let station = self.fetch_station_metadata(station_id).await?;
        let path = self
            .cache
            .historical_raw_path(self.source(), station_id, start, end, "csv");
        if path.exists() && !refresh {
            let byte_count = fs::metadata(&path)?.len() as usize;
            info!(path = %path.display(), byte_count, "reusing cached historical raw file");
            return Ok(HistoricalFetchResult {
                path: path.display().to_string(),
                byte_count,
                reused: true,
            });
        }

        let url = self.request_url(station_id, &station.timezone, start, end);
        info!(station = %station_id, %url, "downloading historical observations");
        let body = self
            .http
            .get(&url)
            .send()
            .await
            .context("failed to request IEM historical data")?
            .error_for_status()
            .context("IEM historical request failed")?
            .text()
            .await
            .context("failed to read IEM historical response body")?;

        write_text(&path, &body)?;
        write_json(
            &self
                .cache
                .fetch_manifest_path(self.source(), station_id, start, Some(end)),
            &SourceMetadata {
                source: self.source(),
                schema_version: SCHEMA_VERSION.to_owned(),
                generated_at_utc: Utc::now(),
                raw_path: path.display().to_string(),
            },
        )?;

        Ok(HistoricalFetchResult {
            path: path.display().to_string(),
            byte_count: body.len(),
            reused: false,
        })
    }

    async fn fetch_current(
        &self,
        _station_id: &StationId,
        _refresh: bool,
    ) -> Result<CurrentFetchResult> {
        bail!("iem-asos-1min does not support current observation fetch in wxmatch v1")
    }

    fn normalize_raw_file(
        &self,
        path: &Path,
        station: &StationRecord,
    ) -> Result<Vec<ObservationRecord>> {
        let timezone: Tz = station
            .timezone
            .parse()
            .with_context(|| format!("unsupported timezone {}", station.timezone))?;

        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let mut reader = ReaderBuilder::new().from_reader(content.as_bytes());
        let headers = reader
            .headers()
            .context("failed to read IEM CSV headers")?
            .clone();
        let indexes = IemHeaderIndexes::from_headers(&headers)?;
        let mut observations = Vec::new();

        for row in reader.records() {
            let row = row.context("failed to parse IEM row")?;
            let observed_local = indexes.parse_local_datetime(&row, timezone)?;

            let mut observation = ObservationRecord::from_parts(
                station.station_id.clone(),
                DataSource::IemAsosOneMinute,
                indexes.station(&row).to_owned(),
                observed_local,
                path.display().to_string(),
            );
            observation.temperature_c = indexes.tmpf(&row)?.map(fahrenheit_to_celsius);
            observation.dewpoint_c = indexes.dwpf(&row)?.map(fahrenheit_to_celsius);
            observation.pressure_hpa = indexes.pres1(&row)?.map(inches_hg_to_hpa);
            observation.wind_direction_deg = indexes.drct(&row)?;
            observation.wind_speed_kt = indexes.sknt(&row)?;
            observation.relative_humidity_pct =
                match (observation.temperature_c, observation.dewpoint_c) {
                    (Some(temp_c), Some(dew_c)) => {
                        observation
                            .quality_flags
                            .push(QualityFlag::DerivedRelativeHumidity);
                        Some(relative_humidity_from_celsius(temp_c, dew_c))
                    }
                    _ => None,
                };
            if let (Some(direction), Some(speed)) =
                (observation.wind_direction_deg, observation.wind_speed_kt)
            {
                let (u, v) = wind_components_knots(direction, speed);
                observation.wind_u_kt = Some(u);
                observation.wind_v_kt = Some(v);
            } else {
                observation.quality_flags.push(QualityFlag::MissingWind);
            }
            if observation.temperature_c.is_none() {
                observation
                    .quality_flags
                    .push(QualityFlag::MissingTemperature);
            }
            if observation.dewpoint_c.is_none() {
                observation.quality_flags.push(QualityFlag::MissingDewpoint);
            }
            if observation.pressure_hpa.is_none() {
                observation.quality_flags.push(QualityFlag::MissingPressure);
            }

            observations.push(observation);
        }

        Ok(observations)
    }
}

pub struct NwsApiAdapter {
    cache: CacheLayout,
    http: Client,
}

impl NwsApiAdapter {
    pub fn new(cache: CacheLayout, http: Client) -> Self {
        Self { cache, http }
    }

    fn station_url(station_id: &StationId) -> String {
        format!(
            "https://api.weather.gov/stations/{}",
            station_id.as_nws_id()
        )
    }

    fn latest_url(station_id: &StationId) -> String {
        format!(
            "https://api.weather.gov/stations/{}/observations/latest",
            station_id.as_nws_id()
        )
    }

    pub fn cached_station_path(&self, station_id: &StationId) -> std::path::PathBuf {
        self.cache.station_metadata_path(station_id)
    }
}

#[async_trait]
impl WeatherSourceAdapter for NwsApiAdapter {
    fn source(&self) -> DataSource {
        DataSource::NwsApi
    }

    #[instrument(skip(self))]
    async fn fetch_station_metadata(&self, station_id: &StationId) -> Result<StationRecord> {
        let path = self.cached_station_path(station_id);
        if path.exists() {
            return read_json(&path);
        }

        let url = Self::station_url(station_id);
        info!(station = %station_id, %url, "fetching station metadata");
        let payload = self
            .http
            .get(&url)
            .send()
            .await
            .context("failed to request NWS station metadata")?
            .error_for_status()
            .context("NWS station metadata request failed")?
            .json::<NwsStationResponse>()
            .await
            .context("failed to deserialize NWS station metadata")?;

        let station = StationRecord {
            station_id: StationId::new(&payload.properties.station_identifier),
            source_station_id: payload.properties.station_identifier,
            name: payload.properties.name,
            timezone: payload.properties.time_zone,
            latitude: payload.geometry.coordinates[1],
            longitude: payload.geometry.coordinates[0],
            elevation_m: payload.properties.elevation.value,
            provider: Some(payload.properties.provider),
            fetched_at_utc: Utc::now(),
        };
        write_json(&path, &station)?;
        Ok(station)
    }

    async fn fetch_historical(
        &self,
        _station_id: &StationId,
        _start: NaiveDate,
        _end: NaiveDate,
        _refresh: bool,
    ) -> Result<HistoricalFetchResult> {
        bail!("nws-api historical fetch is not implemented in wxmatch v1")
    }

    #[instrument(skip(self))]
    async fn fetch_current(
        &self,
        station_id: &StationId,
        refresh: bool,
    ) -> Result<CurrentFetchResult> {
        let today = chrono::Local::now().date_naive();
        let path = self
            .cache
            .current_raw_path(self.source(), station_id, today, "json");
        if path.exists() && !refresh {
            let byte_count = fs::metadata(&path)?.len() as usize;
            info!(path = %path.display(), byte_count, "reusing cached current raw file");
            return Ok(CurrentFetchResult {
                path: path.display().to_string(),
                byte_count,
                reused: true,
            });
        }

        let url = Self::latest_url(station_id);
        let body = self
            .http
            .get(&url)
            .send()
            .await
            .context("failed to request NWS latest observation")?
            .error_for_status()
            .context("NWS latest observation request failed")?
            .text()
            .await
            .context("failed to read NWS latest observation body")?;
        write_text(&path, &body)?;
        Ok(CurrentFetchResult {
            path: path.display().to_string(),
            byte_count: body.len(),
            reused: false,
        })
    }

    fn normalize_raw_file(
        &self,
        path: &Path,
        station: &StationRecord,
    ) -> Result<Vec<ObservationRecord>> {
        let timezone: Tz = station
            .timezone
            .parse()
            .with_context(|| format!("unsupported timezone {}", station.timezone))?;
        let payload = read_json::<NwsObservationResponse>(path)?;
        let observed_utc = DateTime::parse_from_rfc3339(&payload.properties.timestamp)
            .context("failed to parse NWS observation timestamp")?
            .with_timezone(&Utc);
        let observed_local = observed_utc.with_timezone(&timezone).fixed_offset();
        let mut observation = ObservationRecord::from_parts(
            station.station_id.clone(),
            DataSource::NwsApi,
            payload.properties.station_id.clone(),
            observed_local,
            path.display().to_string(),
        );
        observation.temperature_c = payload.properties.temperature.value;
        observation.dewpoint_c = payload.properties.dewpoint.value;
        observation.relative_humidity_pct = payload.properties.relative_humidity.value;
        observation.wind_direction_deg = payload.properties.wind_direction.value;
        observation.wind_speed_kt = payload.properties.wind_speed.value.map(kmh_to_knots);
        observation.wind_gust_kt = payload.properties.wind_gust.value.map(kmh_to_knots);
        observation.pressure_hpa = payload.properties.barometric_pressure.value.map(pa_to_hpa);
        observation.sea_level_pressure_hpa =
            payload.properties.sea_level_pressure.value.map(pa_to_hpa);
        observation.visibility_km = payload.properties.visibility.value.map(meters_to_km);
        observation.precipitation_mm = payload.properties.precipitation_last_3_hours.value;
        observation.text_description = Some(payload.properties.text_description);
        if let Some(layer) = payload.properties.cloud_layers.first() {
            observation.cloud_cover_code = Some(layer.amount.clone());
            observation.cloud_cover_fraction = cloud_fraction_from_code(&layer.amount);
        } else {
            observation
                .quality_flags
                .push(QualityFlag::MissingCloudCover);
        }
        if let (Some(direction), Some(speed)) =
            (observation.wind_direction_deg, observation.wind_speed_kt)
        {
            let (u, v) = wind_components_knots(direction, speed);
            observation.wind_u_kt = Some(u);
            observation.wind_v_kt = Some(v);
        }
        debug!(station = %station.station_id, path = %path.display(), "normalized NWS latest observation");
        Ok(vec![observation])
    }
}

#[derive(Debug, Deserialize)]
struct NwsStationResponse {
    geometry: NwsGeometry,
    properties: NwsStationProperties,
}

#[derive(Debug, Deserialize)]
struct NwsGeometry {
    coordinates: [f64; 2],
}

#[derive(Debug, Deserialize)]
struct NwsStationProperties {
    #[serde(rename = "stationIdentifier")]
    station_identifier: String,
    name: String,
    #[serde(rename = "timeZone")]
    time_zone: String,
    provider: String,
    elevation: NwsQuantitativeValue,
}

#[derive(Debug, Deserialize)]
struct NwsObservationResponse {
    properties: NwsObservationProperties,
}

#[derive(Debug, Deserialize)]
struct NwsObservationProperties {
    #[serde(rename = "stationId")]
    station_id: String,
    timestamp: String,
    #[serde(rename = "textDescription")]
    text_description: String,
    temperature: NwsQuantitativeValue,
    dewpoint: NwsQuantitativeValue,
    #[serde(rename = "relativeHumidity")]
    relative_humidity: NwsQuantitativeValue,
    #[serde(rename = "windDirection")]
    wind_direction: NwsQuantitativeValue,
    #[serde(rename = "windSpeed")]
    wind_speed: NwsQuantitativeValue,
    #[serde(rename = "windGust")]
    wind_gust: NwsQuantitativeValue,
    #[serde(rename = "barometricPressure")]
    barometric_pressure: NwsQuantitativeValue,
    #[serde(rename = "seaLevelPressure")]
    sea_level_pressure: NwsQuantitativeValue,
    visibility: NwsQuantitativeValue,
    #[serde(rename = "precipitationLast3Hours")]
    precipitation_last_3_hours: NwsQuantitativeValue,
    #[serde(rename = "cloudLayers")]
    cloud_layers: Vec<NwsCloudLayer>,
}

#[derive(Debug, Deserialize)]
struct NwsCloudLayer {
    amount: String,
}

#[derive(Debug, Deserialize)]
struct NwsQuantitativeValue {
    value: Option<f64>,
}

#[derive(Debug)]
struct IemHeaderIndexes {
    station: usize,
    valid: usize,
    tmpf: usize,
    dwpf: usize,
    drct: usize,
    sknt: usize,
    pres1: usize,
}

impl IemHeaderIndexes {
    fn from_headers(headers: &StringRecord) -> Result<Self> {
        Ok(Self {
            station: find_header(headers, "station")?,
            valid: headers
                .iter()
                .position(|value| value.starts_with("valid("))
                .context("IEM CSV missing valid(...) column")?,
            tmpf: find_header(headers, "tmpf")?,
            dwpf: find_header(headers, "dwpf")?,
            drct: find_header(headers, "drct")?,
            sknt: find_header(headers, "sknt")?,
            pres1: find_header(headers, "pres1")?,
        })
    }

    fn station<'a>(&self, row: &'a StringRecord) -> &'a str {
        row.get(self.station).unwrap_or_default()
    }

    fn parse_local_datetime(
        &self,
        row: &StringRecord,
        timezone: Tz,
    ) -> Result<chrono::DateTime<chrono::FixedOffset>> {
        let value = row.get(self.valid).unwrap_or_default();
        let naive = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M")
            .with_context(|| format!("failed to parse IEM local timestamp {value}"))?;
        timezone
            .from_local_datetime(&naive)
            .single()
            .map(|datetime| datetime.fixed_offset())
            .context("failed to resolve IEM local timestamp in station timezone")
    }

    fn tmpf(&self, row: &StringRecord) -> Result<Option<f64>> {
        parse_optional_f64_field(row.get(self.tmpf))
    }

    fn dwpf(&self, row: &StringRecord) -> Result<Option<f64>> {
        parse_optional_f64_field(row.get(self.dwpf))
    }

    fn drct(&self, row: &StringRecord) -> Result<Option<f64>> {
        parse_optional_f64_field(row.get(self.drct))
    }

    fn sknt(&self, row: &StringRecord) -> Result<Option<f64>> {
        parse_optional_f64_field(row.get(self.sknt))
    }

    fn pres1(&self, row: &StringRecord) -> Result<Option<f64>> {
        parse_optional_f64_field(row.get(self.pres1))
    }
}

fn find_header(headers: &StringRecord, expected: &str) -> Result<usize> {
    headers
        .iter()
        .position(|value| value == expected)
        .with_context(|| format!("IEM CSV missing {expected} column"))
}

fn parse_optional_f64_field(raw: Option<&str>) -> Result<Option<f64>> {
    match raw.map(str::trim) {
        None | Some("") | Some("M") | Some("VRB") => Ok(None),
        Some(value) => value
            .parse::<f64>()
            .map(Some)
            .with_context(|| format!("failed to parse numeric field {value}")),
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::parse_optional_f64_field;

    #[derive(Deserialize)]
    struct Probe {
        value: Option<String>,
    }

    #[test]
    fn parses_numeric_optional_f64() {
        let probe: Probe = serde_json::from_str(r#"{"value":"29.015"}"#).unwrap();
        assert_eq!(
            parse_optional_f64_field(probe.value.as_deref()).unwrap(),
            Some(29.015)
        );
    }

    #[test]
    fn parses_missing_tokens_as_none() {
        for raw in [
            r#"{"value":"M"}"#,
            r#"{"value":"VRB"}"#,
            r#"{"value":null}"#,
        ] {
            let probe: Probe = serde_json::from_str(raw).unwrap();
            assert_eq!(
                parse_optional_f64_field(probe.value.as_deref()).unwrap(),
                None
            );
        }
    }
}
