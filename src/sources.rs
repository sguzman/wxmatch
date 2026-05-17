use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;
use csv::{ReaderBuilder, StringRecord};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
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
    pub ncei: NceiAsosFiveMinuteAdapter,
    pub ghcnh: GhcnhAdapter,
}

impl SourceRegistry {
    pub fn new(cache: CacheLayout, http: Client) -> Self {
        Self {
            iem: IemAsosOneMinuteAdapter::new(cache.clone(), http.clone()),
            nws: NwsApiAdapter::new(cache.clone(), http.clone()),
            ncei: NceiAsosFiveMinuteAdapter::new(cache.clone(), http.clone()),
            ghcnh: GhcnhAdapter::new(cache, http),
        }
    }

    pub fn adapter(&self, source: DataSource) -> &dyn WeatherSourceAdapter {
        match source {
            DataSource::IemAsosOneMinute => &self.iem,
            DataSource::NwsApi => &self.nws,
            DataSource::NceiAsosFiveMinute => &self.ncei,
            DataSource::Ghcnh => &self.ghcnh,
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
        include_optional_fields: bool,
    ) -> String {
        let optional = if include_optional_fields {
            "&vars=p01i&vars=vsby&vars=skyc1"
        } else {
            ""
        };
        format!(
            "https://mesonet.agron.iastate.edu/cgi-bin/request/asos1min.py?station={station}&vars=tmpf&vars=dwpf&vars=drct&vars=sknt&vars=pres1{optional}&year1={year1}&month1={month1}&day1={day1}&hour1=0&minute1=0&year2={year2}&month2={month2}&day2={day2}&hour2=23&minute2=59&what=download&tz={timezone}",
            station = station_id.as_iem_id(),
            optional = optional,
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

        let rich_url = self.request_url(station_id, &station.timezone, start, end, true);
        info!(station = %station_id, url = %rich_url, "downloading historical observations");
        let initial = self
            .http
            .get(&rich_url)
            .send()
            .await
            .context("failed to request IEM historical data")?;
        let response = if initial.status() == reqwest::StatusCode::UNPROCESSABLE_ENTITY {
            let fallback_url = self.request_url(station_id, &station.timezone, start, end, false);
            info!(
                station = %station_id,
                url = %fallback_url,
                "IEM optional field request was rejected; retrying with core field set"
            );
            self.http
                .get(&fallback_url)
                .send()
                .await
                .context("failed to request fallback IEM historical data")?
        } else {
            initial
        };
        let body = response
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
            observation.precipitation_mm = indexes.p01i(&row)?.map(|value| value * 25.4);
            observation.visibility_km = indexes.vsby(&row)?.map(|value| value * 1.60934);
            observation.cloud_cover_code = indexes.skyc1(&row)?;
            observation.cloud_cover_fraction = observation
                .cloud_cover_code
                .as_deref()
                .and_then(cloud_fraction_from_code);
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
            if indexes.skyc1.is_none() {
                observation
                    .quality_flags
                    .push(QualityFlag::SourceFieldMissing("skyc1".to_owned()));
            }
            if indexes.p01i.is_none() {
                observation
                    .quality_flags
                    .push(QualityFlag::SourceFieldMissing("p01i".to_owned()));
            }
            if indexes.vsby.is_none() {
                observation
                    .quality_flags
                    .push(QualityFlag::SourceFieldMissing("vsby".to_owned()));
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

    fn observations_url(station_id: &StationId, limit: usize) -> String {
        format!(
            "https://api.weather.gov/stations/{}/observations?limit={limit}",
            station_id.as_nws_id()
        )
    }

    fn points_url(latitude: f64, longitude: f64) -> String {
        format!("https://api.weather.gov/points/{latitude:.4},{longitude:.4}")
    }

    pub fn cached_station_path(&self, station_id: &StationId) -> std::path::PathBuf {
        self.cache.station_metadata_path(station_id)
    }

    pub async fn fetch_recent_observations(
        &self,
        station_id: &StationId,
        refresh: bool,
    ) -> Result<CurrentFetchResult> {
        let station = self.fetch_station_metadata(station_id).await?;
        let today = Utc::now().date_naive();
        let path = self.cache.current_raw_path(
            self.source(),
            station_id,
            today,
            "obs-recent",
            "json",
        );
        if path.exists() && !refresh {
            let byte_count = fs::metadata(&path)?.len() as usize;
            return Ok(CurrentFetchResult {
                path: path.display().to_string(),
                byte_count,
                reused: true,
            });
        }
        let url = Self::observations_url(station_id, 36);
        let body = self
            .http
            .get(&url)
            .send()
            .await
            .context("failed to request NWS recent observations")?
            .error_for_status()
            .context("NWS recent observations request failed")?
            .text()
            .await
            .context("failed to read NWS recent observations body")?;
        write_text(&path, &body)?;
        let _ = station;
        Ok(CurrentFetchResult {
            path: path.display().to_string(),
            byte_count: body.len(),
            reused: false,
        })
    }

    pub async fn fetch_hourly_forecast(
        &self,
        station: &StationRecord,
        refresh: bool,
    ) -> Result<CurrentFetchResult> {
        let today = Utc::now().date_naive();
        let path = self.cache.current_raw_path(
            self.source(),
            &station.station_id,
            today,
            "forecast-hourly",
            "json",
        );
        if path.exists() && !refresh {
            let byte_count = fs::metadata(&path)?.len() as usize;
            return Ok(CurrentFetchResult {
                path: path.display().to_string(),
                byte_count,
                reused: true,
            });
        }
        let points_url = Self::points_url(station.latitude, station.longitude);
        let points = self
            .http
            .get(&points_url)
            .send()
            .await
            .context("failed to request NWS points lookup")?
            .error_for_status()
            .context("NWS points lookup failed")?
            .json::<NwsPointsResponse>()
            .await
            .context("failed to deserialize NWS points lookup")?;
        let forecast_url = points.properties.forecast_hourly;
        let body = self
            .http
            .get(&forecast_url)
            .send()
            .await
            .context("failed to request NWS hourly forecast")?
            .error_for_status()
            .context("NWS hourly forecast request failed")?
            .text()
            .await
            .context("failed to read NWS hourly forecast body")?;
        write_text(&path, &body)?;
        Ok(CurrentFetchResult {
            path: path.display().to_string(),
            byte_count: body.len(),
            reused: false,
        })
    }

    pub fn parse_hourly_forecast(&self, path: &Path) -> Result<Vec<NwsHourlyForecastPeriod>> {
        let payload = read_json::<NwsHourlyForecastResponse>(path)?;
        Ok(payload.properties.periods)
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
        let payload: Value =
            serde_json::from_str(&body).context("failed to parse NWS latest observation JSON")?;
        let observed_at = payload["properties"]["timestamp"]
            .as_str()
            .context("NWS latest observation payload missing properties.timestamp")?;
        let observed_at = DateTime::parse_from_rfc3339(observed_at)
            .context("failed to parse NWS latest observation timestamp")?
            .with_timezone(&Utc);
        let today = observed_at.date_naive();
        let path = self.cache.current_raw_path(
            self.source(),
            station_id,
            today,
            &format!("obs-{}", observed_at.format("%Y%m%dT%H%M%SZ")),
            "json",
        );
        if path.exists() && !refresh {
            let byte_count = fs::metadata(&path)?.len() as usize;
            info!(path = %path.display(), byte_count, "reusing cached current raw file");
            return Ok(CurrentFetchResult {
                path: path.display().to_string(),
                byte_count,
                reused: true,
            });
        }
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
        let payload = read_json::<Value>(path)?;
        let observations = if let Some(features) = payload.get("features").and_then(|value| value.as_array()) {
            let mut observations = Vec::new();
            for feature in features {
                let properties: NwsObservationProperties = serde_json::from_value(
                    feature
                        .get("properties")
                        .cloned()
                        .context("NWS observation feature missing properties")?,
                )
                .context("failed to parse NWS observation properties")?;
                observations.push(
                    nws_observation_from_properties(properties, station, timezone, path)?,
                );
            }
            observations
        } else {
            let payload = serde_json::from_value::<NwsObservationResponse>(payload)
                .context("failed to parse NWS latest observation JSON")?;
            vec![nws_observation_from_properties(
                payload.properties,
                station,
                timezone,
                path,
            )?]
        };
        debug!(station = %station.station_id, path = %path.display(), count = observations.len(), "normalized NWS observation payload");
        Ok(observations)
    }
}

pub struct NceiAsosFiveMinuteAdapter {
    cache: CacheLayout,
    http: Client,
}

impl NceiAsosFiveMinuteAdapter {
    pub fn new(cache: CacheLayout, http: Client) -> Self {
        Self { cache, http }
    }

    fn request_url(station_id: &StationId, year: i32, month: u32) -> String {
        format!(
            "https://www.ncei.noaa.gov/data/automated-surface-observing-system-five-minute/access/{year}/{month:02}/asos-5min-{station}-{year}{month:02}.dat",
            station = station_id.as_nws_id(),
        )
    }
}

#[async_trait]
impl WeatherSourceAdapter for NceiAsosFiveMinuteAdapter {
    fn source(&self) -> DataSource {
        DataSource::NceiAsosFiveMinute
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
        let path = self
            .cache
            .historical_raw_path(self.source(), station_id, start, end, "dat");
        if path.exists() && !refresh {
            let byte_count = fs::metadata(&path)?.len() as usize;
            return Ok(HistoricalFetchResult {
                path: path.display().to_string(),
                byte_count,
                reused: true,
            });
        }

        let mut bodies = Vec::new();
        for (year, month) in month_span(start, end) {
            let url = Self::request_url(station_id, year, month);
            info!(station = %station_id, %url, "downloading NCEI ASOS 5-minute source month");
            let body = self
                .http
                .get(&url)
                .send()
                .await
                .with_context(|| format!("failed to request NCEI 5-minute data {url}"))?
                .error_for_status()
                .with_context(|| format!("NCEI 5-minute request failed {url}"))?
                .text()
                .await
                .context("failed to read NCEI 5-minute response body")?;
            bodies.push(body);
        }
        let body = bodies.join("\n");
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
        bail!("ncei-asos-5min does not provide current observation fetch in wxmatch");
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
        let mut observations = Vec::new();
        for line in content.lines().filter(|line| !line.trim().is_empty()) {
            if let Some(observation) = parse_ncei_line(line, station, timezone, path)? {
                observations.push(observation);
            }
        }
        Ok(observations)
    }
}

pub struct GhcnhAdapter {
    cache: CacheLayout,
    http: Client,
}

impl GhcnhAdapter {
    pub fn new(cache: CacheLayout, http: Client) -> Self {
        Self { cache, http }
    }

    fn station_list_path(&self) -> std::path::PathBuf {
        self.cache
            .source_root(DataSource::Ghcnh)
            .join("station-list.csv")
    }

    async fn ensure_station_list(&self) -> Result<Vec<GhcnhStationListRow>> {
        let path = self.station_list_path();
        if !path.exists() {
            let url = "https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/doc/ghcnh-station-list.csv";
            let body = self
                .http
                .get(url)
                .send()
                .await
                .context("failed to request GHCNh station list")?
                .error_for_status()
                .context("GHCNh station list request failed")?
                .text()
                .await
                .context("failed to read GHCNh station list body")?;
            write_text(&path, &body)?;
        }
        let mut reader = ReaderBuilder::new()
            .from_path(&path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        let mut rows = Vec::new();
        for row in reader.deserialize() {
            rows.push(row.context("failed to parse GHCNh station list row")?);
        }
        Ok(rows)
    }

    async fn ghcnh_station_id(&self, station_id: &StationId) -> Result<String> {
        let rows = self.ensure_station_list().await?;
        let station = rows
            .into_iter()
            .find(|row| row.icao.as_deref() == Some(station_id.as_nws_id()))
            .with_context(|| format!("no GHCNh station mapping found for {station_id}"))?;
        Ok(station.ghcn_id)
    }

    fn year_url(ghcnh_station_id: &str, year: i32) -> String {
        format!(
            "https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/by-year/{year}/psv/GHCNh_{ghcnh_station_id}_{year}.psv"
        )
    }
}

#[async_trait]
impl WeatherSourceAdapter for GhcnhAdapter {
    fn source(&self) -> DataSource {
        DataSource::Ghcnh
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
        let path = self
            .cache
            .historical_raw_path(self.source(), station_id, start, end, "psv");
        if path.exists() && !refresh {
            let byte_count = fs::metadata(&path)?.len() as usize;
            return Ok(HistoricalFetchResult {
                path: path.display().to_string(),
                byte_count,
                reused: true,
            });
        }

        let ghcnh_station_id = self.ghcnh_station_id(station_id).await?;
        let mut bodies = Vec::new();
        for year in start.year()..=end.year() {
            let url = Self::year_url(&ghcnh_station_id, year);
            info!(station = %station_id, source_station_id = %ghcnh_station_id, %url, "downloading GHCNh source year");
            let body = self
                .http
                .get(&url)
                .send()
                .await
                .with_context(|| format!("failed to request GHCNh data {url}"))?
                .error_for_status()
                .with_context(|| format!("GHCNh request failed {url}"))?
                .text()
                .await
                .context("failed to read GHCNh response body")?;
            bodies.push(body);
        }
        let body = bodies.join("\n");
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
        bail!("ghcnh does not provide current observation fetch in wxmatch");
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
        let mut reader = ReaderBuilder::new()
            .delimiter(b'|')
            .flexible(true)
            .from_path(path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        let headers = reader
            .headers()
            .context("failed to read GHCNh headers")?
            .clone();
        let indexes = GhcnhHeaderIndexes::from_headers(&headers)?;
        let mut observations = Vec::new();
        for row in reader.records() {
            let row = row.context("failed to parse GHCNh row")?;
            if let Some(observation) = indexes.to_observation(&row, station, timezone, path)? {
                observations.push(observation);
            }
        }
        Ok(observations)
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
struct NwsObservationCollectionResponse {
    features: Vec<NwsObservationFeature>,
}

#[derive(Debug, Deserialize)]
struct NwsObservationFeature {
    properties: NwsObservationProperties,
}

#[derive(Debug, Deserialize)]
struct NwsPointsResponse {
    properties: NwsPointsProperties,
}

#[derive(Debug, Deserialize)]
struct NwsPointsProperties {
    #[serde(rename = "forecastHourly")]
    forecast_hourly: String,
}

#[derive(Debug, Deserialize)]
struct NwsHourlyForecastResponse {
    properties: NwsHourlyForecastProperties,
}

#[derive(Debug, Deserialize)]
struct NwsHourlyForecastProperties {
    periods: Vec<NwsHourlyForecastPeriod>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NwsHourlyForecastPeriod {
    pub number: u32,
    #[serde(rename = "startTime")]
    pub start_time: String,
    #[serde(rename = "endTime")]
    pub end_time: String,
    pub temperature: i32,
    #[serde(rename = "temperatureUnit")]
    pub temperature_unit: String,
    #[serde(rename = "shortForecast")]
    pub short_forecast: String,
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

fn nws_observation_from_properties(
    properties: NwsObservationProperties,
    station: &StationRecord,
    timezone: Tz,
    path: &Path,
) -> Result<ObservationRecord> {
    let observed_utc = DateTime::parse_from_rfc3339(&properties.timestamp)
        .context("failed to parse NWS observation timestamp")?
        .with_timezone(&Utc);
    let observed_local = observed_utc.with_timezone(&timezone).fixed_offset();
    let mut observation = ObservationRecord::from_parts(
        station.station_id.clone(),
        DataSource::NwsApi,
        properties.station_id.clone(),
        observed_local,
        path.display().to_string(),
    );
    observation.temperature_c = properties.temperature.value;
    observation.dewpoint_c = properties.dewpoint.value;
    observation.relative_humidity_pct = properties.relative_humidity.value;
    observation.wind_direction_deg = properties.wind_direction.value;
    observation.wind_speed_kt = properties.wind_speed.value.map(kmh_to_knots);
    observation.wind_gust_kt = properties.wind_gust.value.map(kmh_to_knots);
    observation.pressure_hpa = properties.barometric_pressure.value.map(pa_to_hpa);
    observation.sea_level_pressure_hpa = properties.sea_level_pressure.value.map(pa_to_hpa);
    observation.visibility_km = properties.visibility.value.map(meters_to_km);
    observation.precipitation_mm = properties.precipitation_last_3_hours.value;
    observation.text_description = Some(properties.text_description);
    if let Some(layer) = properties.cloud_layers.first() {
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
    Ok(observation)
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
    p01i: Option<usize>,
    vsby: Option<usize>,
    skyc1: Option<usize>,
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
            p01i: find_optional_header(headers, "p01i"),
            vsby: find_optional_header(headers, "vsby"),
            skyc1: find_optional_header(headers, "skyc1"),
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

    fn p01i(&self, row: &StringRecord) -> Result<Option<f64>> {
        parse_optional_f64_field(self.p01i.and_then(|index| row.get(index)))
    }

    fn vsby(&self, row: &StringRecord) -> Result<Option<f64>> {
        parse_optional_f64_field(self.vsby.and_then(|index| row.get(index)))
    }

    fn skyc1(&self, row: &StringRecord) -> Result<Option<String>> {
        Ok(self
            .skyc1
            .and_then(|index| row.get(index))
            .map(str::trim)
            .filter(|value| !value.is_empty() && *value != "M")
            .map(ToOwned::to_owned))
    }
}

fn find_header(headers: &StringRecord, expected: &str) -> Result<usize> {
    headers
        .iter()
        .position(|value| value == expected)
        .with_context(|| format!("IEM CSV missing {expected} column"))
}

fn find_optional_header(headers: &StringRecord, expected: &str) -> Option<usize> {
    headers.iter().position(|value| value == expected)
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

#[derive(Debug, Deserialize)]
struct GhcnhStationListRow {
    #[serde(rename = "GHCN_ID")]
    ghcn_id: String,
    #[serde(rename = "ICAO")]
    icao: Option<String>,
}

#[derive(Debug)]
struct GhcnhHeaderIndexes {
    station: usize,
    date: usize,
    temperature: usize,
    dewpoint: usize,
    relative_humidity: usize,
    station_level_pressure: usize,
    sea_level_pressure: usize,
    wind_direction: usize,
    wind_speed: usize,
    wind_gust: usize,
    precipitation: usize,
    visibility: usize,
    altimeter: usize,
    sky_cover_layer_1: usize,
}

impl GhcnhHeaderIndexes {
    fn from_headers(headers: &StringRecord) -> Result<Self> {
        Ok(Self {
            station: find_header(headers, "STATION")?,
            date: find_header(headers, "DATE")?,
            temperature: find_header(headers, "temperature")?,
            dewpoint: find_header(headers, "dew_point_temperature")?,
            relative_humidity: find_header(headers, "relative_humidity")?,
            station_level_pressure: find_header(headers, "station_level_pressure")?,
            sea_level_pressure: find_header(headers, "sea_level_pressure")?,
            wind_direction: find_header(headers, "wind_direction")?,
            wind_speed: find_header(headers, "wind_speed")?,
            wind_gust: find_header(headers, "wind_gust")?,
            precipitation: find_header(headers, "precipitation")?,
            visibility: find_header(headers, "visibility")?,
            altimeter: find_header(headers, "altimeter")?,
            sky_cover_layer_1: find_header(headers, "sky_cover_layer_1")?,
        })
    }

    fn to_observation(
        &self,
        row: &StringRecord,
        station: &StationRecord,
        timezone: Tz,
        path: &Path,
    ) -> Result<Option<ObservationRecord>> {
        let observed_utc = row.get(self.date).unwrap_or_default();
        if observed_utc.is_empty() || observed_utc == "DATE" {
            return Ok(None);
        }
        let observed_utc = if let Ok(value) = DateTime::parse_from_rfc3339(observed_utc) {
            value.with_timezone(&Utc)
        } else {
            let naive = chrono::NaiveDateTime::parse_from_str(observed_utc, "%Y-%m-%dT%H:%M:%S")
                .with_context(|| format!("failed to parse GHCNh timestamp {observed_utc}"))?;
            Utc.from_utc_datetime(&naive)
        };
        let observed_local = observed_utc.with_timezone(&timezone).fixed_offset();
        let mut observation = ObservationRecord::from_parts(
            station.station_id.clone(),
            DataSource::Ghcnh,
            row.get(self.station).unwrap_or_default().to_owned(),
            observed_local,
            path.display().to_string(),
        );
        observation.temperature_c = parse_optional_f64_field(row.get(self.temperature))?;
        observation.dewpoint_c = parse_optional_f64_field(row.get(self.dewpoint))?;
        observation.relative_humidity_pct =
            parse_optional_f64_field(row.get(self.relative_humidity))?;
        observation.wind_direction_deg = parse_optional_f64_field(row.get(self.wind_direction))?;
        observation.wind_speed_kt = parse_optional_f64_field(row.get(self.wind_speed))?;
        observation.wind_gust_kt = parse_optional_f64_field(row.get(self.wind_gust))?;
        observation.precipitation_mm = parse_optional_f64_field(row.get(self.precipitation))?;
        observation.pressure_hpa = parse_optional_f64_field(row.get(self.station_level_pressure))?;
        observation.sea_level_pressure_hpa =
            parse_optional_f64_field(row.get(self.sea_level_pressure))?;
        observation.visibility_km = parse_optional_f64_field(row.get(self.visibility))?;
        if observation.pressure_hpa.is_none() {
            observation.pressure_hpa = parse_optional_f64_field(row.get(self.altimeter))?;
            if observation.pressure_hpa.is_none() {
                observation
                    .quality_flags
                    .push(QualityFlag::SourceFieldMissing("altimeter".to_owned()));
            }
        }
        observation.cloud_cover_code = row
            .get(self.sky_cover_layer_1)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        observation.cloud_cover_fraction = observation
            .cloud_cover_code
            .as_deref()
            .and_then(cloud_fraction_from_code);
        if let (Some(direction), Some(speed)) =
            (observation.wind_direction_deg, observation.wind_speed_kt)
        {
            let (u, v) = wind_components_knots(direction, speed);
            observation.wind_u_kt = Some(u);
            observation.wind_v_kt = Some(v);
        }
        if observation.relative_humidity_pct.is_none() {
            if let (Some(temp_c), Some(dew_c)) = (observation.temperature_c, observation.dewpoint_c)
            {
                observation.relative_humidity_pct =
                    Some(relative_humidity_from_celsius(temp_c, dew_c));
                observation
                    .quality_flags
                    .push(QualityFlag::DerivedRelativeHumidity);
            } else {
                observation
                    .quality_flags
                    .push(QualityFlag::SourceFieldMissing(
                        "relative_humidity".to_owned(),
                    ));
            }
        }
        Ok(Some(observation))
    }
}

fn month_span(start: NaiveDate, end: NaiveDate) -> Vec<(i32, u32)> {
    let mut year = start.year();
    let mut month = start.month();
    let mut months = Vec::new();
    loop {
        months.push((year, month));
        if year == end.year() && month == end.month() {
            break;
        }
        month += 1;
        if month == 13 {
            month = 1;
            year += 1;
        }
    }
    months
}

fn parse_ncei_line(
    line: &str,
    station: &StationRecord,
    timezone: Tz,
    path: &Path,
) -> Result<Option<ObservationRecord>> {
    let parts = line.split_whitespace().collect::<Vec<_>>();
    if parts.len() < 10 {
        return Ok(None);
    }

    let local_stamp = parts
        .get(1)
        .copied()
        .context("NCEI line missing local timestamp token")?;
    let station_prefix_len = station.station_id.as_iem_id().len();
    if local_stamp.len() < station_prefix_len + 12 {
        return Ok(None);
    }
    let local_stamp = &local_stamp[station_prefix_len..station_prefix_len + 12];
    let observed_local_naive = chrono::NaiveDateTime::parse_from_str(local_stamp, "%Y%m%d%H%M")
        .with_context(|| format!("failed to parse NCEI local timestamp {local_stamp}"))?;
    let observed_local = timezone
        .from_local_datetime(&observed_local_naive)
        .single()
        .context("failed to resolve NCEI local timestamp in station timezone")?
        .fixed_offset();

    let mut observation = ObservationRecord::from_parts(
        station.station_id.clone(),
        DataSource::NceiAsosFiveMinute,
        station.station_id.to_string(),
        observed_local,
        path.display().to_string(),
    );

    for token in &parts {
        if token.ends_with("KT") {
            let (direction, speed, gust) = parse_metar_wind_token(token)?;
            observation.wind_direction_deg = direction;
            observation.wind_speed_kt = speed;
            observation.wind_gust_kt = gust;
            continue;
        }
        if token.starts_with('P') && token.len() == 5 {
            observation.precipitation_mm = parse_ncei_precip_token(token);
            continue;
        }
        if token.ends_with("SM") {
            observation.visibility_km = parse_visibility_token(token);
            continue;
        }
        if token.starts_with('A') && token.len() == 5 {
            observation.pressure_hpa = parse_altimeter_token(token);
            continue;
        }
        if token.contains('/') && !token.contains(':') && !token.ends_with('Z') {
            let (temp_c, dew_c) = parse_temp_dew_token(token)?;
            if observation.temperature_c.is_none() {
                observation.temperature_c = temp_c;
                observation.dewpoint_c = dew_c;
            }
            continue;
        }
        if observation.cloud_cover_code.is_none() {
            let cloud_code = token
                .get(0..3)
                .filter(|_| cloud_fraction_from_code(token).is_some())
                .or_else(|| {
                    token
                        .get(0..3)
                        .filter(|code| cloud_fraction_from_code(code).is_some())
                });
            if let Some(code) = cloud_code {
                observation.cloud_cover_code = Some(code.to_owned());
                observation.cloud_cover_fraction = cloud_fraction_from_code(code);
            }
        } else if let Some(code) = token.get(0..3) {
            if let Some(fraction) = cloud_fraction_from_code(code) {
                observation.cloud_cover_fraction = Some(
                    observation
                        .cloud_cover_fraction
                        .map(|current| current.max(fraction))
                        .unwrap_or(fraction),
                );
            }
        }
    }

    if observation.relative_humidity_pct.is_none() {
        if let (Some(temp_c), Some(dew_c)) = (observation.temperature_c, observation.dewpoint_c) {
            observation.relative_humidity_pct = Some(relative_humidity_from_celsius(temp_c, dew_c));
            observation
                .quality_flags
                .push(QualityFlag::DerivedRelativeHumidity);
        }
    }
    if let (Some(direction), Some(speed)) =
        (observation.wind_direction_deg, observation.wind_speed_kt)
    {
        let (u, v) = wind_components_knots(direction, speed);
        observation.wind_u_kt = Some(u);
        observation.wind_v_kt = Some(v);
    }
    if observation.precipitation_mm.is_none() {
        observation
            .quality_flags
            .push(QualityFlag::SourceFieldMissing("precip-token".to_owned()));
    }
    if observation.cloud_cover_code.is_none() {
        observation
            .quality_flags
            .push(QualityFlag::SourceFieldMissing("cloud-layer".to_owned()));
    }
    Ok(Some(observation))
}

fn parse_metar_wind_token(token: &str) -> Result<(Option<f64>, Option<f64>, Option<f64>)> {
    let trimmed = token.trim_end_matches("KT");
    if trimmed == "/////" || trimmed.len() < 5 {
        return Ok((None, None, None));
    }
    let (direction, rest) = if let Some(rest) = trimmed.strip_prefix("VRB") {
        (None, rest)
    } else {
        let direction = trimmed
            .get(0..3)
            .context("wind token missing direction")?
            .parse::<f64>()
            .ok();
        (direction, &trimmed[3..])
    };
    let (speed_str, gust_str) = if let Some((speed, gust)) = rest.split_once('G') {
        (speed, Some(gust))
    } else {
        (rest, None)
    };
    let speed = speed_str.parse::<f64>().ok();
    let gust = gust_str.and_then(|gust| gust.parse::<f64>().ok());
    Ok((direction, speed, gust))
}

fn parse_visibility_token(token: &str) -> Option<f64> {
    let trimmed = token.trim_end_matches("SM");
    if let Some((whole, frac)) = trimmed.split_once(' ') {
        return Some((parse_miles_number(whole)? + parse_miles_number(frac)?) * 1.60934);
    }
    parse_miles_number(trimmed).map(|miles| miles * 1.60934)
}

fn parse_miles_number(token: &str) -> Option<f64> {
    if let Some((num, den)) = token.split_once('/') {
        return Some(num.parse::<f64>().ok()? / den.parse::<f64>().ok()?);
    }
    token.parse::<f64>().ok()
}

fn parse_altimeter_token(token: &str) -> Option<f64> {
    let value = token.strip_prefix('A')?.parse::<f64>().ok()?;
    Some(inches_hg_to_hpa(value / 100.0))
}

fn parse_ncei_precip_token(token: &str) -> Option<f64> {
    let hundredths = token.strip_prefix('P')?.parse::<f64>().ok()?;
    Some(hundredths * 0.254)
}

fn parse_temp_dew_token(token: &str) -> Result<(Option<f64>, Option<f64>)> {
    let (temp, dew) = token
        .split_once('/')
        .with_context(|| format!("invalid temp/dew token {token}"))?;
    Ok((
        parse_signed_metar_number(temp),
        parse_signed_metar_number(dew),
    ))
}

fn parse_signed_metar_number(token: &str) -> Option<f64> {
    let token = token.trim();
    if token.is_empty() || token == "MM" || token == "M" {
        return None;
    }
    if let Some(value) = token.strip_prefix('M') {
        return value.parse::<f64>().ok().map(|value| -value);
    }
    token.parse::<f64>().ok()
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use chrono::{Timelike, Utc};
    use chrono_tz::America::Chicago;
    use serde::Deserialize;

    use crate::domain::{StationId, StationRecord};

    use super::{parse_ncei_line, parse_optional_f64_field};

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

    #[test]
    fn parses_ncei_five_minute_line() {
        let station = StationRecord {
            station_id: StationId::new("KDSM"),
            source_station_id: "KDSM".to_owned(),
            name: "Des Moines".to_owned(),
            timezone: "America/Chicago".to_owned(),
            latitude: 0.0,
            longitude: 0.0,
            elevation_m: None,
            provider: None,
            fetched_at_utc: Utc::now(),
        };
        let line = "14933KDSM DSM20260501000010105/01/26 00:00:31  5-MIN KDSM 010600Z 24003KT 10SM CLR 06/M02 A3007 820 57 0 230/03 RMK AO2 T00611017";
        let observation = parse_ncei_line(line, &station, Chicago, Path::new("/tmp/ncei.dat"))
            .unwrap()
            .unwrap();
        assert_eq!(observation.station_id.to_string(), "KDSM");
        assert_eq!(observation.observed_at_local.hour(), 0);
        assert_eq!(observation.temperature_c, Some(6.0));
        assert_eq!(observation.dewpoint_c, Some(-2.0));
        assert_eq!(observation.wind_speed_kt, Some(3.0));
        assert_eq!(observation.wind_direction_deg, Some(240.0));
        assert_eq!(observation.cloud_cover_code.as_deref(), Some("CLR"));
    }
}
