use std::collections::{BTreeMap, HashMap, HashSet};

use crate::domain::{
    AnalogResult, DailySummary, DayProfile, HourlyProfilePoint, MethodAvailability,
    ObservationRecord, ProbabilityBreakdown, ProbabilityEstimate, StationId,
};
use anyhow::{Result, bail};
use chrono::{Datelike, Local, NaiveDate, Timelike};

pub trait Normalizer {
    fn normalize(&self) -> Result<Vec<ObservationRecord>>;
}

pub trait DerivedDatasetBuilder<T> {
    fn build(&self, observations: &[ObservationRecord]) -> Result<Vec<T>>;
}

pub trait ProbabilityMethod {
    fn name(&self) -> &'static str;
    fn estimate(
        &self,
        station_id: &StationId,
        target_date: NaiveDate,
        threshold_high_c: f64,
        daily: &[DailySummary],
        profiles: &[DayProfile],
        target_profile: Option<&DayProfile>,
        as_of_hour: Option<u8>,
    ) -> ProbabilityMethodOutcome;
}

pub enum ProbabilityMethodOutcome {
    Available(ProbabilityEstimate),
    Unavailable(MethodAvailability),
}

pub fn dedupe_observations(observations: Vec<ObservationRecord>) -> Vec<ObservationRecord> {
    let mut seen = HashSet::new();
    let mut canonical: HashMap<(String, chrono::DateTime<chrono::Utc>), ObservationRecord> =
        HashMap::new();
    for observation in observations {
        let exact_key = (
            observation.source,
            observation.station_id.to_string(),
            observation.observed_at_utc,
        );
        if !seen.insert(exact_key) {
            continue;
        }
        let merge_key = (
            observation.station_id.to_string(),
            observation.observed_at_utc,
        );
        match canonical.get(&merge_key) {
            Some(existing)
                if source_priority(existing.source) <= source_priority(observation.source) => {}
            _ => {
                canonical.insert(merge_key, observation);
            }
        }
    }
    let mut deduped = canonical.into_values().collect::<Vec<_>>();
    deduped.sort_by_key(|observation| observation.observed_at_utc);
    deduped
}

fn source_priority(source: crate::source::DataSource) -> u8 {
    match source {
        crate::source::DataSource::NceiAsosFiveMinute => 0,
        crate::source::DataSource::IemAsosOneMinute => 1,
        crate::source::DataSource::NwsApi => 2,
        crate::source::DataSource::Ghcnh => 3,
    }
}

pub struct DailySummaryBuilder;

impl DerivedDatasetBuilder<DailySummary> for DailySummaryBuilder {
    fn build(&self, observations: &[ObservationRecord]) -> Result<Vec<DailySummary>> {
        let mut grouped: BTreeMap<(String, NaiveDate), Vec<&ObservationRecord>> = BTreeMap::new();
        for observation in observations {
            grouped
                .entry((observation.station_id.to_string(), observation.local_date))
                .or_default()
                .push(observation);
        }

        let mut summaries = Vec::new();
        for ((station_id, local_date), rows) in grouped {
            let station_id = StationId::new(&station_id);
            let temps: Vec<f64> = rows.iter().filter_map(|row| row.temperature_c).collect();
            let dewpoints: Vec<f64> = rows.iter().filter_map(|row| row.dewpoint_c).collect();
            let rhs: Vec<f64> = rows
                .iter()
                .filter_map(|row| row.relative_humidity_pct)
                .collect();
            let winds: Vec<f64> = rows.iter().filter_map(|row| row.wind_speed_kt).collect();
            let wind_u: Vec<f64> = rows.iter().filter_map(|row| row.wind_u_kt).collect();
            let wind_v: Vec<f64> = rows.iter().filter_map(|row| row.wind_v_kt).collect();
            let precip: Vec<f64> = rows.iter().filter_map(|row| row.precipitation_mm).collect();
            let clouds: Vec<f64> = rows
                .iter()
                .filter_map(|row| row.cloud_cover_fraction)
                .collect();

            summaries.push(DailySummary {
                station_id,
                local_date,
                observation_count: rows.len(),
                high_temp_c: temps.iter().copied().reduce(f64::max),
                low_temp_c: temps.iter().copied().reduce(f64::min),
                mean_temp_c: mean(&temps),
                mean_dewpoint_c: mean(&dewpoints),
                mean_relative_humidity_pct: mean(&rhs),
                max_wind_speed_kt: winds.iter().copied().reduce(f64::max),
                mean_wind_u_kt: mean(&wind_u),
                mean_wind_v_kt: mean(&wind_v),
                total_precipitation_mm: if precip.is_empty() {
                    None
                } else {
                    Some(precip.iter().sum())
                },
                mean_cloud_cover_fraction: mean(&clouds),
            });
        }

        Ok(summaries)
    }
}

pub struct DayProfileBuilder;

impl DerivedDatasetBuilder<DayProfile> for DayProfileBuilder {
    fn build(&self, observations: &[ObservationRecord]) -> Result<Vec<DayProfile>> {
        let mut grouped: BTreeMap<(String, NaiveDate), Vec<&ObservationRecord>> = BTreeMap::new();
        for observation in observations {
            grouped
                .entry((observation.station_id.to_string(), observation.local_date))
                .or_default()
                .push(observation);
        }

        let mut profiles = Vec::new();
        for ((station_id, local_date), rows) in grouped {
            let mut hourly: BTreeMap<u8, Vec<&ObservationRecord>> = BTreeMap::new();
            for row in rows {
                hourly
                    .entry(row.observed_at_local.hour() as u8)
                    .or_default()
                    .push(row);
            }

            let hours = (0u8..24)
                .map(|hour| {
                    let values = hourly.get(&hour).cloned().unwrap_or_default();
                    let temps: Vec<f64> =
                        values.iter().filter_map(|row| row.temperature_c).collect();
                    let dewpoints: Vec<f64> =
                        values.iter().filter_map(|row| row.dewpoint_c).collect();
                    let rhs: Vec<f64> = values
                        .iter()
                        .filter_map(|row| row.relative_humidity_pct)
                        .collect();
                    let wind_u: Vec<f64> = values.iter().filter_map(|row| row.wind_u_kt).collect();
                    let wind_v: Vec<f64> = values.iter().filter_map(|row| row.wind_v_kt).collect();
                    let clouds: Vec<f64> = values
                        .iter()
                        .filter_map(|row| row.cloud_cover_fraction)
                        .collect();
                    let precip: Vec<f64> = values
                        .iter()
                        .filter_map(|row| row.precipitation_mm)
                        .collect();

                    HourlyProfilePoint {
                        hour,
                        sample_count: values.len(),
                        temperature_c: mean(&temps),
                        dewpoint_c: mean(&dewpoints),
                        relative_humidity_pct: mean(&rhs),
                        wind_u_kt: mean(&wind_u),
                        wind_v_kt: mean(&wind_v),
                        cloud_cover_fraction: mean(&clouds),
                        precipitation_mm: if precip.is_empty() {
                            None
                        } else {
                            Some(precip.iter().sum())
                        },
                    }
                })
                .collect::<Vec<_>>();

            let observed_hour_count = hours.iter().filter(|hour| hour.sample_count > 0).count();
            profiles.push(DayProfile {
                station_id: StationId::new(&station_id),
                local_date,
                observed_hour_count,
                hours,
            });
        }

        Ok(profiles)
    }
}

pub struct ClimatologyMethod {
    pub window_days: u16,
}

impl ProbabilityMethod for ClimatologyMethod {
    fn name(&self) -> &'static str {
        "seasonal-climatology"
    }

    fn estimate(
        &self,
        _station_id: &StationId,
        target_date: NaiveDate,
        threshold_high_c: f64,
        daily: &[DailySummary],
        _profiles: &[DayProfile],
        _target_profile: Option<&DayProfile>,
        _as_of_hour: Option<u8>,
    ) -> ProbabilityMethodOutcome {
        let mut matches = 0usize;
        let mut hits = 0usize;
        let target_ordinal = target_date.ordinal() as i32;
        for summary in daily {
            let ordinal = summary.local_date.ordinal() as i32;
            let diff = seasonal_distance(target_ordinal, ordinal);
            if diff <= i32::from(self.window_days) {
                if let Some(high) = summary.high_temp_c {
                    matches += 1;
                    if high >= threshold_high_c {
                        hits += 1;
                    }
                }
            }
        }

        if matches == 0 {
            return ProbabilityMethodOutcome::Unavailable(MethodAvailability {
                method: self.name().to_owned(),
                reason: "no daily summaries matched the seasonal window".to_owned(),
            });
        }

        ProbabilityMethodOutcome::Available(ProbabilityEstimate {
            method: self.name().to_owned(),
            probability: hits as f64 / matches as f64,
            sample_size: matches,
            note: Some(format!("window=+/-{} calendar days", self.window_days)),
        })
    }
}

pub struct TrajectoryMethod;

impl ProbabilityMethod for TrajectoryMethod {
    fn name(&self) -> &'static str {
        "temperature-trajectory"
    }

    fn estimate(
        &self,
        _station_id: &StationId,
        target_date: NaiveDate,
        threshold_high_c: f64,
        daily: &[DailySummary],
        profiles: &[DayProfile],
        target_profile: Option<&DayProfile>,
        as_of_hour: Option<u8>,
    ) -> ProbabilityMethodOutcome {
        let Some(target_profile) = target_profile else {
            return ProbabilityMethodOutcome::Unavailable(MethodAvailability {
                method: self.name().to_owned(),
                reason: "no target profile is available".to_owned(),
            });
        };

        let limit_hour =
            as_of_hour.unwrap_or_else(|| latest_observed_hour(target_profile).unwrap_or(23));
        let Some((target_temp, target_slope)) =
            target_temperature_signature(target_profile, limit_hour)
        else {
            return ProbabilityMethodOutcome::Unavailable(MethodAvailability {
                method: self.name().to_owned(),
                reason: "not enough observed hours to compute a temperature trajectory".to_owned(),
            });
        };

        let daily_highs: HashMap<NaiveDate, Option<f64>> = daily
            .iter()
            .map(|summary| (summary.local_date, summary.high_temp_c))
            .collect();
        let mut weighted_hits = 0.0;
        let mut total_weight = 0.0;
        let mut sample_size = 0usize;

        for profile in profiles.iter().filter(|profile| profile.local_date != target_date) {
            let Some((candidate_temp, candidate_slope)) =
                target_temperature_signature(profile, limit_hour)
            else {
                continue;
            };
            let Some(high) = daily_highs.get(&profile.local_date).copied().flatten() else {
                continue;
            };
            let temp_delta = (candidate_temp - target_temp) / 8.0;
            let slope_delta = (candidate_slope - target_slope) / 4.0;
            let distance = (temp_delta * temp_delta + slope_delta * slope_delta).sqrt();
            let weight = 1.0 / (distance + 0.1);
            total_weight += weight;
            sample_size += 1;
            if high >= threshold_high_c {
                weighted_hits += weight;
            }
        }

        if total_weight <= f64::EPSILON {
            return ProbabilityMethodOutcome::Unavailable(MethodAvailability {
                method: self.name().to_owned(),
                reason: "no historical profiles had enough same-hour temperature coverage"
                    .to_owned(),
            });
        }

        ProbabilityMethodOutcome::Available(ProbabilityEstimate {
            method: self.name().to_owned(),
            probability: weighted_hits / total_weight,
            sample_size,
            note: Some(format!(
                "compares latest temperature and warming rate through hour {}",
                limit_hour
            )),
        })
    }
}

pub fn build_probability_breakdown(
    station_id: StationId,
    target_date: NaiveDate,
    threshold_high_c: f64,
    daily: &[DailySummary],
    profiles: &[DayProfile],
    target_profile: Option<&DayProfile>,
    as_of_hour: Option<u8>,
) -> ProbabilityBreakdown {
    let methods: Vec<Box<dyn ProbabilityMethod>> = vec![
        Box::new(ClimatologyMethod { window_days: 15 }),
        Box::new(TrajectoryMethod),
    ];
    let mut estimates = Vec::new();
    let mut unavailable_methods = Vec::new();
    for method in methods {
        match method.estimate(
            &station_id,
            target_date,
            threshold_high_c,
            daily,
            profiles,
            target_profile,
            as_of_hour,
        ) {
            ProbabilityMethodOutcome::Available(estimate) => estimates.push(estimate),
            ProbabilityMethodOutcome::Unavailable(unavailable) => {
                unavailable_methods.push(unavailable)
            }
        }
    }

    let analog_estimate = analog_probability_estimate(
        &station_id,
        target_date,
        threshold_high_c,
        daily,
        profiles,
        target_profile,
        as_of_hour,
    );
    match analog_estimate {
        ProbabilityMethodOutcome::Available(estimate) => estimates.push(estimate),
        ProbabilityMethodOutcome::Unavailable(unavailable) => unavailable_methods.push(unavailable),
    }

    let combined_probability = if estimates.len() >= 2 {
        Some(
            estimates
                .iter()
                .map(|estimate| estimate.probability)
                .sum::<f64>()
                / estimates.len() as f64,
        )
    } else {
        None
    };

    ProbabilityBreakdown {
        station_id,
        target_date,
        threshold_high_c,
        methods: estimates,
        unavailable_methods,
        combined_probability,
    }
}

pub fn top_analogs(
    station_id: &StationId,
    target_date: NaiveDate,
    daily: &[DailySummary],
    profiles: &[DayProfile],
    target_profile: &DayProfile,
    as_of_hour: Option<u8>,
    top_n: usize,
) -> Vec<AnalogResult> {
    let daily_highs: HashMap<NaiveDate, Option<f64>> = daily
        .iter()
        .map(|summary| (summary.local_date, summary.high_temp_c))
        .collect();

    let mut analogs = profiles
        .iter()
        .filter(|profile| profile.local_date != target_date)
        .filter_map(|profile| {
            let (distance, compared_hours) = profile_distance(target_profile, profile, as_of_hour)?;
            Some(AnalogResult {
                station_id: station_id.clone(),
                target_date,
                analog_date: profile.local_date,
                distance,
                observed_high_c: daily_highs.get(&profile.local_date).copied().flatten(),
                compared_hours,
            })
        })
        .collect::<Vec<_>>();

    analogs.sort_by(|left, right| left.distance.total_cmp(&right.distance));
    analogs.truncate(top_n);
    analogs
}

pub fn target_date_or_today(date: Option<NaiveDate>, today: bool) -> Result<NaiveDate> {
    match (date, today) {
        (Some(date), false) => Ok(date),
        (None, true) => Ok(Local::now().date_naive()),
        (None, false) => bail!("pass either --date YYYY-MM-DD or --today"),
        (Some(_), true) => bail!("--date and --today are mutually exclusive"),
    }
}

fn analog_probability_estimate(
    station_id: &StationId,
    target_date: NaiveDate,
    threshold_high_c: f64,
    daily: &[DailySummary],
    profiles: &[DayProfile],
    target_profile: Option<&DayProfile>,
    as_of_hour: Option<u8>,
) -> ProbabilityMethodOutcome {
    let Some(target_profile) = target_profile else {
        return ProbabilityMethodOutcome::Unavailable(MethodAvailability {
            method: analog_method_name(as_of_hour).to_owned(),
            reason: "no target profile is available".to_owned(),
        });
    };
    let analogs = top_analogs(
        station_id,
        target_date,
        daily,
        profiles,
        target_profile,
        as_of_hour,
        30,
    );
    if analogs.is_empty() {
        return ProbabilityMethodOutcome::Unavailable(MethodAvailability {
            method: analog_method_name(as_of_hour).to_owned(),
            reason: "no comparable analog profiles were found".to_owned(),
        });
    }

    let mut weighted_hits = 0.0;
    let mut total_weight = 0.0;
    for analog in &analogs {
        if let Some(high) = analog.observed_high_c {
            let weight = 1.0 / (analog.distance + 0.05);
            total_weight += weight;
            if high >= threshold_high_c {
                weighted_hits += weight;
            }
        }
    }

    if total_weight <= f64::EPSILON {
        return ProbabilityMethodOutcome::Unavailable(MethodAvailability {
            method: analog_method_name(as_of_hour).to_owned(),
            reason: "analog matches did not include usable observed highs".to_owned(),
        });
    }

    ProbabilityMethodOutcome::Available(ProbabilityEstimate {
        method: analog_method_name(as_of_hour).to_owned(),
        probability: weighted_hits / total_weight,
        sample_size: analogs.len(),
        note: Some("weighted by inverse profile distance".to_owned()),
    })
}

fn profile_distance(
    left: &DayProfile,
    right: &DayProfile,
    as_of_hour: Option<u8>,
) -> Option<(f64, usize)> {
    let mut total = 0.0;
    let mut count = 0usize;

    for hour in 0..24usize {
        if let Some(limit) = as_of_hour {
            if hour as u8 > limit {
                break;
            }
        }
        let left_hour = left.hours.get(hour)?;
        let right_hour = right.hours.get(hour)?;
        if let Some(point_distance) = hourly_distance(left_hour, right_hour) {
            total += point_distance;
            count += 1;
        }
    }

    let min_required = if as_of_hour.is_some() { 2 } else { 6 };
    if count < min_required {
        return None;
    }

    Some((total / count as f64, count))
}

fn analog_method_name(as_of_hour: Option<u8>) -> &'static str {
    if as_of_hour.is_some() {
        "partial-profile-analogs"
    } else {
        "nearest-neighbor-analogs"
    }
}

fn latest_observed_hour(profile: &DayProfile) -> Option<u8> {
    profile
        .hours
        .iter()
        .rev()
        .find(|hour| hour.sample_count > 0 && hour.temperature_c.is_some())
        .map(|hour| hour.hour)
}

fn target_temperature_signature(profile: &DayProfile, limit_hour: u8) -> Option<(f64, f64)> {
    let mut observed = profile
        .hours
        .iter()
        .filter(|hour| hour.hour <= limit_hour)
        .filter_map(|hour| hour.temperature_c.map(|temp| (hour.hour, temp)))
        .collect::<Vec<_>>();
    observed.sort_by_key(|(hour, _)| *hour);
    let (first_hour, first_temp) = *observed.first()?;
    let (last_hour, last_temp) = *observed.last()?;
    if observed.len() < 2 || first_hour == last_hour {
        return None;
    }
    let slope = (last_temp - first_temp) / f64::from(last_hour - first_hour);
    Some((last_temp, slope))
}

fn hourly_distance(left: &HourlyProfilePoint, right: &HourlyProfilePoint) -> Option<f64> {
    let mut total = 0.0;
    let mut count = 0usize;

    collect_distance(
        &mut total,
        &mut count,
        left.temperature_c,
        right.temperature_c,
        10.0,
    );
    collect_distance(
        &mut total,
        &mut count,
        left.dewpoint_c,
        right.dewpoint_c,
        10.0,
    );
    collect_distance(
        &mut total,
        &mut count,
        left.relative_humidity_pct,
        right.relative_humidity_pct,
        20.0,
    );
    collect_distance(
        &mut total,
        &mut count,
        left.wind_u_kt,
        right.wind_u_kt,
        10.0,
    );
    collect_distance(
        &mut total,
        &mut count,
        left.wind_v_kt,
        right.wind_v_kt,
        10.0,
    );
    collect_distance(
        &mut total,
        &mut count,
        left.cloud_cover_fraction,
        right.cloud_cover_fraction,
        0.5,
    );
    collect_distance(
        &mut total,
        &mut count,
        left.precipitation_mm,
        right.precipitation_mm,
        5.0,
    );

    if count == 0 {
        return None;
    }
    Some((total / count as f64).sqrt())
}

fn collect_distance(
    total: &mut f64,
    count: &mut usize,
    left: Option<f64>,
    right: Option<f64>,
    scale: f64,
) {
    if let (Some(left), Some(right)) = (left, right) {
        let delta = (left - right) / scale;
        *total += delta * delta;
        *count += 1;
    }
}

fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

fn seasonal_distance(left: i32, right: i32) -> i32 {
    let delta = (left - right).abs();
    delta.min(366 - delta)
}

#[cfg(test)]
mod tests {
    use chrono::{FixedOffset, NaiveDateTime, TimeZone};

    use crate::domain::{ObservationRecord, StationId};
    use crate::source::DataSource;

    use super::{
        ClimatologyMethod, DailySummaryBuilder, DayProfileBuilder, DerivedDatasetBuilder,
        ProbabilityMethod, TrajectoryMethod, build_probability_breakdown, dedupe_observations,
        top_analogs,
    };

    fn observation(station: &str, date: &str, temp: f64) -> ObservationRecord {
        let offset = FixedOffset::west_opt(6 * 3600).unwrap();
        let naive = NaiveDateTime::parse_from_str(date, "%Y-%m-%d %H:%M").unwrap();
        let dt = offset.from_local_datetime(&naive).single().unwrap();
        let mut observation = ObservationRecord::from_parts(
            StationId::new(station),
            DataSource::IemAsosOneMinute,
            "DSM".to_owned(),
            dt,
            "raw.csv".to_owned(),
        );
        observation.temperature_c = Some(temp);
        observation.dewpoint_c = Some(temp - 5.0);
        observation.relative_humidity_pct = Some(50.0);
        observation.wind_u_kt = Some(1.0);
        observation.wind_v_kt = Some(1.0);
        observation
    }

    #[test]
    fn aggregates_daily_summaries() {
        let observations = vec![
            observation("KDSM", "2026-05-14 00:00", 20.0),
            observation("KDSM", "2026-05-14 01:00", 24.0),
            observation("KDSM", "2026-05-14 02:00", 18.0),
        ];
        let summaries = DailySummaryBuilder.build(&observations).unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].high_temp_c, Some(24.0));
        assert_eq!(summaries[0].low_temp_c, Some(18.0));
    }

    #[test]
    fn builds_profiles() {
        let observations = vec![
            observation("KDSM", "2026-05-14 00:00", 20.0),
            observation("KDSM", "2026-05-14 00:30", 22.0),
            observation("KDSM", "2026-05-14 01:00", 24.0),
        ];
        let profiles = DayProfileBuilder.build(&observations).unwrap();
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].observed_hour_count, 2);
        assert_eq!(profiles[0].hours[0].temperature_c, Some(21.0));
    }

    #[test]
    fn dedupes_observations_by_source_station_and_time() {
        let left = observation("KDSM", "2026-05-14 00:00", 20.0);
        let right = observation("KDSM", "2026-05-14 00:00", 22.0);
        let deduped = dedupe_observations(vec![left.clone(), right]);
        assert_eq!(deduped.len(), 1);
        assert_eq!(deduped[0].temperature_c, left.temperature_c);
    }

    #[test]
    fn prefers_ncei_over_iem_for_same_timestamp() {
        let mut iem = observation("KDSM", "2026-05-14 00:00", 20.0);
        let mut ncei = observation("KDSM", "2026-05-14 00:00", 19.0);
        ncei.source = DataSource::NceiAsosFiveMinute;
        let deduped = dedupe_observations(vec![iem.clone(), ncei.clone()]);
        assert_eq!(deduped.len(), 1);
        assert_eq!(deduped[0].source, DataSource::NceiAsosFiveMinute);
        assert_eq!(deduped[0].temperature_c, ncei.temperature_c);
        iem.source = DataSource::Ghcnh;
        let deduped = dedupe_observations(vec![iem, ncei.clone()]);
        assert_eq!(deduped[0].source, DataSource::NceiAsosFiveMinute);
    }

    #[test]
    fn climatology_estimate_counts_hits() {
        let observations = vec![
            observation("KDSM", "2024-05-14 00:00", 25.0),
            observation("KDSM", "2025-05-14 00:00", 28.0),
            observation("KDSM", "2026-05-14 00:00", 18.0),
        ];
        let daily = DailySummaryBuilder.build(&observations).unwrap();
        let method = ClimatologyMethod { window_days: 1 };
        match method.estimate(
            &StationId::new("KDSM"),
            chrono::NaiveDate::from_ymd_opt(2026, 5, 14).unwrap(),
            24.0,
            &daily,
            &[],
            None,
            None,
        ) {
            super::ProbabilityMethodOutcome::Available(estimate) => {
                assert_eq!(estimate.sample_size, 3);
                assert!((estimate.probability - (2.0 / 3.0)).abs() < 1e-9);
            }
            super::ProbabilityMethodOutcome::Unavailable(reason) => {
                panic!("climatology unexpectedly unavailable: {reason:?}")
            }
        }
    }

    #[test]
    fn produces_analogs_and_combined_probability() {
        let observations = vec![
            observation("KDSM", "2024-05-14 00:00", 25.0),
            observation("KDSM", "2024-05-14 01:00", 26.0),
            observation("KDSM", "2025-05-14 00:00", 25.0),
            observation("KDSM", "2025-05-14 01:00", 26.0),
            observation("KDSM", "2026-05-14 00:00", 25.0),
            observation("KDSM", "2026-05-14 01:00", 26.0),
        ];
        let daily = DailySummaryBuilder.build(&observations).unwrap();
        let profiles = DayProfileBuilder.build(&observations).unwrap();
        let target_date = chrono::NaiveDate::from_ymd_opt(2026, 5, 14).unwrap();
        let target_profile = profiles
            .iter()
            .find(|profile| profile.local_date == target_date)
            .unwrap();

        let analogs = top_analogs(
            &StationId::new("KDSM"),
            target_date,
            &daily,
            &profiles,
            target_profile,
            Some(1),
            5,
        );
        assert_eq!(analogs.len(), 2);

        let breakdown = build_probability_breakdown(
            StationId::new("KDSM"),
            target_date,
            24.0,
            &daily,
            &profiles,
            Some(target_profile),
            Some(1),
        );
        assert!(breakdown.methods.len() >= 2);
        assert!(breakdown.combined_probability.is_some());
        assert!(breakdown.unavailable_methods.is_empty());
    }

    #[test]
    fn sparse_profiles_can_still_match_when_some_hours_are_missing() {
        let mut left = observation("KDSM", "2026-05-14 00:00", 20.0);
        let mut right = observation("KDSM", "2026-05-13 00:00", 20.0);
        left.relative_humidity_pct = None;
        right.relative_humidity_pct = None;
        let observations = vec![
            left,
            observation("KDSM", "2026-05-14 03:00", 24.0),
            right,
            observation("KDSM", "2026-05-13 03:00", 24.0),
        ];
        let daily = DailySummaryBuilder.build(&observations).unwrap();
        let profiles = DayProfileBuilder.build(&observations).unwrap();
        let target_date = chrono::NaiveDate::from_ymd_opt(2026, 5, 14).unwrap();
        let target_profile = profiles
            .iter()
            .find(|profile| profile.local_date == target_date)
            .unwrap();
        let analogs = top_analogs(
            &StationId::new("KDSM"),
            target_date,
            &daily,
            &profiles,
            target_profile,
            Some(3),
            5,
        );
        assert_eq!(analogs.len(), 1);
        assert_eq!(analogs[0].compared_hours, 2);
    }

    #[test]
    fn reports_unavailable_methods_when_target_profile_is_missing() {
        let observations = vec![observation("KDSM", "2024-05-14 00:00", 25.0)];
        let daily = DailySummaryBuilder.build(&observations).unwrap();
        let profiles = DayProfileBuilder.build(&observations).unwrap();
        let breakdown = build_probability_breakdown(
            StationId::new("KDSM"),
            chrono::NaiveDate::from_ymd_opt(2026, 5, 14).unwrap(),
            24.0,
            &daily,
            &profiles,
            None,
            Some(4),
        );
        assert!(
            breakdown
                .unavailable_methods
                .iter()
                .any(|method| method.method == "partial-profile-analogs")
        );
        assert!(
            breakdown
                .unavailable_methods
                .iter()
                .any(|method| method.method == "temperature-trajectory")
        );
    }

    #[test]
    fn trajectory_method_estimates_when_hours_exist() {
        let observations = vec![
            observation("KDSM", "2024-05-14 00:00", 20.0),
            observation("KDSM", "2024-05-14 03:00", 24.0),
            observation("KDSM", "2025-05-14 00:00", 21.0),
            observation("KDSM", "2025-05-14 03:00", 25.0),
            observation("KDSM", "2026-05-14 00:00", 20.5),
            observation("KDSM", "2026-05-14 03:00", 24.5),
        ];
        let daily = DailySummaryBuilder.build(&observations).unwrap();
        let profiles = DayProfileBuilder.build(&observations).unwrap();
        let target_date = chrono::NaiveDate::from_ymd_opt(2026, 5, 14).unwrap();
        let target_profile = profiles
            .iter()
            .find(|profile| profile.local_date == target_date)
            .unwrap();
        match TrajectoryMethod.estimate(
            &StationId::new("KDSM"),
            target_date,
            23.0,
            &daily,
            &profiles,
            Some(target_profile),
            Some(3),
        ) {
            super::ProbabilityMethodOutcome::Available(estimate) => {
                assert!(estimate.probability > 0.0);
                assert!(estimate.sample_size >= 2);
            }
            super::ProbabilityMethodOutcome::Unavailable(reason) => {
                panic!("trajectory method unexpectedly unavailable: {reason:?}")
            }
        }
    }
}
