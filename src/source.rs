use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ValueEnum)]
pub enum DataSource {
    IemAsosOneMinute,
    NceiAsosFiveMinute,
    NwsApi,
    Ghcnh,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct SourceDescriptor {
    pub source: DataSource,
    pub slug: &'static str,
    pub summary: &'static str,
    pub cadence: &'static str,
    pub scope: &'static str,
}

impl DataSource {
    pub fn slug(self) -> &'static str {
        match self {
            Self::IemAsosOneMinute => "iem-asos-1min",
            Self::NceiAsosFiveMinute => "ncei-asos-5min",
            Self::NwsApi => "nws-api",
            Self::Ghcnh => "ghcnh",
        }
    }

    pub fn summary(self) -> &'static str {
        match self {
            Self::IemAsosOneMinute => {
                "Processed ASOS observations with convenient station history access."
            }
            Self::NceiAsosFiveMinute => {
                "Official NOAA ASOS five-minute archive for authoritative U.S. station history."
            }
            Self::NwsApi => {
                "Live and recent National Weather Service observations for current-day state."
            }
            Self::Ghcnh => "Hourly global station archive fallback for broad geographic coverage.",
        }
    }

    pub fn cadence(self) -> &'static str {
        match self {
            Self::IemAsosOneMinute => "1 minute",
            Self::NceiAsosFiveMinute => "5 minutes",
            Self::NwsApi => "live/recent",
            Self::Ghcnh => "hourly",
        }
    }

    pub fn scope(self) -> &'static str {
        match self {
            Self::IemAsosOneMinute => "U.S. ASOS/METAR",
            Self::NceiAsosFiveMinute => "U.S. ASOS",
            Self::NwsApi => "U.S. NWS stations",
            Self::Ghcnh => "global",
        }
    }
}

impl SourceDescriptor {
    pub fn from_source(source: DataSource) -> Self {
        Self {
            source,
            slug: source.slug(),
            summary: source.summary(),
            cadence: source.cadence(),
            scope: source.scope(),
        }
    }
}

pub fn all_sources() -> Vec<DataSource> {
    vec![
        DataSource::IemAsosOneMinute,
        DataSource::NceiAsosFiveMinute,
        DataSource::NwsApi,
        DataSource::Ghcnh,
    ]
}
