use chrono::{DateTime, Utc};
use helium_proto::services::poc_mobile::Heartbeat;
use huckli_import_derive::Import;
use uuid::Uuid;

use crate::{PublicKeyBinary, determine_timestamp};

#[derive(Debug, Import)]
#[import(s3decode(
    proto = Heartbeat,
    bucket = "helium-mainnet-mobile-verified",
    prefix = "validated_heartbeat"
))]
pub struct VerifiedWifiHeartbeat {
    hotspot_key: String,
    #[import(sql = "timestamptz")]
    timestamp: DateTime<Utc>,
    validity: String,
    #[import(sql = "double")]
    lat: f64,
    #[import(sql = "double")]
    lon: f64,
    coverage_object: String,
    #[import(sql = "timestamptz")]
    location_validation_timestamp: DateTime<Utc>,
    #[import(sql = "uint64")]
    distance_to_asserted: u64,
    #[import(sql = "uint32")]
    location_trust_score_multiplier: u32,
    location_source: String,
}

impl From<Heartbeat> for VerifiedWifiHeartbeat {
    fn from(value: Heartbeat) -> Self {
        Self {
            hotspot_key: PublicKeyBinary::from(value.pub_key.clone()).to_string(),
            timestamp: determine_timestamp(value.timestamp),
            validity: value.validity().as_str_name().to_string(),
            lat: value.lat,
            lon: value.lon,
            coverage_object: Uuid::from_slice(&value.coverage_object)
                .unwrap()
                .to_string(),
            location_validation_timestamp: determine_timestamp(value.location_validation_timestamp),
            distance_to_asserted: value.distance_to_asserted,
            location_trust_score_multiplier: value.location_trust_score_multiplier,
            location_source: value.location_source().as_str_name().to_string(),
        }
    }
}
