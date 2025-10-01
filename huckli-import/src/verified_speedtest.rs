use chrono::{DateTime, Utc};
use helium_proto::services::poc_mobile::VerifiedSpeedtest;
use huckli_import_derive::Import;

use crate::{PublicKeyBinary, determine_timestamp};

#[derive(Debug, Import)]
#[import(s3decode(
    proto = VerifiedSpeedtest,
    bucket = "helium-mainnet-mobile-verified",
    prefix = "verified_speedtest"
))]
pub struct VerifiedSpeedtestReport {
    hotspot_key: String,
    serial: String,
    #[import(sql = "timestamptz")]
    timestamp: DateTime<Utc>,
    #[import(sql = "uint64")]
    upload_speed: u64,
    #[import(sql = "uint64")]
    download_speed: u64,
    #[import(sql = "uint32")]
    latency: u32,
    #[import(sql = "timestamptz")]
    received_timestamp: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    verified_timestamp: DateTime<Utc>,
    result: String,
}

impl From<VerifiedSpeedtest> for VerifiedSpeedtestReport {
    fn from(value: VerifiedSpeedtest) -> Self {
        let ingest = value.report.as_ref().unwrap();
        let req = ingest.report.as_ref().unwrap();

        Self {
            hotspot_key: PublicKeyBinary::from(req.pub_key.clone()).to_string(),
            serial: req.serial.clone(),
            timestamp: determine_timestamp(req.timestamp),
            upload_speed: req.upload_speed,
            download_speed: req.download_speed,
            latency: req.latency,
            received_timestamp: determine_timestamp(ingest.received_timestamp),
            verified_timestamp: determine_timestamp(value.timestamp),
            result: value.result().as_str_name().to_string(),
        }
    }
}
