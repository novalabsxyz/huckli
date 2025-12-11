use chrono::{DateTime, Utc};
use helium_proto::services::poc_mobile::EnabledCarriersInfoReportV1;
use huckli_import_derive::Import;

use crate::{PublicKeyBinary, determine_timestamp};

#[derive(Debug, Import)]
#[import(s3decode(
    proto = EnabledCarriersInfoReportV1,
    bucket = "helium-mainnet-mobile-ingest",
    prefix = "enabled_carriers_report",
))]
pub struct EnabledCarriersInfo {
    hotspot_key: String,
    #[import(sql = "json")]
    enabled_carriers: serde_json::Value,
    sampled_carriers: serde_json::Value,
    firmware_version: String,
    timestamp_ms: DateTime<Utc>,
}

impl From<EnabledCarriersInfoReportV1> for EnabledCarriersInfo {
    fn from(v: EnabledCarriersInfoReportV1) -> Self {
        let req = v.report.as_ref().unwrap();

        let enabled_carriers: Vec<String> = req
            .enabled_carriers()
            .map(|v| v.as_str_name().to_string())
            .collect();

        let sampled_carriers: Vec<String> = req
            .sampling_enabled_carriers()
            .map(|v| v.as_str_name().to_string())
            .collect();

        Self {
            hotspot_key: PublicKeyBinary::from(req.hotspot_pubkey.clone()).to_string(),
            enabled_carriers: serde_json::to_value(enabled_carriers).unwrap(),
            sampled_carriers: serde_json::to_value(sampled_carriers).unwrap(),
            firmware_version: req.firmware_version.clone(),
            timestamp_ms: determine_timestamp(req.timestamp_ms),
        }
    }
}
