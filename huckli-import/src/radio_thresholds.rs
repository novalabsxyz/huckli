use chrono::{DateTime, Utc};
use helium_proto::services::poc_mobile::{
    VerifiedInvalidatedRadioThresholdIngestReportV1, VerifiedRadioThresholdIngestReportV1,
};
use huckli_import_derive::Import;

use crate::{PublicKeyBinary, determine_timestamp};

#[derive(Debug, Import)]
#[import(s3decode(
    proto = VerifiedRadioThresholdIngestReportV1,
    bucket = "helium-mainnet-mobile-verified",
    prefix = "verified_radio_threshold_report",
))]
pub struct VerifiedRadioThreshold {
    radio_key: String,
    #[import(sql = "uint64")]
    bytes_threshold: u64,
    #[import(sql = "uint32")]
    subscriber_threshold: u32,
    #[import(sql = "timestamptz")]
    threshold_timestamp: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    received_timestamp: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    verified_timestamp: DateTime<Utc>,
    status: String,
}

impl From<VerifiedRadioThresholdIngestReportV1> for VerifiedRadioThreshold {
    fn from(value: VerifiedRadioThresholdIngestReportV1) -> Self {
        let ingest = value.report.as_ref().unwrap();
        let req = ingest.report.as_ref().unwrap();

        let radio_key = if !req.hotspot_pubkey.is_empty() {
            PublicKeyBinary::from(req.hotspot_pubkey.clone()).to_string()
        } else {
            req.cbsd_id.clone()
        };

        Self {
            radio_key,
            bytes_threshold: req.bytes_threshold,
            subscriber_threshold: req.subscriber_threshold,
            threshold_timestamp: determine_timestamp(req.threshold_timestamp),
            received_timestamp: determine_timestamp(ingest.received_timestamp),
            verified_timestamp: determine_timestamp(value.timestamp),
            status: value.status().as_str_name().to_string(),
        }
    }
}

#[derive(Debug, Import)]
#[import(s3decode(
    proto = VerifiedInvalidatedRadioThresholdIngestReportV1,
    bucket = "helium-mainnet-mobile-verified",
    prefix = "verified_invalidated_radio_threshold_report",
))]
pub struct VerifiedInvalidatedRadioThreshold {
    radio_key: String,
    reason: String,
    #[import(sql = "timestamptz")]
    threshold_timestamp: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    received_timestamp: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    verified_timestamp: DateTime<Utc>,
    status: String,
}

impl From<VerifiedInvalidatedRadioThresholdIngestReportV1> for VerifiedInvalidatedRadioThreshold {
    fn from(value: VerifiedInvalidatedRadioThresholdIngestReportV1) -> Self {
        let ingest = value.report.as_ref().unwrap();
        let req = ingest.report.as_ref().unwrap();

        let radio_key = if !req.hotspot_pubkey.is_empty() {
            PublicKeyBinary::from(req.hotspot_pubkey.clone()).to_string()
        } else {
            req.cbsd_id.clone()
        };

        Self {
            radio_key,
            reason: req.reason().as_str_name().to_string(),
            threshold_timestamp: determine_timestamp(req.timestamp),
            received_timestamp: determine_timestamp(ingest.received_timestamp),
            verified_timestamp: determine_timestamp(value.timestamp),
            status: value.status().as_str_name().to_string(),
        }
    }
}
