use chrono::{DateTime, Utc};
use helium_proto::services::poc_mobile::RadioUsageStatsIngestReportV1;
use import_derive::Import;

use crate::{PublicKeyBinary, determine_timestamp};

#[derive(Debug, Import)]
#[import(s3decode(
    proto = RadioUsageStatsIngestReportV1,
    bucket = "helium-mainnet-mobile-ingest",
    prefix = "radio_usage_stats_ingest_report",
))]
pub struct RadioUsageStats {
    hotspot_key: String,
    #[import(sql = "timestamptz")]
    start_period: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    end_period: DateTime<Utc>,
    #[import(sql = "uint64")]
    service_provider_user_count: u64,
    #[import(sql = "uint64")]
    disco_mapping_user_count: u64,
    #[import(sql = "uint64")]
    offload_user_count: u64,
    #[import(sql = "uint64")]
    service_provider_transfer_bytes: u64,
    #[import(sql = "uint64")]
    offload_transfer_bytes: u64,
    #[import(sql = "timestamptz")]
    timestamp: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    received_timestamp: DateTime<Utc>,
}

impl From<RadioUsageStatsIngestReportV1> for RadioUsageStats {
    fn from(value: RadioUsageStatsIngestReportV1) -> Self {
        let req = value.report.as_ref().unwrap();

        Self {
            hotspot_key: PublicKeyBinary::from(req.hotspot_pubkey.clone()).to_string(),
            start_period: determine_timestamp(req.epoch_start_timestamp),
            end_period: determine_timestamp(req.epoch_end_timestamp),
            service_provider_user_count: req.service_provider_user_count,
            disco_mapping_user_count: req.disco_mapping_user_count,
            offload_user_count: req.offload_user_count,
            service_provider_transfer_bytes: req.service_provider_transfer_bytes,
            offload_transfer_bytes: req.offload_transfer_bytes,
            timestamp: determine_timestamp(req.timestamp),
            received_timestamp: determine_timestamp(value.received_timestamp),
        }
    }
}
