use chrono::{DateTime, Utc};
use helium_proto::services::poc_mobile::RadioUsageStatsIngestReportV2;
use huckli_import_derive::Import;

use crate::{determine_timestamp, PublicKeyBinary};

#[derive(Debug, Import)]
#[import(s3decode(
    proto = RadioUsageStatsIngestReportV2,
    bucket = "helium-mainnet-mobile-ingest",
    prefix = "radio_usage_stats_ingest_report_v2",
))]
pub struct RadioUsageStatsV2 {
    hotspot_key: String,
    #[import(sql = "timestamptz")]
    start_period: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    end_period: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    timestamp: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    received_timestamp: DateTime<Utc>,
    carrier_pubkey: String,
    #[import(sql = "uint64")]
    user_count_total: u64,
    #[import(sql = "uint64")]
    rewarded_bytes_transferred_total: u64,
    #[import(sql = "uint64")]
    unrewarded_bytes_transferred_total: u64,
    #[import(sql = "uint64")]
    sampling_user_count_total: u64,
    #[import(sql = "uint64")]
    sampling_bytes_transferred_total: u64,
    #[import(sql = "json")]
    carrier_transfer_info: serde_json::Value,
    #[import(sql = "json")]
    sampling_carrier_transfer_info: serde_json::Value,
}

impl From<RadioUsageStatsIngestReportV2> for RadioUsageStatsV2 {
    fn from(value: RadioUsageStatsIngestReportV2) -> Self {
        let req = value.report.as_ref().unwrap();

        // Convert carrier_transfer_info to JSON
        let carrier_transfer_info = req
            .carrier_transfer_info
            .iter()
            .map(|i| {
                let mut m = serde_json::Map::new();
                m.insert(
                    "carrier_id".to_string(),
                    serde_json::Value::String(i.carrier_id().as_str_name().to_string()),
                );
                m.insert(
                    "user_count".to_string(),
                    serde_json::Value::Number(i.user_count.into()),
                );
                m.insert(
                    "rewarded_bytes_transferred".to_string(),
                    serde_json::Value::Number(i.rewarded_bytes_transferred.into()),
                );
                m.insert(
                    "unrewarded_bytes_transferred".to_string(),
                    serde_json::Value::Number(i.unrewarded_bytes_transferred.into()),
                );
                serde_json::Value::Object(m)
            })
            .collect::<Vec<_>>();

        // Convert sampling_carrier_transfer_info to JSON
        let sampling_carrier_transfer_info = req
            .sampling_carrier_transfer_info
            .iter()
            .map(|i| {
                let mut m = serde_json::Map::new();
                m.insert(
                    "carrier_id".to_string(),
                    serde_json::Value::String(i.carrier_id().as_str_name().to_string()),
                );
                m.insert(
                    "user_count".to_string(),
                    serde_json::Value::Number(i.user_count.into()),
                );
                m.insert(
                    "bytes_transferred".to_string(),
                    serde_json::Value::Number(i.bytes_transferred.into()),
                );
                serde_json::Value::Object(m)
            })
            .collect::<Vec<_>>();

        Self {
            hotspot_key: PublicKeyBinary::from(req.hotspot_pubkey.clone()).to_string(),
            start_period: determine_timestamp(req.epoch_start_timestamp_ms),
            end_period: determine_timestamp(req.epoch_end_timestamp_ms),
            timestamp: determine_timestamp(req.timestamp_ms),
            received_timestamp: determine_timestamp(value.received_timestamp_ms),
            carrier_pubkey: PublicKeyBinary::from(req.carrier_pubkey.clone()).to_string(),
            user_count_total: req.user_count_total,
            rewarded_bytes_transferred_total: req.rewarded_bytes_transferred_total,
            unrewarded_bytes_transferred_total: req.unrewarded_bytes_transferred_total,
            sampling_user_count_total: req.sampling_user_count_total,
            sampling_bytes_transferred_total: req.sampling_bytes_transferred_total,
            carrier_transfer_info: serde_json::Value::Array(carrier_transfer_info),
            sampling_carrier_transfer_info: serde_json::Value::Array(
                sampling_carrier_transfer_info,
            ),
        }
    }
}
