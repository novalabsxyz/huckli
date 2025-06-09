use chrono::{DateTime, Utc};
use helium_proto::services::poc_mobile::{
    DataTransferSessionIngestReportV1, VerifiedDataTransferIngestReportV1,
};
use import_derive::Import;

use crate::{PublicKeyBinary, determine_timestamp};

#[derive(Debug, Import)]
#[import(s3decode(
    proto = DataTransferSessionIngestReportV1,
    bucket = "helium-mainnet-mobile-ingest",
    prefix = "data_transfer_session_ingest_report",
))]
pub struct DataTransferIngestReport {
    hotspot_key: String,
    #[import(sql = "uint64")]
    upload_bytes: u64,
    #[import(sql = "uint64")]
    download_bytes: u64,
    #[import(sql = "uint64")]
    rewardable_bytes: u64,
    technology: String,
    event_id: String,
    payer: String,
    #[import(sql = "timestamptz")]
    timestamp: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    received_timestamp: DateTime<Utc>,
}

impl From<DataTransferSessionIngestReportV1> for DataTransferIngestReport {
    fn from(ingest: DataTransferSessionIngestReportV1) -> Self {
        let req = ingest.report.as_ref().unwrap();
        let event = req.data_transfer_usage.as_ref().unwrap();

        Self {
            hotspot_key: PublicKeyBinary::from(event.pub_key.clone()).to_string(),
            upload_bytes: event.upload_bytes,
            download_bytes: event.download_bytes,
            rewardable_bytes: req.rewardable_bytes,
            technology: event.radio_access_technology().as_str_name().to_string(),
            event_id: event.event_id.clone(),
            payer: PublicKeyBinary::from(event.payer.clone()).to_string(),
            timestamp: determine_timestamp(event.timestamp),
            received_timestamp: determine_timestamp(ingest.received_timestamp),
        }
    }
}

#[derive(Debug, Import)]
#[import(s3decode(
    proto = VerifiedDataTransferIngestReportV1,
    bucket = "helium-mainnet-mobile-packet-verifier",
    prefix = "verified_data_transfer_session",
))]
pub struct VerifiedDataTransferIngestReport {
    hotspot_key: String,
    #[import(sql = "uint64")]
    upload_bytes: u64,
    #[import(sql = "uint64")]
    download_bytes: u64,
    #[import(sql = "uint64")]
    rewardable_bytes: u64,
    technology: String,
    event_id: String,
    payer: String,
    #[import(sql = "timestamptz")]
    timestamp: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    received_timestamp: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    verified_timestamp: DateTime<Utc>,
    status: String,
}

impl From<VerifiedDataTransferIngestReportV1> for VerifiedDataTransferIngestReport {
    fn from(value: VerifiedDataTransferIngestReportV1) -> Self {
        let ingest = value.report.as_ref().unwrap();
        let req = ingest.report.as_ref().unwrap();
        let event = req.data_transfer_usage.as_ref().unwrap();

        Self {
            hotspot_key: PublicKeyBinary::from(event.pub_key.clone()).to_string(),
            upload_bytes: event.upload_bytes,
            download_bytes: event.download_bytes,
            rewardable_bytes: req.rewardable_bytes,
            technology: event.radio_access_technology().as_str_name().to_string(),
            event_id: event.event_id.clone(),
            payer: PublicKeyBinary::from(event.payer.clone()).to_string(),
            timestamp: determine_timestamp(event.timestamp),
            received_timestamp: determine_timestamp(ingest.received_timestamp),
            verified_timestamp: determine_timestamp(value.timestamp),
            status: value.status().as_str_name().to_string(),
        }
    }
}
