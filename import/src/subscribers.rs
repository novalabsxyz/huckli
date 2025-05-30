use chrono::{DateTime, Utc};
use helium_proto::services::poc_mobile::{
    SubscriberMappingActivityIngestReportV1, VerifiedSubscriberMappingActivityReportV1,
};
use import_derive::Import;
use uuid::Uuid;

use crate::determine_timestamp;

#[derive(Debug, Import)]
#[import(
    proto = SubscriberMappingActivityIngestReportV1,
    bucket = "helium-mainnet-mobile-ingest",
    prefix = "subscriber_mapping_activity_ingest_report",
)]
pub struct SubscriberMappingActivityIngest {
    subscriber_id: String,
    #[import(sql = "bigint")]
    discovery_reward_shares: u64,
    #[import(sql = "bigint")]
    verification_reward_shares: u64,
    #[import(sql = "timestamptz")]
    timestamp: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    received_timestamp: DateTime<Utc>,
}

impl From<SubscriberMappingActivityIngestReportV1> for SubscriberMappingActivityIngest {
    fn from(value: SubscriberMappingActivityIngestReportV1) -> Self {
        let report = value.report.unwrap().clone();
        Self {
            subscriber_id: Uuid::from_slice(&report.subscriber_id).unwrap().to_string(),
            discovery_reward_shares: report.discovery_reward_shares,
            verification_reward_shares: report.verification_reward_shares,
            timestamp: determine_timestamp(report.timestamp),
            received_timestamp: determine_timestamp(value.received_timestamp),
        }
    }
}

#[derive(Debug, Import)]
#[import(
    proto = VerifiedSubscriberMappingActivityReportV1,
    bucket = "helium-mainnet-mobile-verified",
    prefix = "verified_subscriber_mapping_activity_report",
)]
pub struct VerifiedSubscriberMappingActivity {
    subscriber_id: String,
    #[import(sql = "bigint")]
    discovery_reward_shares: u64,
    #[import(sql = "bigint")]
    verification_reward_shares: u64,
    #[import(sql = "timestamptz")]
    timestamp: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    received_timestamp: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    verification_timestamp: DateTime<Utc>,
}

impl From<VerifiedSubscriberMappingActivityReportV1> for VerifiedSubscriberMappingActivity {
    fn from(value: VerifiedSubscriberMappingActivityReportV1) -> Self {
        let report = value
            .report
            .as_ref()
            .unwrap()
            .report
            .as_ref()
            .unwrap()
            .clone();

        let ingest = value.report.as_ref().unwrap().clone();

        Self {
            subscriber_id: Uuid::from_slice(&report.subscriber_id).unwrap().to_string(),
            discovery_reward_shares: report.discovery_reward_shares,
            verification_reward_shares: report.verification_reward_shares,
            timestamp: determine_timestamp(report.timestamp),
            received_timestamp: determine_timestamp(ingest.received_timestamp),
            verification_timestamp: determine_timestamp(value.timestamp),
        }
    }
}
