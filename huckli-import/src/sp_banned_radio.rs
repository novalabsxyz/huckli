use chrono::{DateTime, Utc};
use helium_proto::services::poc_mobile::{
    VerifiedServiceProviderBoostedRewardsBannedRadioIngestReportV1,
    service_provider_boosted_rewards_banned_radio_req_v1::KeyType,
};
use huckli_import_derive::Import;

use crate::{PublicKeyBinary, determine_timestamp};

#[derive(Debug, Import)]
#[import(s3decode(
    proto = VerifiedServiceProviderBoostedRewardsBannedRadioIngestReportV1,
    bucket = "helium-mainnet-mobile-verified",
    prefix = "verified_service_provider_boosted_rewards_banned_radio",
))]
pub struct VerifiedCdrVerification {
    hotspot_key: String,
    reason: String,
    #[import(sql = "timestamptz")]
    until: DateTime<Utc>,
    ban_type: String,
    #[import(sql = "timestamptz")]
    received_timestamp: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    verified_timestamp: DateTime<Utc>,
    status: String,
}

impl From<VerifiedServiceProviderBoostedRewardsBannedRadioIngestReportV1>
    for VerifiedCdrVerification
{
    fn from(value: VerifiedServiceProviderBoostedRewardsBannedRadioIngestReportV1) -> Self {
        let ingest = value.report.as_ref().unwrap();
        let req = ingest.report.as_ref().unwrap();

        let hotspot_key = match req.key_type.as_ref() {
            Some(KeyType::HotspotKey(key)) => PublicKeyBinary::from(key.clone()).to_string(),
            Some(KeyType::CbsdId(cbsd_id)) => cbsd_id.to_string(),
            _ => panic!("unknown key type"),
        };

        Self {
            hotspot_key,
            reason: req.reason().as_str_name().to_string(),
            until: determine_timestamp(req.until),
            ban_type: req.ban_type().as_str_name().to_string(),
            received_timestamp: determine_timestamp(ingest.received_timestamp),
            verified_timestamp: determine_timestamp(value.timestamp),
            status: value.status().as_str_name().to_string(),
        }
    }
}
