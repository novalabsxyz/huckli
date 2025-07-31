use crate::{PublicKeyBinary, determine_timestamp};
use chrono::{DateTime, Utc};
use helium_proto::services::poc_mobile::VerifiedUniqueConnectionsIngestReportV1;
use huckli_import_derive::Import;

#[derive(Debug, Import)]
#[import(s3decode(
    proto = "VerifiedUniqueConnectionsIngestReportV1",
    bucket = "helium-mainnet-mobile-verified",
    prefix = "verified_unique_connections_report",
))]
pub struct VerifiedUniqueConnections {
    hotspot_key: String,
    #[import(sql = "timestamptz")]
    start_timestamp: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    end_timestamp: DateTime<Utc>,
    #[import(sql = "uint64")]
    unique_connections: u64,
    #[import(sql = "timestamptz")]
    sent_timestamp: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    received_timestamp: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    verified_timestamp: DateTime<Utc>,
    status: String,
}

impl From<VerifiedUniqueConnectionsIngestReportV1> for VerifiedUniqueConnections {
    fn from(value: VerifiedUniqueConnectionsIngestReportV1) -> Self {
        let ingest = value.report.as_ref().unwrap();
        let req = ingest.report.as_ref().unwrap();

        Self {
            hotspot_key: PublicKeyBinary::from(req.pubkey.clone()).to_string(),
            start_timestamp: determine_timestamp(req.start_timestamp),
            end_timestamp: determine_timestamp(req.end_timestamp),
            unique_connections: req.unique_connections,
            sent_timestamp: determine_timestamp(req.timestamp),
            received_timestamp: determine_timestamp(ingest.received_timestamp),
            verified_timestamp: determine_timestamp(value.timestamp),
            status: value.status().as_str_name().to_string(),
        }
    }
}
