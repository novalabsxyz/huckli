use chrono::{DateTime, Utc};
use helium_proto::BoostedHexUpdateV1;
use huckli_import_derive::Import;

use crate::determine_timestamp;

#[derive(Debug, Import)]
#[import(s3decode(
    proto = BoostedHexUpdateV1,
    bucket = "helium-mainnet-mobile-verified",
    prefix = "boosted_hex_update",
))]
pub struct BoostedHexUpdate {
    #[import(sql = "timestamptz")]
    timestamp: DateTime<Utc>,
    #[import(sql = "uint64")]
    location: u64,
    #[import(sql = "timestamptz")]
    start_ts: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    end_ts: DateTime<Utc>,
    #[import(sql = "uint32")]
    period_length: u32,
    #[import(sql = "uint32")]
    multiplier: u32,
    #[import(sql = "uint32")]
    version: u32,
}

impl From<BoostedHexUpdateV1> for BoostedHexUpdate {
    fn from(value: BoostedHexUpdateV1) -> Self {
        let update = value.update.as_ref().unwrap();
        Self {
            timestamp: determine_timestamp(value.timestamp),
            location: update.location,
            start_ts: determine_timestamp(update.start_ts),
            end_ts: determine_timestamp(update.end_ts),
            period_length: update.period_length,
            multiplier: update.multipliers.get(0).unwrap_or(&0).to_owned(),
            version: update.version,
        }
    }
}
