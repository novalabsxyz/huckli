use chrono::{DateTime, Utc};
use helium_proto::services::chain_rewardable_entities::EntityOwnershipChangeReportV1;
use huckli_import_derive::Import;

use crate::{PublicKeyBinary, determine_timestamp};

#[derive(Debug, Import)]
#[import(s3decode(
    proto = EntityOwnershipChangeReportV1,
    bucket = "helium-mainnet-chain-ingest",
    prefix = "entity_ownership_change_report",
))]
pub struct EntityOwnershipChange {
    #[import(sql = "timestamptz")]
    received_timestamp: DateTime<Utc>,
    #[import(sql = "uint64")]
    block: u64,
    #[import(sql = "timestamptz")]
    timestamp: DateTime<Utc>,
    entity_pub_key: String,
    asset: String,
    owner: String,
    owner_type: String,
    signer: String,
}

impl From<EntityOwnershipChangeReportV1> for EntityOwnershipChange {
    fn from(value: EntityOwnershipChangeReportV1) -> Self {
        let req = value.report.unwrap_or_default();
        let change = req.change.unwrap_or_default();
        let owner = change.owner.unwrap_or_default();

        Self {
            received_timestamp: determine_timestamp(value.received_timestamp_ms),
            block: change.block,
            timestamp: determine_timestamp(change.timestamp_seconds),
            entity_pub_key: change
                .entity_pub_key
                .map(|k| PublicKeyBinary::from(k.value).to_string())
                .unwrap_or_default(),
            asset: change
                .asset
                .map(|a| bs58::encode(a.value).into_string())
                .unwrap_or_default(),
            owner_type: owner.r#type().as_str_name().to_string(),
            owner: owner
                .wallet
                .map(|w| bs58::encode(w.value).into_string())
                .unwrap_or_default(),
            signer: req.signer,
        }
    }
}
