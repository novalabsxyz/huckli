use chrono::{DateTime, Utc};
use helium_proto::{RewardManifest, services::poc_mobile};
use huckli_s3::FileInfo;
use huckli_import_derive::Import;
use uuid::Uuid;

use crate::{PublicKeyBinary, determine_timestamp};

mod radio_reward;

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum MobileReward {
    Gateway(GatewayReward),
    Subscriber(SubscriberReward),
    ServiceProvider(ServiceProviderReward),
    Unallocated(UnallocatedReward),
    Promotion(PromotionReward),
    Radio(radio_reward::Rewards),
    Deprecated,
}

impl From<poc_mobile::MobileRewardShare> for MobileReward {
    fn from(value: poc_mobile::MobileRewardShare) -> Self {
        let start = determine_timestamp(value.start_period);
        let end = determine_timestamp(value.end_period);

        match value.reward {
            Some(poc_mobile::mobile_reward_share::Reward::GatewayReward(g)) => {
                g.to_mobile_reward(start, end)
            }
            Some(poc_mobile::mobile_reward_share::Reward::SubscriberReward(s)) => {
                s.to_mobile_reward(start, end)
            }
            Some(poc_mobile::mobile_reward_share::Reward::ServiceProviderReward(s)) => {
                s.to_mobile_reward(start, end)
            }
            Some(poc_mobile::mobile_reward_share::Reward::UnallocatedReward(u)) => {
                u.to_mobile_reward(start, end)
            }
            Some(poc_mobile::mobile_reward_share::Reward::PromotionReward(p)) => {
                p.to_mobile_reward(start, end)
            }
            Some(poc_mobile::mobile_reward_share::Reward::RadioRewardV2(r)) => {
                r.to_mobile_reward(start, end)
            }
            _ => MobileReward::Deprecated,
        }
    }
}

impl crate::DbTable for MobileReward {
    type Item = Self;
    fn create_table(db: &huckli_db::Db) -> Result<(), huckli_db::DbError> {
        GatewayReward::create_table(db)?;
        SubscriberReward::create_table(db)?;
        ServiceProviderReward::create_table(db)?;
        UnallocatedReward::create_table(db)?;
        PromotionReward::create_table(db)?;

        radio_reward::Rewards::create_tables(db)?;

        Ok(())
    }

    fn save(db: &huckli_db::Db, data: Vec<Self>) -> Result<(), huckli_db::DbError> {
        let mut gateway_rewards = Vec::new();
        let mut subscriber_rewards = Vec::new();
        let mut provider_rewards = Vec::new();
        let mut unallocated_rewards = Vec::new();
        let mut promotions = Vec::new();
        let mut radios = Vec::new();

        for mobile_reward in data {
            match mobile_reward {
                MobileReward::Gateway(gateway) => {
                    gateway_rewards.push(gateway);
                }
                MobileReward::Subscriber(subscriber) => {
                    subscriber_rewards.push(subscriber);
                }
                MobileReward::ServiceProvider(sp) => {
                    provider_rewards.push(sp);
                }
                MobileReward::Unallocated(u) => {
                    unallocated_rewards.push(u);
                }
                MobileReward::Promotion(p) => {
                    promotions.push(p);
                }
                MobileReward::Radio(r) => {
                    radios.push(r);
                }
                _ => (),
            }
        }

        GatewayReward::save(db, gateway_rewards)?;
        SubscriberReward::save(db, subscriber_rewards)?;
        ServiceProviderReward::save(db, provider_rewards)?;
        UnallocatedReward::save(db, unallocated_rewards)?;
        PromotionReward::save(db, promotions)?;

        radio_reward::Rewards::save(db, radios)?;

        Ok(())
    }
}

const BUCKET: &str = "helium-mainnet-mobile-verified";
const PREFIX: &str = "mobile_network_reward_shares_v1";

impl MobileReward {
    pub async fn get_and_persist(
        db: &huckli_db::Db,
        s3: &huckli_s3::S3,
        time: &crate::TimeArgs,
    ) -> Result<(), crate::ImportError> {
        crate::get_and_persist::<poc_mobile::MobileRewardShare, MobileReward>(
            db,
            s3,
            BUCKET,
            PREFIX,
            time,
        )
        .await
    }

    pub async fn get_and_persist_from_files(
        db: &huckli_db::Db,
        s3: &huckli_s3::S3,
        file_infos: &[FileInfo],
    ) -> Result<(), crate::ImportError> {
        crate::get_and_persist_files::<poc_mobile::MobileRewardShare, MobileReward>(
            db,
            s3,
            BUCKET,
            file_infos,
        )
        .await
    }
}

trait ToMobileReward {
    fn to_mobile_reward(self, start: DateTime<Utc>, end: DateTime<Utc>) -> MobileReward;
}

#[derive(Debug, Import)]
#[import(table_name = "mobile_promotion_rewards")]
pub struct PromotionReward {
    #[import(sql = "timestamptz")]
    start_period: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    end_period: DateTime<Utc>,
    entity: String,
    #[import(sql = "uint64")]
    service_provider_amount: u64,
    #[import(sql = "uint64")]
    matched_amount: u64,
}

impl ToMobileReward for poc_mobile::PromotionReward {
    fn to_mobile_reward(self, start: DateTime<Utc>, end: DateTime<Utc>) -> MobileReward {
        MobileReward::Promotion(PromotionReward {
            start_period: start,
            end_period: end,
            entity: self.entity,
            service_provider_amount: self.service_provider_amount,
            matched_amount: self.matched_amount,
        })
    }
}

#[derive(Debug, Import)]
#[import(table_name = "mobile_unallocated_rewards")]
pub struct UnallocatedReward {
    #[import(sql = "timestamptz")]
    start_period: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    end_period: DateTime<Utc>,
    reward_type: String,
    #[import(sql = "uint64")]
    amount: u64,
}

impl ToMobileReward for poc_mobile::UnallocatedReward {
    fn to_mobile_reward(self, start: DateTime<Utc>, end: DateTime<Utc>) -> MobileReward {
        MobileReward::Unallocated(UnallocatedReward {
            start_period: start,
            end_period: end,
            reward_type: self.reward_type().as_str_name().to_string(),
            amount: self.amount,
        })
    }
}

#[derive(Debug, Import)]
#[import(table_name = "mobile_service_provider_rewards")]
pub struct ServiceProviderReward {
    #[import(sql = "timestamptz")]
    start_period: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    end_period: DateTime<Utc>,
    service_provider: String,
    #[import(sql = "uint64")]
    amount: u64,
}

impl ToMobileReward for poc_mobile::ServiceProviderReward {
    fn to_mobile_reward(self, start: DateTime<Utc>, end: DateTime<Utc>) -> MobileReward {
        MobileReward::ServiceProvider(ServiceProviderReward {
            start_period: start,
            end_period: end,
            service_provider: self.service_provider_id().as_str_name().to_string(),
            amount: self.amount,
        })
    }
}

#[derive(Debug, Import)]
#[import(table_name = "mobile_subscriber_rewards")]
pub struct SubscriberReward {
    #[import(sql = "timestamptz")]
    start_period: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    end_period: DateTime<Utc>,
    subscriber_id: String,
    #[import(sql = "uint64")]
    discovery_location_amount: u64,
    #[import(sql = "uint64")]
    verification_mapping_amount: u64,
    reward_override_entity_key: String,
}

impl ToMobileReward for poc_mobile::SubscriberReward {
    fn to_mobile_reward(self, start: DateTime<Utc>, end: DateTime<Utc>) -> MobileReward {
        MobileReward::Subscriber(SubscriberReward {
            start_period: start,
            end_period: end,
            subscriber_id: Uuid::from_slice(&self.subscriber_id).unwrap().to_string(),
            discovery_location_amount: self.discovery_location_amount,
            verification_mapping_amount: self.verification_mapping_amount,
            reward_override_entity_key: self.reward_override_entity_key,
        })
    }
}

#[derive(Debug, Import)]
#[import(table_name = "mobile_gateway_rewards")]
pub struct GatewayReward {
    #[import(sql = "timestamptz")]
    start_period: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    end_period: DateTime<Utc>,
    hotspot_key: String,
    #[import(sql = "bigint")]
    dc_transfer_reward: u64,
    #[import(sql = "bigint")]
    rewardable_bytes: u64,
    #[import(sql = "bigint")]
    price: u64,
}

impl ToMobileReward for poc_mobile::GatewayReward {
    fn to_mobile_reward(self, start: DateTime<Utc>, end: DateTime<Utc>) -> MobileReward {
        MobileReward::Gateway(GatewayReward {
            start_period: start,
            end_period: end,
            hotspot_key: PublicKeyBinary::from(self.hotspot_key).to_string(),
            dc_transfer_reward: self.dc_transfer_reward,
            rewardable_bytes: self.rewardable_bytes,
            price: self.price,
        })
    }
}


#[derive(Debug, Import)]
#[import(s3decode(
    proto = "RewardManifest",
    bucket = "helium-mainnet-mobile-verified",
    prefix = "network_reward_manifest_v1",
))]
pub struct MobileRewardManifest {
    #[import(sql = "timestamptz")]
    start_period: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    end_period: DateTime<Utc>,
    #[import(sql = "uint64")]
    epoch: u64,
    #[import(sql = "uint64")]
    price: u64,
    #[import(sql = "json")]
    written_files: serde_json::Value,
}

impl From<RewardManifest> for MobileRewardManifest {
    fn from(value: RewardManifest) -> Self {
        let written_files: Vec<serde_json::Value> = value
            .written_files
            .into_iter()
            .map(serde_json::Value::String)
            .collect();

        Self {
            start_period: determine_timestamp(value.start_timestamp),
            end_period: determine_timestamp(value.end_timestamp),
            epoch: value.epoch,
            price: value.price,
            written_files: serde_json::Value::Array(written_files),
        }
    }
}
