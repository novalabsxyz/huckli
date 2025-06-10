use chrono::{DateTime, Utc};
use helium_proto::services::poc_mobile;
use import_derive::Import;
use uuid::Uuid;

use crate::{PublicKeyBinary, determine_timestamp};

mod radio_reward;

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
                MobileReward::Gateway(GatewayReward::from((start, end, g)))
            }
            Some(poc_mobile::mobile_reward_share::Reward::SubscriberReward(s)) => {
                MobileReward::Subscriber(SubscriberReward::from((start, end, s)))
            }
            Some(poc_mobile::mobile_reward_share::Reward::ServiceProviderReward(s)) => {
                MobileReward::ServiceProvider(ServiceProviderReward::from((start, end, s)))
            }
            Some(poc_mobile::mobile_reward_share::Reward::UnallocatedReward(u)) => {
                MobileReward::Unallocated(UnallocatedReward::from((start, end, u)))
            }
            Some(poc_mobile::mobile_reward_share::Reward::PromotionReward(p)) => {
                MobileReward::Promotion(PromotionReward::from((start, end, p)))
            }
            Some(poc_mobile::mobile_reward_share::Reward::RadioRewardV2(r)) => {
                MobileReward::Radio(radio_reward::Rewards::from((start, end, r)))
            }
            _ => MobileReward::Deprecated,
        }
    }
}

impl crate::DbTable for MobileReward {
    fn name() -> &'static str {
        ""
    }

    fn fields() -> Vec<db::TableField> {
        vec![]
    }

    fn create_table(db: &db::Db) -> anyhow::Result<()> {
        GatewayReward::create_table(db)?;
        SubscriberReward::create_table(db)?;
        ServiceProviderReward::create_table(db)?;
        UnallocatedReward::create_table(db)?;
        PromotionReward::create_table(db)?;

        radio_reward::Rewards::create_tables(db)?;

        Ok(())
    }

    fn save(db: &db::Db, data: Vec<Self>) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        for mobile_reward in data {
            let mut gateway_rewards = Vec::new();
            let mut subscriber_rewards = Vec::new();
            let mut provider_rewards = Vec::new();
            let mut unallocated_rewards = Vec::new();
            let mut promotions = Vec::new();
            let mut radios = Vec::new();

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

            GatewayReward::save(db, gateway_rewards)?;
            SubscriberReward::save(db, subscriber_rewards)?;
            ServiceProviderReward::save(db, provider_rewards)?;
            UnallocatedReward::save(db, unallocated_rewards)?;
            PromotionReward::save(db, promotions)?;

            radio_reward::Rewards::save(db, radios)?;
        }

        Ok(())
    }
}

impl MobileReward {
    pub async fn get_and_persist(
        db: &db::Db,
        s3: &s3::S3,
        time: &crate::TimeArgs,
    ) -> anyhow::Result<()> {
        crate::get_and_persist::<poc_mobile::MobileRewardShare, MobileReward>(
            db,
            s3,
            "helium-mainnet-mobile-verified",
            "mobile_network_reward_shares_v1",
            time,
        )
        .await
    }
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

impl From<(DateTime<Utc>, DateTime<Utc>, poc_mobile::PromotionReward)> for PromotionReward {
    fn from(value: (DateTime<Utc>, DateTime<Utc>, poc_mobile::PromotionReward)) -> Self {
        let (start, end, reward) = value;

        Self {
            start_period: start,
            end_period: end,
            entity: reward.entity,
            service_provider_amount: reward.service_provider_amount,
            matched_amount: reward.matched_amount,
        }
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

impl From<(DateTime<Utc>, DateTime<Utc>, poc_mobile::UnallocatedReward)> for UnallocatedReward {
    fn from(value: (DateTime<Utc>, DateTime<Utc>, poc_mobile::UnallocatedReward)) -> Self {
        let (start, end, reward) = value;
        Self {
            start_period: start,
            end_period: end,
            reward_type: reward.reward_type().as_str_name().to_string(),
            amount: reward.amount,
        }
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

impl
    From<(
        DateTime<Utc>,
        DateTime<Utc>,
        poc_mobile::ServiceProviderReward,
    )> for ServiceProviderReward
{
    fn from(
        value: (
            DateTime<Utc>,
            DateTime<Utc>,
            poc_mobile::ServiceProviderReward,
        ),
    ) -> Self {
        let (start, end, reward) = value;
        Self {
            start_period: start,
            end_period: end,
            service_provider: reward.service_provider_id().as_str_name().to_string(),
            amount: reward.amount,
        }
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
}

impl From<(DateTime<Utc>, DateTime<Utc>, poc_mobile::SubscriberReward)> for SubscriberReward {
    fn from(value: (DateTime<Utc>, DateTime<Utc>, poc_mobile::SubscriberReward)) -> Self {
        let (start, end, reward) = value;

        Self {
            start_period: start,
            end_period: end,
            subscriber_id: Uuid::from_slice(&reward.subscriber_id).unwrap().to_string(),
            discovery_location_amount: reward.discovery_location_amount,
            verification_mapping_amount: reward.verification_mapping_amount,
        }
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

impl From<(DateTime<Utc>, DateTime<Utc>, poc_mobile::GatewayReward)> for GatewayReward {
    fn from(value: (DateTime<Utc>, DateTime<Utc>, poc_mobile::GatewayReward)) -> Self {
        let (start, end, reward) = value;

        Self {
            start_period: start,
            end_period: end,
            hotspot_key: PublicKeyBinary::from(reward.hotspot_key).to_string(),
            dc_transfer_reward: reward.dc_transfer_reward,
            rewardable_bytes: reward.rewardable_bytes,
            price: reward.price,
        }
    }
}
