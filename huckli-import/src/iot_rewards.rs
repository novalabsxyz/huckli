use chrono::{DateTime, Utc};
use helium_proto::services::poc_lora;
use huckli_import_derive::Import;

use crate::{PublicKeyBinary, determine_timestamp};

#[derive(Debug)]
pub enum IotReward {
    Gateway(IotGatewayReward),
    Operational(IotOperationalReward),
    Unallocated(IotUnallocatedReward),
    Deprecated,
}

impl From<poc_lora::IotRewardShare> for IotReward {
    fn from(value: poc_lora::IotRewardShare) -> Self {
        let start = determine_timestamp(value.start_period);
        let end = determine_timestamp(value.end_period);

        match value.reward {
            Some(poc_lora::iot_reward_share::Reward::GatewayReward(g)) => {
                g.to_iot_reward(start, end)
            }
            Some(poc_lora::iot_reward_share::Reward::OperationalReward(o)) => {
                o.to_iot_reward(start, end)
            }
            Some(poc_lora::iot_reward_share::Reward::UnallocatedReward(u)) => {
                u.to_iot_reward(start, end)
            }
            _ => IotReward::Deprecated,
        }
    }
}

impl crate::DbTable for IotReward {
    fn create_table(db: &huckli_db::Db) -> anyhow::Result<()> {
        IotGatewayReward::create_table(db)?;
        IotOperationalReward::create_table(db)?;
        IotUnallocatedReward::create_table(db)?;
        Ok(())
    }

    fn save(db: &huckli_db::Db, data: Vec<Self>) -> anyhow::Result<()> {
        let mut gateway_rewards = Vec::new();
        let mut operational_rewards = Vec::new();
        let mut unallocated_rewards = Vec::new();

        for iot_reward in data {
            match iot_reward {
                IotReward::Gateway(gateway) => {
                    gateway_rewards.push(gateway);
                }
                IotReward::Operational(operational) => {
                    operational_rewards.push(operational);
                }
                IotReward::Unallocated(unallocated) => {
                    unallocated_rewards.push(unallocated);
                }
                _ => (),
            }
        }

        IotGatewayReward::save(db, gateway_rewards)?;
        IotOperationalReward::save(db, operational_rewards)?;
        IotUnallocatedReward::save(db, unallocated_rewards)?;

        Ok(())
    }
}

impl IotReward {
    pub async fn get_and_persist(
        db: &huckli_db::Db,
        s3: &huckli_s3::S3,
        time: &crate::TimeArgs,
    ) -> anyhow::Result<()> {
        crate::get_and_persist::<poc_lora::IotRewardShare, IotReward>(
            db,
            s3,
            "helium-mainnet-iot-verified-rewards",
            "iot_network_reward_shares_v1",
            time,
        )
        .await
    }
}

trait ToIotReward {
    fn to_iot_reward(self, start: DateTime<Utc>, end: DateTime<Utc>) -> IotReward;
}

#[derive(Debug, Import)]
#[import(table_name = "iot_gateway_rewards")]
pub struct IotGatewayReward {
    #[import(sql = "timestamptz")]
    start_period: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    end_period: DateTime<Utc>,
    hotspot_key: String,
    #[import(sql = "uint64")]
    beacon_amount: u64,
    #[import(sql = "uint64")]
    witness_amount: u64,
    #[import(sql = "uint64")]
    dc_transfer_amount: u64,
}

impl ToIotReward for poc_lora::GatewayReward {
    fn to_iot_reward(self, start: DateTime<Utc>, end: DateTime<Utc>) -> IotReward {
        IotReward::Gateway(IotGatewayReward {
            start_period: start,
            end_period: end,
            hotspot_key: PublicKeyBinary::from(self.hotspot_key).to_string(),
            beacon_amount: self.beacon_amount,
            witness_amount: self.witness_amount,
            dc_transfer_amount: self.dc_transfer_amount,
        })
    }
}

#[derive(Debug, Import)]
#[import(table_name = "iot_operational_rewards")]
pub struct IotOperationalReward {
    #[import(sql = "timestamptz")]
    start_period: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    end_period: DateTime<Utc>,
    #[import(sql = "uint64")]
    amount: u64,
}

impl ToIotReward for poc_lora::OperationalReward {
    fn to_iot_reward(self, start: DateTime<Utc>, end: DateTime<Utc>) -> IotReward {
        IotReward::Operational(IotOperationalReward {
            start_period: start,
            end_period: end,
            amount: self.amount,
        })
    }
}

#[derive(Debug, Import)]
#[import(table_name = "iot_unallocated_rewards")]
pub struct IotUnallocatedReward {
    #[import(sql = "timestamptz")]
    start_period: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    end_period: DateTime<Utc>,
    reward_type: String,
    #[import(sql = "uint64")]
    amount: u64,
}

impl ToIotReward for poc_lora::UnallocatedReward {
    fn to_iot_reward(self, start: DateTime<Utc>, end: DateTime<Utc>) -> IotReward {
        IotReward::Unallocated(IotUnallocatedReward {
            start_period: start,
            end_period: end,
            reward_type: self.reward_type().as_str_name().to_string(),
            amount: self.amount,
        })
    }
}
