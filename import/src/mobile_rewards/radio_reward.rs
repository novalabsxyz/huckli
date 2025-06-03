use chrono::{DateTime, Utc};
use futures::stream::iter;
use helium_proto::services::poc_mobile;
use import_derive::Import;

use crate::{PublicKeyBinary, from_proto_decimal};

pub struct Rewards {
    radio: RadioReward,
    trust_scores: Vec<LocationTrustScore>,
}

impl From<(DateTime<Utc>, DateTime<Utc>, poc_mobile::RadioRewardV2)> for Rewards {
    fn from(value: (DateTime<Utc>, DateTime<Utc>, poc_mobile::RadioRewardV2)) -> Self {
        let (start, end, reward) = value;
        let radio = RadioReward::from((start, end, &reward));
        let id = radio.id.clone();

        Self {
            radio,
            trust_scores: reward
                .location_trust_scores
                .iter()
                .map(|lts| LocationTrustScore::from((id.clone(), lts)))
                .collect(),
        }
    }
}

impl Rewards {
    pub async fn persist(db: &db::Db, rewards: Vec<Rewards>) -> anyhow::Result<()> {
        let mut radios = Vec::new();
        let mut trust_scores = Vec::new();

        for mut r in rewards {
            radios.push(r.radio);
            trust_scores.append(&mut r.trust_scores);
        }

        RadioReward::persist(db, iter(radios)).await?;
        LocationTrustScore::persist(db, iter(trust_scores)).await?;

        Ok(())
    }
}

#[derive(Debug, Import)]
#[import(table_name = "mobile_radio_rewards")]
pub struct RadioReward {
    id: String,
    #[import(sql = "timestamptz")]
    start_period: DateTime<Utc>,
    #[import(sql = "timestamptz")]
    end_period: DateTime<Utc>,
    hotspot_key: String,
    #[import(sql = "double")]
    base_coverage_points_sum: f64,
    #[import(sql = "double")]
    boosted_coverage_points_sum: f64,
}

impl From<(DateTime<Utc>, DateTime<Utc>, &poc_mobile::RadioRewardV2)> for RadioReward {
    fn from(value: (DateTime<Utc>, DateTime<Utc>, &poc_mobile::RadioRewardV2)) -> Self {
        let (start, end, reward) = value;
        let id = uuid::Uuid::new_v4().to_string();

        Self {
            id,
            start_period: start,
            end_period: end,
            hotspot_key: PublicKeyBinary::from(reward.hotspot_key.clone()).to_string(),
            base_coverage_points_sum: from_proto_decimal(reward.base_coverage_points_sum.as_ref()),
            boosted_coverage_points_sum: from_proto_decimal(
                reward.boosted_coverage_points_sum.as_ref(),
            ),
        }
    }
}

#[derive(Debug, Import)]
pub struct LocationTrustScore {
    id: String,
    #[import(sql = "uint64")]
    meters_to_asserted: u64,
    #[import(sql = "double")]
    trust_score: f64,
}

impl From<(String, &poc_mobile::radio_reward_v2::LocationTrustScore)> for LocationTrustScore {
    fn from(value: (String, &poc_mobile::radio_reward_v2::LocationTrustScore)) -> Self {
        let (id, lts) = value;
        Self {
            id,
            meters_to_asserted: lts.meters_to_asserted,
            trust_score: from_proto_decimal(lts.trust_score.as_ref()),
        }
    }
}
