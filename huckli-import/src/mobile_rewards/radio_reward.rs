use chrono::{DateTime, Utc};
use helium_proto::services::poc_mobile;
use huckli_import_derive::Import;
use uuid::Uuid;

use crate::{DbTable, PublicKeyBinary, determine_timestamp, from_proto_decimal};

#[derive(Debug)]
pub struct Rewards {
    radio: RadioReward,
    trust_scores: Vec<LocationTrustScore>,
    speedtests: Vec<Speedtest>,
    covered_hexes: Vec<CoveredHex>,
}

impl super::ToMobileReward for poc_mobile::RadioRewardV2 {
    fn to_mobile_reward(self, start: DateTime<Utc>, end: DateTime<Utc>) -> super::MobileReward {
        let radio = RadioReward::from((start, end, &self));
        let id = radio.id.clone();

        super::MobileReward::Radio(Rewards {
            radio,
            trust_scores: self
                .location_trust_scores
                .iter()
                .map(|lts| LocationTrustScore::from((id.clone(), lts)))
                .collect(),
            speedtests: self
                .speedtests
                .iter()
                .map(|st| Speedtest::from((id.clone(), st)))
                .collect(),
            covered_hexes: self
                .covered_hexes
                .iter()
                .map(|ch| CoveredHex::from((id.clone(), ch)))
                .collect(),
        })
    }
}

impl Rewards {
    pub async fn create_tables(db: &huckli_db::Db) -> Result<(), huckli_db::DbError> {
        RadioReward::create_table(db).await?;
        LocationTrustScore::create_table(db).await?;
        Speedtest::create_table(db).await?;
        CoveredHex::create_table(db).await?;

        Ok(())
    }

    pub async fn save(db: &huckli_db::Db, rewards: Vec<Rewards>) -> Result<(), huckli_db::DbError> {
        let mut radios = Vec::new();
        let mut trust_scores = Vec::new();
        let mut speedtests = Vec::new();
        let mut hexes = Vec::new();

        for mut r in rewards {
            radios.push(r.radio);
            trust_scores.append(&mut r.trust_scores);
            speedtests.append(&mut r.speedtests);
            hexes.append(&mut r.covered_hexes);
        }

        RadioReward::save(db, radios).await?;
        LocationTrustScore::save(db, trust_scores).await?;
        Speedtest::save(db, speedtests).await?;
        CoveredHex::save(db, hexes).await?;

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
    #[import(sql = "double")]
    base_reward_shares: f64,
    #[import(sql = "double")]
    boosted_reward_shares: f64,
    #[import(sql = "uint64")]
    base_poc_reward: u64,
    #[import(sql = "uint64")]
    boosted_poc_reward: u64,
    #[import(sql = "timestamptz")]
    seniority_timestamp: DateTime<Utc>,
    coverage_object: String,
    #[import(sql = "double")]
    location_trust_score_multiplier: f64,
    #[import(sql = "double")]
    speedtest_multiplier: f64,
    sp_boosted_hex_status: String,
    oracle_boosted_hex_status: String,
    #[import(sql = "uint64")]
    speedtest_avg_upload: u64,
    #[import(sql = "uint64")]
    speedtest_avg_download: u64,
    #[import(sql = "uint32")]
    speedtest_avg_latency_ms: u32,
    #[import(sql = "timestamptz")]
    speedtest_avg_timestamp: DateTime<Utc>,
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
            base_reward_shares: from_proto_decimal(reward.base_reward_shares.as_ref()),
            boosted_reward_shares: from_proto_decimal(reward.boosted_reward_shares.as_ref()),
            base_poc_reward: reward.base_poc_reward,
            boosted_poc_reward: reward.boosted_poc_reward,
            seniority_timestamp: determine_timestamp(reward.seniority_timestamp),
            coverage_object: Uuid::from_slice(&reward.coverage_object)
                .unwrap()
                .to_string(),
            location_trust_score_multiplier: from_proto_decimal(
                reward.location_trust_score_multiplier.as_ref(),
            ),
            speedtest_multiplier: from_proto_decimal(reward.speedtest_multiplier.as_ref()),
            sp_boosted_hex_status: reward.sp_boosted_hex_status().as_str_name().to_string(),
            oracle_boosted_hex_status: reward.oracle_boosted_hex_status().as_str_name().to_string(),
            speedtest_avg_upload: reward.speedtest_average.as_ref().unwrap().upload_speed_bps,
            speedtest_avg_download: reward
                .speedtest_average
                .as_ref()
                .unwrap()
                .download_speed_bps,
            speedtest_avg_latency_ms: reward.speedtest_average.as_ref().unwrap().latency_ms,
            speedtest_avg_timestamp: determine_timestamp(
                reward.speedtest_average.as_ref().unwrap().timestamp,
            ),
        }
    }
}

#[derive(Debug, Import)]
#[import(table_name = "mobile_reward_covered_hexes")]
pub struct CoveredHex {
    id: String,
    #[import(sql = "uint64")]
    location: u64,
    #[import(sql = "double")]
    base_coverage_points: f64,
    #[import(sql = "double")]
    boosted_coverage_points: f64,
    urbanized: String,
    footfall: String,
    landtype: String,
    #[import(sql = "double")]
    assignment_multiplier: f64,
    #[import(sql = "uint32")]
    rank: u32,
    #[import(sql = "double")]
    rank_multiplier: f64,
    #[import(sql = "uint32")]
    boosted_multiplier: u32,
    #[import(sql = "bool")]
    service_provider_override: bool,
}

impl From<(String, &poc_mobile::radio_reward_v2::CoveredHex)> for CoveredHex {
    fn from(value: (String, &poc_mobile::radio_reward_v2::CoveredHex)) -> Self {
        let (id, hex) = value;
        Self {
            id,
            location: hex.location,
            base_coverage_points: from_proto_decimal(hex.base_coverage_points.as_ref()),
            boosted_coverage_points: from_proto_decimal(hex.boosted_coverage_points.as_ref()),
            urbanized: hex.urbanized().as_str_name().to_string(),
            footfall: hex.footfall().as_str_name().to_string(),
            landtype: hex.landtype().as_str_name().to_string(),
            assignment_multiplier: from_proto_decimal(hex.assignment_multiplier.as_ref()),
            rank: hex.rank,
            rank_multiplier: from_proto_decimal(hex.rank_multiplier.as_ref()),
            boosted_multiplier: hex.boosted_multiplier,
            service_provider_override: hex.service_provider_override,
        }
    }
}

#[derive(Debug, Import)]
#[import(table_name = "mobile_reward_speedtests")]
pub struct Speedtest {
    id: String,
    #[import(sql = "uint64")]
    upload: u64,
    #[import(sql = "uint64")]
    download: u64,
    #[import(sql = "uint32")]
    latency_ms: u32,
    #[import(sql = "timestamptz")]
    timestamp: DateTime<Utc>,
}

impl From<(String, &poc_mobile::Speedtest)> for Speedtest {
    fn from(value: (String, &poc_mobile::Speedtest)) -> Self {
        let (id, st) = value;
        Self {
            id,
            upload: st.upload_speed_bps,
            download: st.download_speed_bps,
            latency_ms: st.latency_ms,
            timestamp: determine_timestamp(st.timestamp),
        }
    }
}

#[derive(Debug, Import)]
#[import(table_name = "mobile_reward_trust_scores")]
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
