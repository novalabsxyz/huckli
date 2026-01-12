pub mod boosting;
pub mod coverage;
pub mod data_transfer;
pub mod enabled_carriers_info;
pub mod heartbeats;
pub mod iot_rewards;
pub mod mobile_rewards;
pub mod radio_thresholds;
pub mod sp_banned_radio;
pub mod subscribers;
pub mod unique_connections;
pub mod usage;
pub mod usage_v2;
pub mod verified_speedtest;

use std::{cell::RefCell, str::FromStr};

use anyhow::Context;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use futures::{StreamExt, TryStreamExt};
use rust_decimal::Decimal;

thread_local! {
    static FILE_SOURCE: RefCell<Option<String>> = RefCell::new(None);
}

pub fn set_file_source(key: &str) {
    FILE_SOURCE.with(|fs| *fs.borrow_mut() = Some(key.to_string()));
}

pub fn get_file_source() -> Option<String> {
    FILE_SOURCE.with(|fs| fs.borrow().clone())
}

pub fn clear_file_source() {
    FILE_SOURCE.with(|fs| *fs.borrow_mut() = None);
}

pub async fn run(
    file_type: SupportedFileTypes,
    db: &huckli_db::Db,
    s3: &huckli_s3::S3,
    selection: &FileSelectionArgs,
) -> anyhow::Result<()> {
    match file_type {
        SupportedFileTypes::BoostedHexUpdate => {
            boosting::BoostedHexUpdate::get_and_persist(db, s3, selection).await?;
        }
        SupportedFileTypes::CoverageObject => {
            coverage::CoverageObjectProto::get_and_persist(db, s3, selection).await?;
        }
        SupportedFileTypes::DataTransferBurn => {
            data_transfer::DataTransferBurn::get_and_persist(db, s3, selection).await?;
        }
        SupportedFileTypes::DataTransferIngest => {
            data_transfer::DataTransferIngestReport::get_and_persist(db, s3, selection).await?;
        }
        SupportedFileTypes::IotRewards => {
            iot_rewards::IotReward::get_and_persist(db, s3, selection).await?;
        }
        SupportedFileTypes::MobileRewards => {
            mobile_rewards::MobileReward::get_and_persist(db, s3, selection).await?;
        }
        SupportedFileTypes::MobileRewardManifest => {
            mobile_rewards::MobileRewardManifest::get_and_persist(db, s3, selection).await?;
        }
        SupportedFileTypes::RadioUsageStats => {
            usage::RadioUsageStats::get_and_persist(db, s3, selection).await?;
        }
        SupportedFileTypes::RadioUsageStatsV2 => {
            usage_v2::RadioUsageStatsV2::get_and_persist(db, s3, selection).await?;
        }
        SupportedFileTypes::SubscriberMappingActivityIngest => {
            subscribers::SubscriberMappingActivityIngest::get_and_persist(db, s3, selection)
                .await?;
        }
        SupportedFileTypes::ValidatedHeartbeat => {
            heartbeats::VerifiedWifiHeartbeat::get_and_persist(db, s3, selection).await?;
        }
        SupportedFileTypes::WifiHeartbeatIngest => {
            heartbeats::WifiHeartbeatIngestReport::get_and_persist(db, s3, selection).await?;
        }
        SupportedFileTypes::VerifiedCdrVerification => {
            sp_banned_radio::VerifiedCdrVerification::get_and_persist(db, s3, selection).await?;
        }
        SupportedFileTypes::VerifiedDataTransfer => {
            data_transfer::VerifiedDataTransferIngestReport::get_and_persist(db, s3, selection)
                .await?;
        }
        SupportedFileTypes::VerifiedInvalidatedRadioThreshold => {
            radio_thresholds::VerifiedInvalidatedRadioThreshold::get_and_persist(db, s3, selection)
                .await?;
        }
        SupportedFileTypes::VerifiedRadioThreshold => {
            radio_thresholds::VerifiedRadioThreshold::get_and_persist(db, s3, selection).await?;
        }
        SupportedFileTypes::VerifiedSubscriberMappingActivity => {
            subscribers::VerifiedSubscriberMappingActivity::get_and_persist(db, s3, selection)
                .await?;
        }
        SupportedFileTypes::VerifiedSpeedtest => {
            verified_speedtest::VerifiedSpeedtestReport::get_and_persist(db, s3, selection).await?;
        }
        SupportedFileTypes::VerifiedUniqueConnections => {
            unique_connections::VerifiedUniqueConnections::get_and_persist(db, s3, selection)
                .await?;
        }
        SupportedFileTypes::EnabledCarriersInfo => {
            enabled_carriers_info::EnabledCarriersInfo::get_and_persist(db, s3, selection).await?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum SupportedFileTypes {
    BoostedHexUpdate,
    CoverageObject,
    DataTransferBurn,
    DataTransferIngest,
    IotRewards,
    MobileRewards,
    MobileRewardManifest,
    RadioUsageStats,
    RadioUsageStatsV2,
    SubscriberMappingActivityIngest,
    ValidatedHeartbeat,
    VerifiedCdrVerification,
    VerifiedDataTransfer,
    WifiHeartbeatIngest,
    VerifiedInvalidatedRadioThreshold,
    VerifiedSubscriberMappingActivity,
    VerifiedRadioThreshold,
    VerifiedSpeedtest,
    VerifiedUniqueConnections,
    EnabledCarriersInfo,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PublicKeyBinary(Vec<u8>);

impl From<Vec<u8>> for PublicKeyBinary {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl std::fmt::Display for PublicKeyBinary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        // allocate one extra byte for the base58 version
        let mut data = vec![0u8; self.0.len() + 1];
        data[1..].copy_from_slice(&self.0);
        let encoded = bs58::encode(&data).with_check().into_string();
        f.write_str(&encoded)
    }
}

pub fn to_datetime(timestamp: u64) -> DateTime<Utc> {
    Utc.timestamp_opt(timestamp as i64, 0).single().unwrap()
}

pub fn to_datetime_ms(timestamp: u64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(timestamp as i64).single().unwrap()
}
pub fn determine_timestamp(timestamp: u64) -> DateTime<Utc> {
    const MILLISECOND_THRESHOLD: u64 = 1_000_000_000_000;

    if timestamp > MILLISECOND_THRESHOLD {
        // Assume milliseconds format
        to_datetime_ms(timestamp)
    } else {
        // Assume seconds format
        to_datetime(timestamp)
    }
}

pub trait DbTable: Sized {
    fn create_table(db: &huckli_db::Db) -> anyhow::Result<()>;

    fn save(db: &huckli_db::Db, data: Vec<Self>) -> anyhow::Result<()>;
}

pub async fn get_and_persist<F, T>(
    db: &huckli_db::Db,
    s3: &huckli_s3::S3,
    bucket: &str,
    prefix: &str,
    selection: &FileSelectionArgs,
) -> anyhow::Result<()>
where
    F: prost::Message + Default,
    T: From<F> + DbTable,
{
    T::create_table(db)?;

    let files = selection.get_files(s3, db, bucket, prefix).await?;

    let mut stream = futures::stream::iter(files)
        .map(|file| async { (file.clone(), get_and_decode::<F, T>(s3, bucket, file).await) })
        .buffered(10);

    while let Some((file, data)) = stream.next().await {
        tracing::info!(file = %file.key, timestamp = %file.timestamp, "processing");

        set_file_source(&file.key);
        T::save(db, data)?;
        clear_file_source();

        db.save_file_processed(&file.key, &file.prefix, file.timestamp)?;
    }

    Ok(())
}

pub async fn get_and_decode<F, T>(
    s3: &huckli_s3::S3,
    bucket: &str,
    file: huckli_s3::FileInfo,
) -> Vec<T>
where
    F: prost::Message + Default,
    T: From<F>,
{
    s3.stream_files(bucket, vec![file])
        .then(|b| async move { F::decode(b) })
        .and_then(|f| async move { Ok(T::from(f)) })
        .filter_map(|result| async move {
            match result {
                Ok(t) => Some(t),
                Err(e) => {
                    eprintln!("error in decoding record: {e}");
                    None
                }
            }
        })
        .collect()
        .await
}

#[derive(Debug, clap::Args)]
#[group(id = "file_selection", multiple = false)]
pub struct FileSelectionArgs {
    #[arg(long, group = "file_selection")]
    after: Option<NaiveDateTime>,
    #[arg(long)]
    before: Option<NaiveDateTime>,
    #[arg(long, default_value_t = false, group = "file_selection")]
    r#continue: bool,
    #[arg(long, group = "file_selection")]
    file: Option<String>,
}

impl FileSelectionArgs {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.r#continue && self.after.is_some() {
            anyhow::bail!("Invalid options, cannot specify both 'continue' and 'after'");
        }

        if self.file.is_some() && self.before.is_some() {
            anyhow::bail!("Invalid options, cannot specify 'before' with 'file'");
        }

        Ok(())
    }

    pub async fn get_files(
        &self,
        s3: &huckli_s3::S3,
        db: &huckli_db::Db,
        bucket: &str,
        prefix: &str,
    ) -> anyhow::Result<Vec<huckli_s3::FileInfo>> {
        if let Some(file_str) = &self.file {
            let file_info = huckli_s3::FileInfo::from_str(file_str)?;
            Ok(vec![file_info])
        } else {
            s3.list_all(
                bucket,
                prefix,
                self.after_utc(db, prefix)?,
                self.before_utc(),
            )
            .await
        }
    }

    pub fn after_utc(
        &self,
        db: &huckli_db::Db,
        prefix: &str,
    ) -> anyhow::Result<Option<DateTime<Utc>>> {
        if self.r#continue {
            let latest = db
                .latest_file_processed_timestamp(prefix)
                .context("Cannot conitnue, no previously processed files")?;

            Ok(Some(latest))
        } else {
            Ok(self.after.as_ref().map(NaiveDateTime::and_utc))
        }
    }

    pub fn before_utc(&self) -> Option<DateTime<Utc>> {
        self.before.as_ref().map(NaiveDateTime::and_utc)
    }
}

fn from_proto_decimal(opt: Option<&helium_proto::Decimal>) -> f64 {
    opt.ok_or_else(|| anyhow::anyhow!("decimal not present"))
        .and_then(|d| Decimal::from_str(&d.value).map_err(anyhow::Error::from))
        .unwrap_or_default()
        .try_into()
        .unwrap()
}
