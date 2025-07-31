pub mod coverage;
pub mod data_transfer;
pub mod heartbeats;
pub mod mobile_rewards;
pub mod radio_thresholds;
pub mod sp_banned_radio;
pub mod subscribers;
pub mod unique_connections;
pub mod usage;

pub use async_duckdb;
pub use huckli_db;
pub use huckli_import_derive;
pub use huckli_s3;

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use huckli_s3::FileInfo;
use rust_decimal::Decimal;
use std::str::FromStr;

#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    #[error("Database error: {0}")]
    Db(#[from] huckli_db::DbError),
    #[error("S3 error: {0}")]
    S3(#[from] huckli_s3::S3Error),
    #[error("Invalid options, cannot specify both 'continue' and 'after'")]
    TimeArgs,
    #[error("Decimal: {0}")]
    Decimal(#[from] rust_decimal::Error),
}

pub async fn run(
    file_type: SupportedFileTypes,
    db: &huckli_db::Db,
    s3: &huckli_s3::S3,
    time: &crate::TimeArgs,
) -> Result<(), ImportError> {
    match file_type {
        SupportedFileTypes::CoverageObject => {
            coverage::CoverageObjectProto::get_and_persist(db, s3, time).await?;
        }
        SupportedFileTypes::DataTransferBurn => {
            data_transfer::DataTransferBurn::get_and_persist(db, s3, time).await?;
        }
        SupportedFileTypes::DataTransferIngest => {
            data_transfer::DataTransferIngestReport::get_and_persist(db, s3, time).await?;
        }
        SupportedFileTypes::MobileRewards => {
            mobile_rewards::MobileReward::get_and_persist(db, s3, time).await?;
        }
        SupportedFileTypes::MobileRewardManifest => {
            mobile_rewards::MobileRewardManifest::get_and_persist(db, s3, time).await?;
        }
        SupportedFileTypes::RadioUsageStats => {
            usage::RadioUsageStats::get_and_persist(db, s3, time).await?;
        }
        SupportedFileTypes::SubscriberMappingActivityIngest => {
            subscribers::SubscriberMappingActivityIngest::get_and_persist(db, s3, time).await?;
        }
        SupportedFileTypes::ValidatedHeartbeat => {
            heartbeats::VerifiedWifiHeartbeat::get_and_persist(db, s3, time).await?;
        }
        SupportedFileTypes::VerifiedCdrVerification => {
            sp_banned_radio::VerifiedCdrVerification::get_and_persist(db, s3, time).await?;
        }
        SupportedFileTypes::VerifiedDataTransfer => {
            data_transfer::VerifiedDataTransferIngestReport::get_and_persist(db, s3, time).await?;
        }
        SupportedFileTypes::VerifiedInvalidatedRadioThreshold => {
            radio_thresholds::VerifiedInvalidatedRadioThreshold::get_and_persist(db, s3, time)
                .await?;
        }
        SupportedFileTypes::VerifiedRadioThreshold => {
            radio_thresholds::VerifiedRadioThreshold::get_and_persist(db, s3, time).await?;
        }
        SupportedFileTypes::VerifiedSubscriberMappingActivity => {
            subscribers::VerifiedSubscriberMappingActivity::get_and_persist(db, s3, time).await?;
        }
        SupportedFileTypes::VerifiedUniqueConnections => {
            unique_connections::VerifiedUniqueConnections::get_and_persist(db, s3, time).await?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum SupportedFileTypes {
    CoverageObject,
    DataTransferBurn,
    DataTransferIngest,
    MobileRewards,
    MobileRewardManifest,
    RadioUsageStats,
    SubscriberMappingActivityIngest,
    ValidatedHeartbeat,
    VerifiedCdrVerification,
    VerifiedDataTransfer,
    VerifiedInvalidatedRadioThreshold,
    VerifiedSubscriberMappingActivity,
    VerifiedRadioThreshold,
    VerifiedUniqueConnections,
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

#[async_trait::async_trait]
pub trait DbTable: Sized {
    type Item;

    async fn create_table(db: &huckli_db::Db) -> Result<(), huckli_db::DbError>;
    async fn save(db: &huckli_db::Db, data: Vec<Self::Item>) -> Result<(), huckli_db::DbError>;
}

pub async fn get_and_persist<F, T>(
    db: &huckli_db::Db,
    s3: &huckli_s3::S3,
    bucket: &str,
    prefix: &str,
    time: &TimeArgs,
) -> Result<(), ImportError>
where
    F: prost::Message + Default,
    T: From<F> + DbTable<Item = T>,
{
    T::create_table(db).await?;

    let files = s3
        .list_all(
            bucket,
            prefix,
            time.after_utc(db, prefix).await?,
            time.before_utc(),
        )
        .await?;

    get_and_persist_files::<F, T>(db, s3, bucket, &files).await?;

    Ok(())
}

pub async fn get_and_persist_files<F, T>(
    db: &huckli_db::Db,
    s3: &huckli_s3::S3,
    bucket: &str,
    file_infos: &[FileInfo],
) -> Result<(), ImportError>
where
    F: prost::Message + Default,
    T: From<F> + DbTable<Item = T>,
{
    get_and_persist_files_using(db, s3, bucket, file_infos, |db, data| {
        T::save(db, data)
    })
    .await?;
    Ok(())
}

pub async fn get_and_persist_files_using<'a, F, T, S>(
    db: &'a huckli_db::Db,
    s3: &huckli_s3::S3,
    bucket: &str,
    file_infos: &[FileInfo],
    saver: S,
) -> Result<(), ImportError>
where
    S: Fn(
        &'a huckli_db::Db,
        Vec<T>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), huckli_db::DbError>> + Send + 'a>,
    >,
    F: prost::Message + Default,
    T: From<F> + DbTable<Item = T>,
{
    futures::stream::iter(file_infos)
        .map(|file_info| Ok::<_, ImportError>(file_info.clone()))
        .map_ok(|file_info| async {
            let data = get_and_decode::<F, T>(s3, bucket, file_info.clone()).await;
            tracing::info!(file = %file_info.key, timestamp = %file_info.timestamp, "processing");
            saver(db, data).await?;
            db.save_file_processed(&file_info.key, &file_info.prefix, file_info.timestamp)
                .map_err(ImportError::from)
                .await?;
            Ok(file_info)
        })
        .try_buffered(10)
        .try_collect::<Vec<_>>()
        .await?;

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

#[derive(Debug, clap::Args, Default)]
pub struct TimeArgs {
    #[arg(long)]
    pub after: Option<NaiveDateTime>,
    #[arg(long)]
    pub before: Option<NaiveDateTime>,
    #[arg(long, default_value_t = false)]
    pub r#continue: bool,
}

impl TimeArgs {
    pub fn validate(&self) -> Result<(), ImportError> {
        if self.r#continue && self.after.is_some() {
            return Err(ImportError::TimeArgs);
        }

        Ok(())
    }

    pub async fn after_utc(
        &self,
        db: &huckli_db::Db,
        prefix: &str,
    ) -> Result<Option<DateTime<Utc>>, ImportError> {
        if self.r#continue {
            let latest = db
                .latest_file_processed_timestamp(prefix)
                .map_err(ImportError::from)
                .await?;

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
    opt.ok_or_else(|| rust_decimal::Error::ErrorString("decimal not present".to_string()))
        .and_then(|d| Decimal::from_str(&d.value))
        .map_err(ImportError::from)
        .unwrap_or_default()
        .try_into()
        .unwrap()
}
