pub mod data_transfer;
pub mod heartbeats;
pub mod mobile_rewards;
pub mod subscribers;
pub mod usage;

use std::str::FromStr;

use anyhow::Context;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use futures::{StreamExt, TryStreamExt};
use rust_decimal::Decimal;

pub async fn run(
    file_type: SupportedFileTypes,
    db: &db::Db,
    s3: &s3::S3,
    time: &crate::TimeArgs,
) -> anyhow::Result<()> {
    match file_type {
        SupportedFileTypes::DataTransferIngest => {
            data_transfer::DataTransferIngestReport::get_and_persist(db, s3, time).await?;
        }
        SupportedFileTypes::MobileRewards => {
            mobile_rewards::MobileReward::get_and_persist(db, s3, time).await?;
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
        SupportedFileTypes::VerifiedDataTransfer => {
            data_transfer::VerifiedDataTransferIngestReport::get_and_persist(db, s3, time).await?;
        }
        SupportedFileTypes::VerifiedSubscriberMappingActivity => {
            subscribers::VerifiedSubscriberMappingActivity::get_and_persist(db, s3, time).await?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum SupportedFileTypes {
    DataTransferIngest,
    MobileRewards,
    RadioUsageStats,
    SubscriberMappingActivityIngest,
    ValidatedHeartbeat,
    VerifiedDataTransfer,
    VerifiedSubscriberMappingActivity,
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
    fn create_table(db: &db::Db) -> anyhow::Result<()>;

    fn save(db: &db::Db, data: Vec<Self>) -> anyhow::Result<()>;
}

pub async fn get_and_persist<F, T>(
    db: &db::Db,
    s3: &s3::S3,
    bucket: &str,
    prefix: &str,
    time: &TimeArgs,
) -> anyhow::Result<()>
where
    F: prost::Message + Default,
    T: From<F> + DbTable,
{
    T::create_table(db)?;

    let files = s3
        .list_all(
            bucket,
            prefix,
            time.after_utc(db, prefix)?,
            time.before_utc(),
        )
        .await?;

    for file in files {
        println!("processing file {} - {}", file.key, file.timestamp);

        let data = get_and_decode::<F, T>(s3, bucket, file.clone()).await;
        T::save(db, data)?;
        db.save_file_processed(&file.key, &file.prefix, file.timestamp)?;
    }

    Ok(())
}

pub async fn get_and_decode<F, T>(s3: &s3::S3, bucket: &str, file: s3::FileInfo) -> Vec<T>
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
                    eprintln!("error in decoding record: {}", e);
                    None
                }
            }
        })
        .collect()
        .await
}

#[derive(Debug, clap::Args)]
pub struct TimeArgs {
    #[arg(long)]
    after: Option<NaiveDateTime>,
    #[arg(long)]
    before: Option<NaiveDateTime>,
    #[arg(long, default_value_t = false)]
    r#continue: bool,
}

impl TimeArgs {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.r#continue && self.after.is_some() {
            anyhow::bail!("Invalid options, cannot specify both 'continue' and 'after'");
        }

        Ok(())
    }

    pub fn after_utc(&self, db: &db::Db, prefix: &str) -> anyhow::Result<Option<DateTime<Utc>>> {
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
