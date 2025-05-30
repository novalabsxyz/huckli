use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use clap::Parser;
use futures::{Stream, StreamExt, TryStreamExt};

mod mobile_rewards;
mod subscribers;

#[derive(Debug, Clone, clap::ValueEnum)]
enum SupportedFileTypes {
    SubscriberMappingActivityIngest,
    VerifiedSubscriberMappingActivity,
}

#[derive(Debug, clap::Parser)]
struct Args {
    #[arg(short, long)]
    db: String,
    #[arg(long)]
    file_type: SupportedFileTypes,
    #[command(flatten)]
    s3: s3::S3Args,
    #[command(flatten)]
    time: TimeArgs,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    match args.file_type {
        SupportedFileTypes::SubscriberMappingActivityIngest => {
            subscribers::SubscriberMappingActivityIngest::get_and_persist(&args).await?;
        }
        SupportedFileTypes::VerifiedSubscriberMappingActivity => {
            subscribers::VerifiedSubscriberMappingActivity::get_and_persist(&args).await?;
        }
    }

    Ok(())
}

pub async fn stream_and_decode<F, T>(
    s3: &s3::S3,
    bucket: &str,
    prefix: &str,
    time: &TimeArgs,
) -> anyhow::Result<impl Stream<Item = T>>
where
    F: prost::Message + Default,
    T: From<F>,
{
    let files = s3
        .list_all(bucket, prefix, time.after_utc(), time.before_utc())
        .await?;

    Ok(s3
        .stream_files(bucket, files)
        .then(|b| async move { F::decode(b) })
        .and_then(|f| async move { Ok(T::from(f)) })
        .filter_map(|result| async move {
            match result {
                Ok(t) => Some(t),
                Err(e) => {
                    eprintln!("error is decoding record: {}", e);
                    None
                }
            }
        }))
}

#[derive(Debug, clap::Args)]
pub struct TimeArgs {
    #[arg(long)]
    after: Option<NaiveDateTime>,
    #[arg(long)]
    before: Option<NaiveDateTime>,
}

impl TimeArgs {
    pub fn after_utc(&self) -> Option<DateTime<Utc>> {
        self.after.as_ref().map(NaiveDateTime::and_utc)
    }

    pub fn before_utc(&self) -> Option<DateTime<Utc>> {
        self.before.as_ref().map(NaiveDateTime::and_utc)
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
