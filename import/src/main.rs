use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use clap::Parser;

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
