use clap::Parser;
use import::{SupportedFileTypes, TimeArgs};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

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
    args.time.validate()?;

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("info"))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let db = db::Db::connect(&args.db)?;
    let s3 = args.s3.connect().await;

    import::run(args.file_type, &db, &s3, &args.time).await
}
