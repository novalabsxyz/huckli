use clap::Parser;
use import::{SupportedFileTypes, TimeArgs};

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

    let db = db::Db::connect(&args.db)?;
    let s3 = args.s3.connect().await;

    import::run(args.file_type, &db, &s3, &args.time).await
}
