use std::str::FromStr;

use aws_sdk_s3::{Client, error::SdkError};
use chrono::{DateTime, TimeZone, Utc};
use futures::{Stream, StreamExt, TryStream, TryStreamExt};
use regex::Regex;

#[derive(Debug, thiserror::Error)]
pub enum S3Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Decode error: {0}")]
    Decode(S3DecodeError),
    #[error("RPC error: {0}")]
    Rpc(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, thiserror::Error)]
pub enum S3DecodeError {
    #[error("Failed to decode file info: {0}")]
    FileInfo(String),
    #[error("Failed to decode timestamp: {0}")]
    Timestamp(String),
}

impl<E, R> From<SdkError<E, R>> for S3Error
where
    E: std::error::Error + Send + Sync + 'static,
    R: std::fmt::Debug + Send + Sync + 'static,
{
    fn from(value: SdkError<E, R>) -> Self {
        match value.into_source() {
            Ok(e) => S3Error::Rpc(e),
            Err(e) => S3Error::Rpc(e.into()),
        }
    }

}

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub key: String,
    pub prefix: String,
    pub timestamp: DateTime<Utc>,
}

lazy_static::lazy_static! {
    static ref RE: Regex = Regex::new(r"([a-z,\d,_]+)\.(\d+)(\.gz)?").unwrap();
}

impl FromStr for FileInfo {
    type Err = S3Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let key = s.to_string();
        let cap = RE
            .captures(s)
            .ok_or_else(|| S3Error::Decode(S3DecodeError::FileInfo(s.to_string())))?;
        let prefix = cap[1].to_owned();
        let timestamp = Utc
            .timestamp_millis_opt(
                i64::from_str(&cap[2])
                    .map_err(|_| S3Error::Decode(S3DecodeError::Timestamp(s.to_string())))?,
            )
            .single()
            .unwrap();
        Ok(Self {
            key,
            prefix,
            timestamp,
        })
    }
}

#[derive(Debug, clap::Args, Default)]
pub struct S3Args {
    #[arg(short, long)]
    prefix: Option<String>,
    #[arg(short, long)]
    bucket: Option<String>,
    #[arg(short, long, default_value = "us-west-2")]
    region: String,
    #[arg(short, long)]
    endpoint: Option<String>,
}

impl S3Args {
    pub async fn connect(&self) -> S3 {
        let region = aws_config::Region::new(self.region.clone());
        let sdk_config = aws_config::load_from_env().await;
        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);
        s3_config_builder.set_region(Some(region));
        s3_config_builder.set_endpoint_url(self.endpoint.clone());
        s3_config_builder.set_force_path_style(Some(true));

        let s3_config = s3_config_builder.build();
        let client = Client::from_conf(s3_config);

        S3 {
            client,
            bucket: self.bucket.clone(),
            prefix: self.prefix.clone(),
        }
    }
}

pub struct S3 {
    client: aws_sdk_s3::Client,
    bucket: Option<String>,
    prefix: Option<String>,
}

impl S3 {
    pub async fn list_all<A, B>(
        &self,
        bucket: &str,
        prefix: &str,
        after: A,
        before: B,
    ) -> Result<Vec<FileInfo>, S3Error>
    where
        A: Into<Option<DateTime<Utc>>>,
        B: Into<Option<DateTime<Utc>>>,
    {
        let prefix = self.prefix.as_deref().unwrap_or(prefix);
        let start_after = after
            .into()
            .map(|dt| format!("{}.{}.gz", prefix, dt.timestamp_millis()));
        let before = before.into();

        let request = self
            .client
            .list_objects_v2()
            .bucket(self.bucket.as_deref().unwrap_or(bucket))
            .prefix(prefix)
            .set_start_after(start_after);

        let objects = futures::stream::unfold(
            (request, true, None),
            |(req, first_time, next)| async move {
                if first_time || next.is_some() {
                    let response = req.clone().set_continuation_token(next).send().await;

                    let next_token = response
                        .as_ref()
                        .ok()
                        .and_then(|r| r.next_continuation_token())
                        .map(|x| x.to_owned());

                    Some((response, (req, false, next_token)))
                } else {
                    None
                }
            },
        )
        .map_err(S3Error::from)
        .try_filter_map(|output| async move {
            match output.contents {
                Some(objs) => {
                    let infos = objs
                        .into_iter()
                        .map(|o| FileInfo::from_str(&o.key.unwrap()))
                        .collect::<Result<Vec<_>, _>>()?;

                    Ok(Some(infos))
                }
                None => Ok(None),
            }
        })
        .try_fold(Vec::new(), |mut acc, v| async move {
            acc.extend(v);
            Ok(acc)
        })
        .await?
        .into_iter()
        .filter(|i| before.is_none_or(|b| i.timestamp <= b))
        .collect();

        Ok(objects)
    }

    pub fn stream_files(
        &self,
        bucket: &str,
        files: Vec<FileInfo>,
    ) -> impl Stream<Item = bytes::BytesMut> {
        futures::stream::iter(files)
            .then(move |f| {
                let client = self.client.clone();
                let self_bucket = self.bucket.clone();
                async move {
                    get_bytes_stream(&client, self_bucket.as_deref().unwrap_or(bucket), &f).await
                }
            })
            .map_ok(stream_source)
            .try_flatten()
            .filter_map(|result| futures::future::ready(result.ok()))
    }
}

fn stream_source(
    stream: aws_sdk_s3::primitives::ByteStream,
) -> impl TryStream<Ok = bytes::BytesMut, Error = S3Error> {
    use async_compression::tokio::bufread::GzipDecoder;
    use tokio_util::codec::{FramedRead, length_delimited::LengthDelimitedCodec};

    Box::pin(
        FramedRead::new(
            GzipDecoder::new(stream.into_async_read()),
            LengthDelimitedCodec::new(),
        )
        .map_err(S3Error::from),
    )
}

async fn get_bytes_stream(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    info: &FileInfo,
) -> Result<aws_sdk_s3::primitives::ByteStream, S3Error> {
    client
        .get_object()
        .bucket(bucket)
        .key(&info.key)
        .send()
        .await
        .map(|o| o.body)
        .map_err(S3Error::from)
}
