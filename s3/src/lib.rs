use std::str::FromStr;

use chrono::{DateTime, TimeZone, Utc};
use futures::{Stream, StreamExt, TryStream, TryStreamExt};
use regex::Regex;

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
    type Err = anyhow::Error;
    fn from_str(s: &str) -> anyhow::Result<Self> {
        let key = s.to_string();
        let cap = RE
            .captures(s)
            .ok_or_else(|| anyhow::anyhow!("failed to decode file info"))?;
        let prefix = cap[1].to_owned();
        let timestamp = Utc
            .timestamp_millis_opt(i64::from_str(&cap[2])?)
            .single()
            .unwrap();
        Ok(Self {
            key,
            prefix,
            timestamp,
        })
    }
}

#[derive(Debug, clap::Args)]
pub struct S3Args {
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
        let mut loader = aws_config::from_env().region(region);

        if let Some(endpoint_url) = self.endpoint.as_ref() {
            loader = loader.endpoint_url(endpoint_url);
        }

        let config = loader.load().await;
        let client = aws_sdk_s3::Client::new(&config);

        S3 {
            client,
            bucket: self.bucket.clone(),
        }
    }
}

pub struct S3 {
    client: aws_sdk_s3::Client,
    bucket: Option<String>,
}

impl S3 {
    pub async fn list_all<A, B>(
        &self,
        bucket: &str,
        prefix: &str,
        after: A,
        before: B,
    ) -> anyhow::Result<Vec<FileInfo>>
    where
        A: Into<Option<DateTime<Utc>>>,
        B: Into<Option<DateTime<Utc>>>,
    {
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
        .err_into::<anyhow::Error>()
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
        futures::stream::iter(files.into_iter())
            .inspect(|f| {
                println!("processing {}", f.key);
            })
            .then(move |f| {
                let client = self.client.clone();
                let self_bucket = self.bucket.clone();
                async move {
                    get_bytes_stream(&client, self_bucket.as_deref().unwrap_or(bucket), &f).await
                }
            })
            .map_ok(|bs| stream_source(bs))
            .try_flatten()
            .filter_map(|result| futures::future::ready(result.ok()))
    }
}

fn stream_source(
    stream: aws_sdk_s3::primitives::ByteStream,
) -> impl TryStream<Ok = bytes::BytesMut, Error = anyhow::Error> {
    use async_compression::tokio::bufread::GzipDecoder;
    use tokio_util::codec::{FramedRead, length_delimited::LengthDelimitedCodec};

    Box::pin(
        FramedRead::new(
            GzipDecoder::new(stream.into_async_read()),
            LengthDelimitedCodec::new(),
        )
        .map_err(anyhow::Error::from),
    )
}

async fn get_bytes_stream(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    info: &FileInfo,
) -> anyhow::Result<aws_sdk_s3::primitives::ByteStream> {
    client
        .get_object()
        .bucket(bucket)
        .key(&info.key)
        .send()
        .await
        .map(|o| o.body)
        .map_err(anyhow::Error::from)
}
