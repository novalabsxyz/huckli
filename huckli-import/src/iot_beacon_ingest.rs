use chrono::{DateTime, Utc};
use helium_proto::DataRate;
use helium_proto::services::poc_lora::LoraBeaconIngestReportV1;
use huckli_import_derive::Import;

use crate::{PublicKeyBinary, determine_timestamp};

#[derive(Debug, Import)]
#[import(table_name = "iot_beacon_ingest")]
pub struct IotBeaconIngest {
    #[import(sql = "timestamptz")]
    received_timestamp: DateTime<Utc>,
    pub_key: String,
    #[import(sql = "uint64")]
    frequency: u64,
    #[import(sql = "int32")]
    channel: i32,
    datarate: String,
    #[import(sql = "int32")]
    tx_power: i32,
    beacon_timestamp: String,
}

impl From<LoraBeaconIngestReportV1> for IotBeaconIngest {
    fn from(value: LoraBeaconIngestReportV1) -> Self {
        let report = value.report.unwrap_or_default();
        Self {
            received_timestamp: determine_timestamp(value.received_timestamp),
            pub_key: PublicKeyBinary::from(report.pub_key).to_string(),
            frequency: report.frequency,
            channel: report.channel,
            datarate: DataRate::try_from(report.datarate)
                .map(|d| d.as_str_name().to_string())
                .unwrap_or_default(),
            tx_power: report.tx_power,
            beacon_timestamp: report.timestamp.to_string(),
        }
    }
}

impl IotBeaconIngest {
    pub async fn get_and_persist(
        db: &huckli_db::Db,
        s3: &huckli_s3::S3,
        selection: &crate::FileSelectionArgs,
    ) -> anyhow::Result<()> {
        crate::get_and_persist::<LoraBeaconIngestReportV1, IotBeaconIngest>(
            db,
            s3,
            "helium-mainnet-iot-ingest",
            "iot_beacon_ingest_report",
            selection,
        )
        .await
    }
}
