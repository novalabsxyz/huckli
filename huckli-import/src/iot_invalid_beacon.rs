use chrono::{DateTime, Utc};
use helium_proto::DataRate;
use helium_proto::services::poc_lora::{InvalidReason, LoraInvalidBeaconReportV1, invalid_details};
use huckli_import_derive::Import;

use crate::{PublicKeyBinary, determine_timestamp};

#[derive(Debug, Import)]
#[import(table_name = "iot_invalid_beacon")]
pub struct IotInvalidBeacon {
    #[import(sql = "timestamptz")]
    received_timestamp: DateTime<Utc>,
    reason: String,
    location: String,
    #[import(sql = "int32")]
    gain: i32,
    #[import(sql = "int32")]
    elevation: i32,
    denylist_tag: String,
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

impl From<LoraInvalidBeaconReportV1> for IotInvalidBeacon {
    fn from(value: LoraInvalidBeaconReportV1) -> Self {
        let report = value.report.unwrap_or_default();
        let denylist_tag = value
            .invalid_details
            .and_then(|d| d.data.map(|invalid_details::Data::DenylistTag(tag)| tag));

        Self {
            received_timestamp: determine_timestamp(value.received_timestamp),
            reason: InvalidReason::try_from(value.reason)
                .map(|r| r.as_str_name().to_string())
                .unwrap_or_default(),
            location: value.location,
            gain: value.gain,
            elevation: value.elevation,
            denylist_tag: denylist_tag.unwrap_or("none".to_string()),
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

impl IotInvalidBeacon {
    pub async fn get_and_persist(
        db: &huckli_db::Db,
        s3: &huckli_s3::S3,
        selection: &crate::FileSelectionArgs,
    ) -> anyhow::Result<()> {
        crate::get_and_persist::<LoraInvalidBeaconReportV1, IotInvalidBeacon>(
            db,
            s3,
            "helium-mainnet-iot-verified-rewards",
            "iot_invalid_beacon",
            selection,
        )
        .await
    }
}
