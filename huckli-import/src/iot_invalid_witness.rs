use chrono::{DateTime, Utc};
use helium_proto::DataRate;
use helium_proto::services::poc_lora::{
    InvalidParticipantSide, InvalidReason, LoraInvalidWitnessReportV1, invalid_details,
};
use huckli_import_derive::Import;

use crate::{PublicKeyBinary, determine_timestamp};

#[derive(Debug, Import)]
#[import(table_name = "iot_invalid_witness")]
pub struct IotInvalidWitness {
    #[import(sql = "timestamptz")]
    received_timestamp: DateTime<Utc>,
    reason: String,
    participant_side: String,
    denylist_tag: String,
    pub_key: String,
    #[import(sql = "uint64")]
    frequency: u64,
    datarate: String,
    #[import(sql = "int32")]
    signal: i32,
    #[import(sql = "int32")]
    snr: i32,
    witness_timestamp: String,
}

impl From<LoraInvalidWitnessReportV1> for IotInvalidWitness {
    fn from(value: LoraInvalidWitnessReportV1) -> Self {
        let report = value.report.unwrap_or_default();
        let denylist_tag = value
            .invalid_details
            .and_then(|d| d.data.map(|invalid_details::Data::DenylistTag(tag)| tag));

        Self {
            received_timestamp: determine_timestamp(value.received_timestamp),
            reason: InvalidReason::try_from(value.reason)
                .map(|r| r.as_str_name().to_string())
                .unwrap_or_default(),
            participant_side: InvalidParticipantSide::try_from(value.participant_side)
                .map(|p| p.as_str_name().to_string())
                .unwrap_or_default(),
            denylist_tag: denylist_tag.unwrap_or("none".to_string()),
            pub_key: PublicKeyBinary::from(report.pub_key).to_string(),
            frequency: report.frequency,
            datarate: DataRate::try_from(report.datarate)
                .map(|d| d.as_str_name().to_string())
                .unwrap_or_default(),
            signal: report.signal,
            snr: report.snr,
            witness_timestamp: report.timestamp.to_string(),
        }
    }
}

impl IotInvalidWitness {
    pub async fn get_and_persist(
        db: &huckli_db::Db,
        s3: &huckli_s3::S3,
        selection: &crate::FileSelectionArgs,
    ) -> anyhow::Result<()> {
        crate::get_and_persist::<LoraInvalidWitnessReportV1, IotInvalidWitness>(
            db,
            s3,
            "helium-mainnet-iot-verified-rewards",
            "iot_invalid_witness",
            selection,
        )
        .await
    }
}
