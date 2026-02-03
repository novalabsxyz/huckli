use chrono::{DateTime, Utc};
use helium_proto::DataRate;
use helium_proto::services::poc_lora::{
    InvalidParticipantSide, InvalidReason, LoraPocV1, LoraValidBeaconReportV1,
    LoraVerifiedWitnessReportV1, VerificationStatus,
};
use huckli_import_derive::Import;

use crate::{PublicKeyBinary, determine_timestamp};

#[derive(Debug)]
pub struct IotPocProto {
    poc: IotPoc,
    beacon: Option<IotPocBeacon>,
    witnesses: Vec<IotPocWitness>,
}

impl From<LoraPocV1> for IotPocProto {
    fn from(value: LoraPocV1) -> Self {
        let poc_id = hex::encode(&value.poc_id);

        let beacon = value.beacon_report.map(|b| b.to_beacon(&poc_id));

        let witnesses = value
            .selected_witnesses
            .into_iter()
            .map(|w| w.to_witness(&poc_id, true))
            .chain(
                value
                    .unselected_witnesses
                    .into_iter()
                    .map(|w| w.to_witness(&poc_id, false)),
            )
            .collect();

        Self {
            poc: IotPoc { poc_id },
            beacon,
            witnesses,
        }
    }
}

impl crate::DbTable for IotPocProto {
    fn create_table(db: &huckli_db::Db) -> anyhow::Result<()> {
        IotPoc::create_table(db)?;
        IotPocBeacon::create_table(db)?;
        IotPocWitness::create_table(db)?;
        Ok(())
    }

    fn save(db: &huckli_db::Db, data: Vec<Self>) -> anyhow::Result<()> {
        let mut pocs = Vec::new();
        let mut beacons = Vec::new();
        let mut witnesses = Vec::new();

        for mut proto in data {
            pocs.push(proto.poc);
            if let Some(beacon) = proto.beacon.take() {
                beacons.push(beacon);
            }
            witnesses.append(&mut proto.witnesses);
        }

        IotPoc::save(db, pocs)?;
        IotPocBeacon::save(db, beacons)?;
        IotPocWitness::save(db, witnesses)?;

        Ok(())
    }
}

impl IotPocProto {
    pub async fn get_and_persist(
        db: &huckli_db::Db,
        s3: &huckli_s3::S3,
        selection: &crate::FileSelectionArgs,
    ) -> anyhow::Result<()> {
        crate::get_and_persist::<LoraPocV1, IotPocProto>(
            db,
            s3,
            "helium-mainnet-iot-verified-rewards",
            "iot_poc",
            selection,
        )
        .await
    }
}

#[derive(Debug, Import)]
pub struct IotPoc {
    poc_id: String,
}

#[derive(Debug, Import)]
pub struct IotPocBeacon {
    poc_id: String,
    #[import(sql = "timestamptz")]
    received_timestamp: DateTime<Utc>,
    location: String,
    #[import(sql = "uint32")]
    hex_scale: u32,
    #[import(sql = "uint32")]
    reward_unit: u32,
    #[import(sql = "int32")]
    gain: i32,
    #[import(sql = "int32")]
    elevation: i32,
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

trait ToBeacon {
    fn to_beacon(self, poc_id: &str) -> IotPocBeacon;
}

impl ToBeacon for LoraValidBeaconReportV1 {
    fn to_beacon(self, poc_id: &str) -> IotPocBeacon {
        let report = self.report.unwrap_or_default();
        IotPocBeacon {
            poc_id: poc_id.to_string(),
            received_timestamp: determine_timestamp(self.received_timestamp),
            location: self.location,
            hex_scale: self.hex_scale,
            reward_unit: self.reward_unit,
            gain: self.gain,
            elevation: self.elevation,
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

#[derive(Debug, Import)]
pub struct IotPocWitness {
    poc_id: String,
    #[import(sql = "bool")]
    selected: bool,
    #[import(sql = "timestamptz")]
    received_timestamp: DateTime<Utc>,
    status: String,
    location: String,
    #[import(sql = "uint32")]
    hex_scale: u32,
    #[import(sql = "uint32")]
    reward_unit: u32,
    invalid_reason: String,
    participant_side: String,
    #[import(sql = "int32")]
    gain: i32,
    #[import(sql = "int32")]
    elevation: i32,
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

trait ToWitness {
    fn to_witness(self, poc_id: &str, selected: bool) -> IotPocWitness;
}

impl ToWitness for LoraVerifiedWitnessReportV1 {
    fn to_witness(self, poc_id: &str, selected: bool) -> IotPocWitness {
        let report = self.report.unwrap_or_default();
        IotPocWitness {
            poc_id: poc_id.to_string(),
            selected,
            received_timestamp: determine_timestamp(self.received_timestamp),
            status: VerificationStatus::try_from(self.status)
                .map(|s| s.as_str_name().to_string())
                .unwrap_or_default(),
            location: self.location,
            hex_scale: self.hex_scale,
            reward_unit: self.reward_unit,
            invalid_reason: InvalidReason::try_from(self.invalid_reason)
                .map(|r| r.as_str_name().to_string())
                .unwrap_or_default(),
            participant_side: InvalidParticipantSide::try_from(self.participant_side)
                .map(|p| p.as_str_name().to_string())
                .unwrap_or_default(),
            gain: self.gain,
            elevation: self.elevation,
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
