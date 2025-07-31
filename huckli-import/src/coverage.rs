use async_trait::async_trait;
use chrono::{DateTime, Utc};
use helium_proto::services::poc_mobile::{CoverageObjectV1, coverage_object_req_v1};
use huckli_import_derive::Import;

use crate::{PublicKeyBinary, determine_timestamp};

#[derive(Debug)]
pub struct CoverageObjectProto {
    object: CoverageObject,
    locations: Vec<CoverageLocation>,
}

#[async_trait]
impl crate::DbTable for CoverageObjectProto {
    type Item = Self;

    async fn create_table(db: &huckli_db::Db) -> Result<(), huckli_db::DbError> {
        CoverageObject::create_table(db).await?;
        CoverageLocation::create_table(db).await?;

        Ok(())
    }

    async fn save(db: &huckli_db::Db, data: Vec<Self>) -> Result<(), huckli_db::DbError> {
        let mut objects = Vec::new();
        let mut locations = Vec::new();

        for mut p in data {
            objects.push(p.object);
            locations.append(&mut p.locations);
        }

        CoverageObject::save(db, objects).await?;
        CoverageLocation::save(db, locations).await?;

        Ok(())
    }
}

impl CoverageObjectProto {
    pub async fn get_and_persist(
        db: &huckli_db::Db,
        s3: &huckli_s3::S3,
        time: &crate::TimeArgs,
    ) -> Result<(), crate::ImportError> {
        crate::get_and_persist::<CoverageObjectV1, CoverageObjectProto>(
            db,
            s3,
            "helium-mainnet-mobile-verified",
            "coverage_object",
            time,
        )
        .await
    }
}

impl From<CoverageObjectV1> for CoverageObjectProto {
    fn from(value: CoverageObjectV1) -> Self {
        let req = value.coverage_object.as_ref().unwrap();

        let (radio_key, radio_type) = match req.key_type.as_ref() {
            Some(coverage_object_req_v1::KeyType::HotspotKey(hk)) => (
                PublicKeyBinary::from(hk.clone()).to_string(),
                "wifi".to_string(),
            ),
            Some(coverage_object_req_v1::KeyType::CbsdId(cbsd_id)) => {
                (cbsd_id.to_owned(), "cbrs".to_string())
            }
            _ => panic!("invalid key type"),
        };

        let uuid = uuid::Uuid::from_slice(&req.uuid).unwrap().to_string();

        Self {
            object: CoverageObject {
                radio_key,
                radio_type,
                uuid: uuid.clone(),
                coverage_claim_time: determine_timestamp(req.coverage_claim_time),
                indoor: req.indoor,
            },
            locations: req
                .coverage
                .iter()
                .map(|c| CoverageLocation {
                    uuid: uuid.clone(),
                    location: c.location.clone(),
                    signal_level: c.signal_level().as_str_name().to_string(),
                    signal_power: c.signal_power,
                })
                .collect(),
        }
    }
}

#[derive(Debug, Import)]
pub struct CoverageObject {
    radio_key: String,
    radio_type: String,
    uuid: String,
    #[import(sql = "timestamptz")]
    coverage_claim_time: DateTime<Utc>,
    #[import(sql = "bool")]
    indoor: bool,
}

#[derive(Debug, Import)]
pub struct CoverageLocation {
    uuid: String,
    location: String,
    signal_level: String,
    #[import(sql = "int32")]
    signal_power: i32,
}
