use chrono::{DateTime, Utc};
use helium_proto::{BlockchainTokenTypeV1, PriceReportV1};
use huckli_import_derive::Import;

use crate::to_datetime;

#[derive(Debug, Import)]
#[import(
    table_name = "price_report",
    s3decode(
        proto = PriceReportV1,
        bucket = "helium-mainnet-mobile-price",
        prefix = "price_report",
    )
)]
pub struct PriceReport {
    #[import(sql = "uint64")]
    price: u64,
    #[import(sql = "timestamptz")]
    timestamp: DateTime<Utc>,
    token_type: String,
}

impl From<PriceReportV1> for PriceReport {
    fn from(value: PriceReportV1) -> Self {
        Self {
            price: value.price,
            timestamp: to_datetime(value.timestamp),
            token_type: BlockchainTokenTypeV1::try_from(value.token_type)
                .map(|t| t.as_str_name().to_string())
                .unwrap_or_default(),
        }
    }
}
