use async_duckdb::{Client, ClientBuilder};
use chrono::{DateTime, Utc};

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error(transparent)]
    Duckdb(#[from] async_duckdb::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub struct Db {
    client: Client,
}

impl Db {
    pub async fn connect(file: &str) -> Result<Self, DbError> {
        let client = ClientBuilder::new().path(file).open().await?;

        client
            .conn(|conn| {
                conn.execute("SET TimeZone = 'UTC'", [])?;
                Ok(())
            })
            .await?;

        Self::create_files_processed_table(&client).await?;

        Ok(Self { client })
    }

    pub async fn execute(&self, query: &str) -> Result<usize, DbError> {
        let query = query.to_string();
        self.client
            .conn(move |conn| conn.execute(&query, []))
            .await
            .map_err(DbError::from)
    }

    async fn create_files_processed_table(client: &Client) -> Result<(), DbError> {
        client
            .conn(|conn| {
                conn.execute(
                    r#"
                    CREATE TABLE IF NOT EXISTS files_processed (
                        file_name TEXT NOT NULL,
                        prefix TEXT NOT NULL,
                        file_timestamp timestamptz NOT NULL,
                        processed_at timestamptz NOT NULL
                    )
                "#,
                    [],
                )
            })
            .await?;

        Ok(())
    }

    pub async fn save_file_processed(
        &self,
        name: &str,
        prefix: &str,
        timestamp: DateTime<Utc>,
    ) -> Result<(), DbError> {
        let name = name.to_string();
        let prefix = prefix.to_string();
        let now = Utc::now();

        self.client.conn(move |conn| {
            conn.execute(
                "INSERT INTO files_processed(file_name, prefix, file_timestamp, processed_at) VALUES(?, ?, ? ,?)",
                duckdb::params![name, prefix, timestamp, now],
            )
        }).await?;

        Ok(())
    }

    pub async fn latest_file_processed_timestamp(
        &self,
        prefix: &str,
    ) -> Result<DateTime<Utc>, DbError> {
        let prefix = prefix.to_string();
        self.client
            .conn(move |conn| {
                let mut stmt = conn.prepare(
                    r#"
                    SELECT file_timestamp
                    FROM files_processed
                    WHERE prefix = ?
                    ORDER BY file_timestamp DESC
                    LIMIT 1
                "#,
                )?;

                let row = stmt.query_row([prefix], |r| r.get(0))?;
                Ok(row)
            })
            .await
            .map_err(DbError::from)
    }

    pub async fn create_table(&self, name: &str, fields: Vec<TableField>) -> Result<(), DbError> {
        let statement = format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            name,
            fields
                .iter()
                .map(|f| f.to_sql())
                .join(","),
        );

        self.execute(&statement).await?;

        Ok(())
    }

    pub async fn append_to_table<A>(&self, table: &str, data: Vec<A>) -> Result<(), DbError>
    where
        A: Appendable + Send + 'static,
    {
        let table = table.to_string();
        self.client
            .conn_mut(move |conn| {
                let mut appender = conn.appender(&table)?;
                for entry in data {
                    entry.append_sync(&mut appender)?;
                }
                Ok(())
            })
            .await
            .map_err(DbError::from)
    }
}

pub trait Appendable {
    fn append_sync(&self, appender: &mut duckdb::Appender) -> Result<(), duckdb::Error>;
}

pub struct TableField {
    name: String,
    sql_type: Option<String>,
    nullable: Option<bool>,
}

impl TableField {
    pub fn new(name: String, sql_type: Option<String>, nullable: Option<bool>) -> Self {
        Self {
            name,
            sql_type,
            nullable,
        }
    }

    fn to_sql(&self) -> String {
        let nullable = if self.nullable.unwrap_or_default() {
            "NULL"
        } else {
            "NOT NULL"
        };

        format!(
            "{} {} {}",
            self.name,
            self.sql_type.as_deref().unwrap_or("TEXT"),
            nullable
        )
    }
}
