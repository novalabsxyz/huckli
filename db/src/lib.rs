use chrono::{DateTime, Utc};
use duckdb::OptionalExt;

pub struct Db {
    connection: duckdb::Connection,
}

impl Db {
    pub fn connect(file: &str) -> anyhow::Result<Self> {
        let connection = duckdb::Connection::open(file)?;
        connection.execute("SET TimeZone = 'UTC'", [])?;
        Self::create_files_processed_table(&connection)?;

        Ok(Self { connection })
    }

    fn create_files_processed_table(connection: &duckdb::Connection) -> anyhow::Result<()> {
        connection.execute(
            r#"
                CREATE TABLE IF NOT EXISTS files_processed (
                    file_name TEXT NOT NULL,
                    prefix TEXT NOT NULL,
                    file_timestamp timestamptz NOT NULL,
                    processed_at timestamptz NOT NULL
                )
            "#,
            [],
        )?;

        Ok(())
    }

    pub fn save_file_processed(
        &self,
        name: &str,
        prefix: &str,
        timestamp: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        self.connection.execute("INSERT INTO files_processed(file_name, prefix, file_timestamp, processed_at) VALUES(?, ?, ? ,?)", duckdb::params![name, prefix, timestamp, Utc::now()])?;

        Ok(())
    }

    pub fn latest_file_processed_timestamp(
        &self,
        prefix: &str,
    ) -> anyhow::Result<Option<DateTime<Utc>>> {
        self.connection
            .prepare(
                r#"
                    SELECT file_timestamp
                    FROM files_processed
                    WHERE prefix = ?
                    ORDER BY file_timestamp DESC
                    LIMIT 1
                "#,
            )?
            .query_row([prefix], |r| r.get(0))
            .optional()
            .map_err(anyhow::Error::from)
    }

    pub fn create_table(&self, name: &str, fields: Vec<TableField>) -> anyhow::Result<()> {
        let statement = format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            name,
            fields
                .iter()
                .map(|f| f.to_sql())
                .collect::<Vec<_>>()
                .join(","),
        );

        self.connection.execute(&statement, [])?;

        Ok(())
    }

    pub fn append_to_table<A>(&self, table: &str, data: Vec<A>) -> anyhow::Result<()>
    where
        A: Appendable,
    {
        let mut appender = self.connection.appender(table)?;
        for entry in data {
            entry.append(&mut appender)?;
        }

        Ok(())
    }
}

pub trait Appendable {
    fn append(&self, appender: &mut duckdb::Appender) -> anyhow::Result<()>;
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
        let nullable = if self.nullable.unwrap_or(false) {
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
