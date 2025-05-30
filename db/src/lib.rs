use futures::{Stream, StreamExt};

pub struct Db {
    connection: duckdb::Connection,
}

impl Db {
    pub fn connect(file: &str) -> anyhow::Result<Self> {
        Ok(Self {
            connection: duckdb::Connection::open(file)?,
        })
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

    pub async fn append_to_table<S, A>(&self, table: &str, stream: S) -> anyhow::Result<()>
    where
        S: Stream<Item = A>,
        A: Appendable,
    {
        let mut stream = std::pin::pin!(stream);

        let mut appender = self.connection.appender(table)?;
        while let Some(a) = stream.next().await {
            a.append(&mut appender)?;
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
