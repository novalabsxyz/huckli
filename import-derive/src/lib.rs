use case::CaseExt;
use darling::FromDeriveInput;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[derive(Debug, darling::FromField, Clone)]
#[darling(attributes(import))]
struct Field {
    ident: Option<syn::Ident>,
    sql: Option<String>,
    nullable: Option<bool>,
}

#[derive(Debug, darling::FromDeriveInput)]
#[darling(attributes(import), supports(struct_any))]
struct PersistDeriveOpts {
    ident: syn::Ident,
    data: darling::ast::Data<(), Field>,
    proto: syn::Ident,
    bucket: String,
    prefix: String,
}

#[proc_macro_derive(Import, attributes(import))]
pub fn persist_derive(input: TokenStream) -> TokenStream {
    let input: DeriveInput = parse_macro_input!(input);

    let opts = match PersistDeriveOpts::from_derive_input(&input) {
        Ok(data) => data,
        Err(e) => return e.write_errors().into(),
    };

    let name = opts.ident.clone();

    let table_name = opts.ident.to_string().to_snake();
    let fields = opts.data.take_struct().unwrap().fields;

    let field_names = fields.iter().map(|f| f.ident.clone()).collect::<Vec<_>>();

    let proto = opts.proto;
    let bucket = opts.bucket;
    let prefix = opts.prefix;

    let create_table_sql = create_table_sql(&table_name, fields);

    let out = quote! {

        impl #name {
            pub async fn get_and_persist(
                args: &crate::Args,
            ) -> anyhow::Result<()> {
                use futures::{ StreamExt, TryStreamExt };
                use prost::Message;

                let connection = duckdb::Connection::open(&args.db)?;
                let s3 = args.s3.connect().await;

                connection.execute(#create_table_sql, [])?;

                let files = s3.list_all(#bucket, #prefix, args.time.after_utc(), args.time.before_utc()).await?;

                let mut stream = s3
                    .stream_bytes(#bucket, files)
                    .then(|b| async move { #proto::decode(b) })
                    .and_then(|i| async move { Ok(#name::from(i)) })
                    .filter_map(|o| async move { o.ok() })
                    .boxed();

                let mut appender = connection.appender(#table_name)?;
                while let Some(d) = stream.next().await {
                    appender.append_row(duckdb::params![#(d.#field_names),*])?;
                }

                Ok(())
            }
        }

    };

    out.into()
}

fn create_table_sql(table_name: &str, fields: Vec<Field>) -> String {
    let fields_sql = fields
        .iter()
        .map(|f| {
            let nullable = if f.nullable.unwrap_or(false) {
                "NULL"
            } else {
                "NOT NULL"
            };

            format!(
                "{} {} {}",
                f.ident.as_ref().unwrap().to_string(),
                f.sql.as_deref().unwrap_or("TEXT"),
                nullable
            )
        })
        .collect::<Vec<_>>()
        .join(",");

    format!("CREATE TABLE IF NOT EXISTS {} ({})", table_name, fields_sql)
}
