use case::CaseExt;
use darling::{FromDeriveInput, FromMeta};
use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{parse_macro_input, DeriveInput};

#[derive(Debug, darling::FromField, Clone)]
#[darling(attributes(import))]
struct Field {
    ident: Option<syn::Ident>,
    sql: Option<String>,
    nullable: Option<bool>,
    #[darling(default)]
    skip: bool,
}

impl ToTokens for Field {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let name = self.ident.as_ref().unwrap().to_string();
        let sql = if let Some(t) = self.sql.as_ref() {
            quote! { Some(#t.to_string()) }
        } else {
            quote! { None }
        };
        let nullable = if let Some(n) = self.nullable {
            quote! { Some(#n) }
        } else {
            quote! { None }
        };

        tokens.extend(quote! { huckli_db::TableField::new(#name.to_string(), #sql, #nullable) });
    }
}

#[derive(Debug, FromMeta)]
struct S3Decode {
    proto: syn::Ident,
    bucket: String,
    prefix: String,
}

#[derive(Debug, darling::FromDeriveInput)]
#[darling(attributes(import), supports(struct_any))]
struct PersistDeriveOpts {
    ident: syn::Ident,
    data: darling::ast::Data<(), Field>,
    s3decode: Option<S3Decode>,
    table_name: Option<String>,
}

#[proc_macro_derive(Import, attributes(import))]
pub fn persist_derive(input: TokenStream) -> TokenStream {
    let input: DeriveInput = parse_macro_input!(input);

    let opts = match PersistDeriveOpts::from_derive_input(&input) {
        Ok(data) => data,
        Err(e) => return e.write_errors().into(),
    };

    let name = opts.ident.clone();

    let table_name = opts
        .table_name
        .unwrap_or_else(|| opts.ident.to_string().to_snake());

    let fields = opts
        .data
        .clone()
        .take_struct()
        .unwrap()
        .fields
        .into_iter()
        .filter(|f| !f.skip)
        .collect::<Vec<_>>();

    let field_names = fields.iter().map(|f| f.ident.clone()).collect::<Vec<_>>();

    let persist = quote! {
        impl crate::DbTable for #name {
            fn create_table(db: &huckli_db::Db) -> anyhow::Result<()> {
                let fields = vec![
                    #(#fields),*,
                    huckli_db::TableField::new(
                        "file_source".to_string(),
                        Some("TEXT".to_string()),
                        Some(false)
                    )
                ];
                db.create_table(#table_name, fields)
            }

            fn save(db: &huckli_db::Db, data: Vec<Self>) -> anyhow::Result<()> {
                db.append_to_table(#table_name, data)
            }
        }

        impl huckli_db::Appendable for #name {
            fn append(&self, appender: &mut duckdb::Appender) -> anyhow::Result<()> {
                let file_source = crate::get_file_source()
                    .unwrap_or_else(|| "unknown".to_string());
                appender.append_row(duckdb::params![#(self.#field_names),*, file_source])
                    .map_err(anyhow::Error::from)
            }
        }

    };

    let decode = if let Some(s3decode) = opts.s3decode {
        let proto = s3decode.proto;
        let bucket = s3decode.bucket;
        let prefix = s3decode.prefix;
        quote! {
            impl #name {
                pub async fn get_and_persist(
                    db: &huckli_db::Db,
                    s3: &huckli_s3::S3,
                    selection: &crate::FileSelectionArgs,
                ) -> anyhow::Result<()> {
                    crate::get_and_persist::<#proto, #name>(
                        db,
                        s3,
                        #bucket,
                        #prefix,
                        selection,
                    ).await
                }
            }
        }
    } else {
        quote! {}
    };

    quote! {
        #persist
        #decode
    }
    .into()
}
