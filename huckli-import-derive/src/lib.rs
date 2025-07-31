// this is to work around a darling default clippy issue in the Field struct below
#![allow(clippy::manual_unwrap_or_default)]

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
        let name = self
            .ident
            .as_ref()
            .map(|i| i.to_string())
            .unwrap_or_default();
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

    // Get the actual struct field types from the input
    let original_fields = match &input.data {
        syn::Data::Struct(data) => &data.fields,
        _ => panic!("Import derive only supports structs"),
    };
    let field_types: Vec<_> = original_fields.iter().map(|f| &f.ty).collect();
    
    // Generate quoted string literals for column names
    let column_names: Vec<proc_macro2::TokenStream> = field_names
        .iter()
        .map(|name| {
            let name_str = name.as_ref().unwrap().to_string();
            quote! { #name_str }
        })
        .collect();

    let persist = quote! {
        #[async_trait::async_trait]
        impl crate::DbTable for #name {
            type Item = Self;

            async fn create_table(db: &huckli_db::Db) -> Result<(), huckli_db::DbError> {
                db.create_table(#table_name, vec![#(#fields),*]).await
            }

            async fn save(db: &huckli_db::Db, data: Vec<Self::Item>) -> Result<(), huckli_db::DbError> {
                db.append_to_table(#table_name, data).await
            }
        }

        impl huckli_db::Appendable for #name {
            fn append_sync(&self, appender: &mut duckdb::Appender) -> Result<(), duckdb::Error> {
                appender.append_row(duckdb::params![#(self.#field_names),*])
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
                    time: &crate::TimeArgs,
                ) -> Result<(), crate::ImportError> {
                    crate::get_and_persist::<#proto, #name>(
                        db,
                        s3,
                        #bucket,
                        #prefix,
                        time,
                    ).await
                }
            }
        }
    } else {
        quote! {}
    };

    let from_row = quote! {
        impl<'stmt> ::std::convert::TryFrom<&duckdb::Row<'stmt>> for #name {
            type Error = duckdb::Error;
            fn try_from(row: &duckdb::Row<'stmt>) -> Result<Self, Self::Error> {
                Ok(#name {
                    #(
                        #field_names: row.get::<_, #field_types>(#column_names)?
                    ),*
                })
            }
        }
    };

    quote! {
        #persist
        #decode
        #from_row
    }
    .into()
}
