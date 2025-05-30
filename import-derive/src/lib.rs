use case::CaseExt;
use darling::FromDeriveInput;
use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{parse_macro_input, DeriveInput};

#[derive(Debug, darling::FromField, Clone)]
#[darling(attributes(import))]
struct Field {
    ident: Option<syn::Ident>,
    sql: Option<String>,
    nullable: Option<bool>,
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

        tokens.extend(quote! { db::TableField::new(#name.to_string(), #sql, #nullable) });
    }
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

    let out = quote! {

        impl #name {
            pub async fn get_and_persist(
                args: &crate::Args,
            ) -> anyhow::Result<()> {
                let db = db::Db::connect(&args.db)?;
                let s3 = args.s3.connect().await;

                db.create_table(#table_name, vec![#(#fields),*])?;

                let stream = crate::stream_and_decode::<#proto, #name>(
                    &s3,
                    #bucket,
                    #prefix,
                    &args.time
                ).await?;

                db.append_to_table(#table_name, stream).await?;

                Ok(())
            }
        }

        impl db::Appendable for #name {
            fn append(&self, appender: &mut duckdb::Appender) -> anyhow::Result<()> {
                appender.append_row(duckdb::params![#(self.#field_names),*])
                    .map_err(anyhow::Error::from)
            }
        }

    };

    out.into()
}
