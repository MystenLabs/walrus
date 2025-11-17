// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//! Implementation of the [`RestApiError`] derive macro.
//!
//! See the [`crate::derive_rest_api_error`] for more details.

use darling::{
    Error,
    FromDeriveInput,
    FromMeta,
    FromVariant,
    Result,
    ast::{self, Data, Style},
    util::{Flag, Ignored, SpannedValue},
};
use proc_macro2::{Span, TokenStream};
use quote::{ToTokens, format_ident, quote};
use syn::{Attribute, DeriveInput, Ident, spanned::Spanned as _};

/// Derive the `RestApiError` trait and associated traits.
pub(super) fn derive(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(item as DeriveInput);

    match try_derive(input) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.write_errors().into(),
    }
}

/// Struct for parsing the `derive(RestApiError)` macro on enums and structs.
#[derive(Debug, FromDeriveInput)]
#[darling(
    attributes(rest_api_error),
    forward_attrs(doc),
    supports(enum_any, struct_any)
)]
struct ParsedTokens {
    /// The identifier for the struct or enum.
    ident: syn::Ident,
    /// The data contained within the struct or enum.
    data: Data<ErrorEnumVariant, Ignored>,
    /// The `rest_api_error` attribute if any.
    attrs: Vec<syn::Attribute>,

    /// The error's domain.
    domain: syn::Expr,

    /// A function that can be used for the implementation of `add_details`.
    details: Option<DetailsFieldAttribute>,

    #[darling(flatten)]
    common_attrs: CommonAttributeFields,
}

#[derive(Debug, FromMeta)]
struct DetailsFieldAttribute {
    serialize: Flag,
}

#[derive(Debug, FromVariant)]
#[darling(attributes(rest_api_error), forward_attrs(doc))]
struct ErrorEnumVariant {
    /// Identifier of the enum variant.
    ident: syn::Ident,
    /// Forwarded attributes, specifically those pertaning to the docs.
    attrs: Vec<syn::Attribute>,
    /// Fields of the enum.
    fields: ast::Fields<syn::Type>,

    /// Whether or not the error info is being delegated to an inner field.
    delegate: darling::util::Flag,

    #[darling(flatten)]
    common_attrs: CommonAttributeFields,
}

impl ErrorEnumVariant {
    fn validate(&self) -> Result<()> {
        let mut errors = Error::accumulator();

        if !self.delegate.is_present() {
            errors.handle(self.common_attrs.validate(&self.ident.span()));
            return errors.finish();
        }

        assert!(self.delegate.is_present());

        if !self.fields.is_newtype() {
            errors.push(
                Error::custom("`delegate` can only be used on new-type enum variants")
                    .with_span(&self.delegate.span()),
            );
        }

        if let Some(reason) = self.common_attrs.reason.as_ref() {
            errors.push(
                Error::custom("the `reason` attribute must not be supplied when delegating")
                    .with_span(&reason.span()),
            );
        }

        if let Some(status) = self.common_attrs.status.as_ref() {
            errors.push(
                Error::custom("the `status` attribute must not be supplied when delegating")
                    .with_span(&status.span()),
            );
        }

        errors.finish()
    }
}

#[derive(Debug, FromMeta, Default)]
struct CommonAttributeFields {
    status: Option<syn::Expr>,
    reason: Option<SpannedValue<String>>,
}

impl CommonAttributeFields {
    /// Validates that the fields `reason` and `status` are both present.
    fn validate(&self, span: &proc_macro2::Span) -> Result<()> {
        let mut errors = Error::accumulator();

        if self.status.is_none() {
            errors.push(Error::missing_field("status").with_span(span));
        }

        if self.reason.is_none() {
            errors.push(Error::missing_field("reason").with_span(span));
        }

        errors.finish()
    }
}

fn try_derive(input: DeriveInput) -> Result<TokenStream> {
    let mut parse_errors = Error::accumulator();
    let Some(parsed) = parse_errors.handle(ParsedTokens::from_derive_input(&input)) else {
        return parse_errors.finish_with(TokenStream::new());
    };

    match parsed.data {
        Data::Enum(ref variants) => {
            variants.iter().for_each(|v| {
                parse_errors.handle(v.validate());
            });
        }
        Data::Struct(_) => {
            parse_errors.handle(parsed.common_attrs.validate(&parsed.ident.span()));
        }
    }
    parse_errors = parse_errors.checkpoint()?;

    let rest_api_error_trait = DeriveRestApiErrorTrait(&parsed);
    let into_response_trait = DeriveIntoResponseTrait(&parsed);
    let into_responses_trait = DeriveIntoResponsesTrait(&parsed);

    let enum_name = &parsed.ident;
    let unique_module_name = format_ident!("__derive_rest_api_error__{}", enum_name);
    let tokens = quote! {
        #[allow(non_snake_case)]
        mod #unique_module_name {
            use super::*;
            use std::collections::HashMap;
            use axum::response::{IntoResponse, Response};
            use reqwest::StatusCode;
            use walrus_storage_node_client::api::errors::{Status, StatusCode as ApiStatusCode};
            use walrus_storage_node_client::error::ServiceError;
            use utoipa::{
                openapi::{response::Response as OpenApiResponse, RefOr},
                IntoResponses,
            };
            use std::collections::BTreeMap;
            use std::sync::Arc;

            #rest_api_error_trait
            #into_response_trait
            #into_responses_trait
        }
    };

    parse_errors.finish_with(tokens)
}

/// Derives the `RestApiError` for the provided input.
struct DeriveRestApiErrorTrait<'a>(&'a ParsedTokens);

impl DeriveRestApiErrorTrait<'_> {
    fn status_method(&self) -> Result<TokenStream> {
        let method_body = match &self.0.data {
            Data::Enum(_) => {
                let match_arms = declare_match_arms(self.0, |variant| {
                    if variant.delegate.is_present() {
                        quote! { inner.status_code() }
                    } else {
                        let status = &variant.common_attrs.status;
                        quote! { #status }
                    }
                });

                quote! {
                    match self { #match_arms }
                }
            }

            Data::Struct(_) => {
                let status_code = self
                    .0
                    .common_attrs
                    .status
                    .as_ref()
                    .expect("caller ensured that the status code is present on the struct");
                quote! { #status_code }
            }
        };

        Ok(quote! {
            fn status_code(&self) -> ApiStatusCode {
                #method_body
            }
        })
    }

    fn domain_method(&self) -> Result<TokenStream> {
        let domain = &self.0.domain;

        Ok(quote! {
            fn domain(&self) -> String {
                #domain.to_owned()
            }
        })
    }

    fn reason_method(&self) -> Result<TokenStream> {
        let method_body = match &self.0.data {
            Data::Enum(_) => {
                let match_arms = declare_match_arms(self.0, |variant| {
                    if variant.delegate.is_present() {
                        quote! { inner.reason() }
                    } else {
                        let maybe_reason = &variant.common_attrs.reason.as_ref();
                        let reason = maybe_reason
                            .expect("validation checks for presence")
                            .as_ref();
                        quote! { #reason.to_owned() }
                    }
                });

                quote! {
                    match self { #match_arms }
                }
            }

            Data::Struct(_) => {
                let maybe_reason = &self.0.common_attrs.reason.as_ref();
                let reason = maybe_reason
                    .expect("validation checks for presence")
                    .as_ref();
                quote! { #reason.to_owned() }
            }
        };

        Ok(quote! {
            fn reason(&self) -> String {
                #method_body
            }
        })
    }

    fn response_descriptions_method(&self) -> Result<TokenStream> {
        let method_body = match &self.0.data {
            Data::Enum(variants) => {
                let mut method_body = quote! {
                    let mut output = HashMap::<StatusCode, Vec<String>>::default();
                };

                for variant in variants {
                    if variant.delegate.is_present() {
                        let inner_type = &variant.fields.fields[0];

                        method_body.extend(quote! {
                            for (code, descriptions) in <#inner_type>::response_descriptions() {
                                output.entry(code).or_default().extend(descriptions);
                            }
                        });
                    } else {
                        let status = &variant.common_attrs.status;
                        let description = get_docstring(&variant.attrs);

                        method_body.extend(quote! {
                            output.entry(#status.http_code()).or_default()
                                .push(#description.to_owned());
                        });
                    }
                }

                quote! {
                    #method_body
                    output
                }
            }

            Data::Struct(_) => {
                let description = get_docstring(&self.0.attrs);
                let status_code = self
                    .0
                    .common_attrs
                    .status
                    .as_ref()
                    .expect("caller ensured that the status code is present on the struct");

                quote! {
                    let mut output = HashMap::<StatusCode, Vec<String>>::default();
                    output.entry(#status_code.http_code()).or_default()
                        .push(#description.to_owned());
                    output
                }
            }
        };

        Ok(quote! {
            fn response_descriptions() -> HashMap<StatusCode, Vec<String>> {
                #method_body
            }
        })
    }

    fn add_details_method(&self) -> Result<TokenStream> {
        let status_ident = Ident::new("status", Span::call_site());
        let serialize_details = self
            .0
            .details
            .as_ref()
            .map(|d| d.serialize.is_present())
            .unwrap_or(false);

        let method_body = match (serialize_details, &self.0.data) {
            (true, _) => {
                quote! {
                    let Some(error_info) = #status_ident.error_info_mut() else {
                        tracing::error!(?status, "missing error info on status object");
                        return;
                    };

                    match serde_json::to_value(&self) {
                        Ok(serde_json::value::Value::Object(serialized)) => {
                            error_info.metadata_mut().extend(serialized);
                        },
                        Ok(_) => {
                            tracing::error!(?self, "errors must serialize to json objects");
                        },
                        Err(error) => {
                            tracing::error!(?self, %error, "serialization to json failed");
                        }
                    }
                }
            }
            (false, Data::Enum(_)) => {
                let match_arms = declare_match_arms(self.0, |variant| {
                    if variant.delegate.is_present() {
                        quote! { inner.add_details(status) }
                    } else {
                        quote! { () }
                    }
                });

                quote! {
                    match self { #match_arms }
                }
            }

            (false, Data::Struct(_)) => TokenStream::new(),
        };

        Ok(quote! {
            fn add_details(&self, status: &mut Status) {
                #method_body
            }
        })
    }
}

impl ToTokens for DeriveRestApiErrorTrait<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let mut parse_errors = Error::accumulator();

        let name = &self.0.ident;
        let status_method = parse_errors.handle(self.status_method());
        let domain_method = parse_errors.handle(self.domain_method());
        let reason_method = parse_errors.handle(self.reason_method());
        let add_details_method = parse_errors.handle(self.add_details_method());
        let descriptions_method = parse_errors.handle(self.response_descriptions_method());

        if let Err(error) = parse_errors.finish() {
            tokens.extend(error.write_errors());
        } else {
            tokens.extend(quote! {
                impl crate::common::api::RestApiError for #name {
                    #status_method
                    #domain_method
                    #reason_method
                    #add_details_method
                    #descriptions_method
                }
            });
        }
    }
}

struct DeriveIntoResponseTrait<'a>(&'a ParsedTokens);

impl ToTokens for DeriveIntoResponseTrait<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let enum_ident = &self.0.ident;

        tokens.extend(quote! {
            impl IntoResponse for #enum_ident {
                fn into_response(self) -> Response {
                    let mut response = self.to_response();

                    if self.status_code() == ApiStatusCode::Internal {
                        response.extensions_mut().insert(
                            crate::common::telemetry::InternalError(Arc::new(self))
                        );
                    }

                    response
                }
            }
        });
    }
}

struct DeriveIntoResponsesTrait<'a>(&'a ParsedTokens);

impl ToTokens for DeriveIntoResponsesTrait<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let enum_ident = &self.0.ident;

        tokens.extend(quote! {
            impl IntoResponses for #enum_ident {
                fn responses() -> BTreeMap<String, RefOr<OpenApiResponse>> {
                    crate::common::api::into_responses(Self::response_descriptions())
                }
            }
        })
    }
}

/// Helper function that generates match arms for enum variants.
///
/// Variants with the `delegate` field are treated differently. The provided function is used to
/// create the match body for each arm.
fn declare_match_arms<F>(parsed: &ParsedTokens, func: F) -> TokenStream
where
    F: Fn(&ErrorEnumVariant) -> TokenStream,
{
    let Data::Enum(variants) = &parsed.data else {
        panic!("called with a non-enum data variant");
    };

    let mut match_arms = TokenStream::new();

    for variant in variants {
        let ident = &variant.ident;
        let match_body = func(variant);

        let match_arm = match variant.fields.style {
            _ if variant.delegate.is_present() => quote! { Self::#ident(inner) => #match_body, },
            Style::Tuple => quote! { Self::#ident(_) => #match_body, },
            Style::Struct => quote! { Self::#ident{..} => #match_body, },
            Style::Unit => quote! { Self::#ident => #match_body, },
        };

        match_arms.extend(match_arm);
    }

    match_arms
}

/// Gets the all the `doc` attributes from a slice of attributes.
///
/// Returns the attributes as a single [TokenStream`] where their strings are concatenated with
/// `concat!` to form a single `&'static str`.
fn get_docstring(attrs: &[Attribute]) -> TokenStream {
    let description = attrs.iter().filter_map(|attr| {
        if let Some(ident) = attr.path().get_ident()
            && ident == "doc"
        {
            return attr.meta.require_name_value().ok().map(|v| &v.value);
        }
        None
    });

    quote! {
        concat!(#(#description),*).trim()
    }
}
