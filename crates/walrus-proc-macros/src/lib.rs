// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! simtest proc macros for setting up walrus simtest.

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse::Parser, parse_macro_input, punctuated::Punctuated, ItemFn, Token};

/// This attribute macro is used to add `simtest_` prefix to all the tests annotated with
/// `[walrus_simtest]`, and annotate them with `[sim_test]` attribute.
#[proc_macro_attribute]
pub fn walrus_simtest(args: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let mut input = parse_macro_input!(item as ItemFn);

    let arg_parser = Punctuated::<syn::Meta, Token![,]>::parse_terminated;
    let args = arg_parser.parse(args).unwrap().into_iter();

    // Extract the function name
    let fn_name = &input.sig.ident;

    // Create the new name by adding "simtest_" prefix
    let new_fn_name = syn::Ident::new(&format!("simtest_{}", fn_name), fn_name.span());

    input.sig.ident = new_fn_name;
    let output = quote! {
        #[sui_macros::sim_test(#(#args)*)]
        #input
    };

    // Return the final tokens
    TokenStream::from(output)
}
