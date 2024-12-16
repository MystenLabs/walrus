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
    let input = parse_macro_input!(item as ItemFn);

    let arg_parser = Punctuated::<syn::Meta, Token![,]>::parse_terminated;
    let args = arg_parser.parse(args).unwrap().into_iter();

    // If running simtests, add a "simtest_" prefix to the function name.
    let output = if cfg!(msim) {
        let mut input = input;
        let fn_name = &input.sig.ident;
        input.sig.ident = syn::Ident::new(&format!("simtest_{}", fn_name), fn_name.span());
        quote! {
            #[sui_macros::sim_test(#(#args),*)]
            #input
        }
    } else {
        quote! {
            #[tokio::test(#(#args),*)]
            #input
        }
    };

    // Return the final tokens
    TokenStream::from(output)
}
