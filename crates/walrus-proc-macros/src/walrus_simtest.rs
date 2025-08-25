// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, Token, parse::Parser, parse_macro_input, punctuated::Punctuated};

pub fn attribute_macro(args: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(item as ItemFn);

    let arg_parser = Punctuated::<syn::Meta, Token![,]>::parse_terminated;
    let args = arg_parser
        .parse(args)
        .expect("should be able to parse arguments")
        .into_iter();

    // If running simtests, add a "simtest_" prefix to the function name.
    let output = if cfg!(msim) {
        let mut input = input;
        let fn_name = &input.sig.ident;
        input.sig.ident = syn::Ident::new(&format!("simtest_{fn_name}"), fn_name.span());
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
