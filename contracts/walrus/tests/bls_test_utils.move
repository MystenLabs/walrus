// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::bls_test_utils;

use sui::bls12381::{Self, bls12381_min_pk_verify};

public fun bls_min_pk_sign(msg: &vector<u8>, sk: &vector<u8>): vector<u8> {
    let sk_element = bls12381::scalar_from_bytes(sk);
    let hashed_msg = bls12381::hash_to_g2(msg);
    let sig = bls12381::g2_mul(&sk_element, &hashed_msg);
    *sig.bytes()
}

public fun bls_min_pk_from_sk(sk: &vector<u8>): vector<u8> {
    let sk_element = bls12381::scalar_from_bytes(sk);
    let g1 = bls12381::g1_generator();
    let pk = bls12381::g1_mul(&sk_element, &g1);
    *pk.bytes()
}

#[test]
fun test_bls_pk() {
    let sk = x"0000000000000000000000000000000000000000000000000000000000000075";
    let pub_key_bytes = x"95eacc3adc09c827593f581e8e2de068bf4cf5d0c0eb29e5372f0d23364788ee0f9beb112c8a7e9c2f0c720433705cf0";
    assert!(bls_min_pk_from_sk(&sk) == pub_key_bytes)
}

#[test]
fun test_bls_sign() {
    let sk = x"0000000000000000000000000000000000000000000000000000000000000075";
    let pub_key_bytes = bls_min_pk_from_sk(&sk);
    let msg = x"deadbeef";
    let sig = bls_min_pk_sign(&msg, &sk);

    assert!(
        bls12381_min_pk_verify(
            &sig,
            &pub_key_bytes,
            &msg,
        ),
    );
}
