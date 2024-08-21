// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// editorconfig-checker-disable-file
// Data here autogenerated by python file

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module walrus::committee_cert_tests;

use sui::bls12381::bls12381_min_pk_verify;
use walrus::{
    bls_aggregate::new_bls_committee,
    messages,
    storage_node
};


const PUB_KEY_BYTES: vector<u8> = vector[142, 78, 70, 3, 179, 142, 145, 75, 170, 36, 5, 232, 153, 164, 205, 57, 24, 216, 208, 34, 87, 213, 225, 76, 5, 157, 212, 88, 161, 34, 75, 145, 206, 144, 85, 11, 197, 110, 75, 175, 215, 194, 78, 51, 192, 196, 59, 204];
const MESSAGE: vector<u8> = vector[1, 0, 3, 5, 0, 0, 0, 0, 0, 0, 0, 104, 101, 108, 108, 111];
const SIGNATURE: vector<u8>  = vector[173, 231, 27, 143, 41, 154, 49, 14, 85, 88, 187, 65, 86, 190, 161, 255, 219, 210, 78, 88, 179, 53, 11, 104, 168, 220, 40, 13, 91, 254, 191, 116, 161, 252, 196, 19, 24, 153, 126, 248, 68, 136, 245, 85, 144, 17, 163, 161, 10, 195, 145, 26, 88, 205, 255, 211, 19, 42, 132, 34, 230, 155, 148, 10, 173, 151, 182, 93, 50, 73, 126, 112, 119, 153, 116, 80, 198, 215, 82, 228, 9, 186, 90, 83, 85, 143, 155, 191, 109, 190, 84, 129, 178, 100, 228, 118];


#[test]
public fun test_basic_correct() {
    let pub_key_bytes = PUB_KEY_BYTES;
    let message = MESSAGE;
    let signature = SIGNATURE;

    assert!(
        bls12381_min_pk_verify(
            &signature,
            &pub_key_bytes,
            &message,
        ),
        0,
    );

    // Make a new committee
    let committee_at_5 = new_bls_committee(5, &vector[
        storage_node::new_for_testing(pub_key_bytes, 10),
    ]);

    let cert = committee_at_5.verify_quorum_in_epoch(signature, vector[0], message);

    assert!(messages::intent_type(&cert) == 1, 0);

}

#[test, expected_failure]
public fun test_incorrect_epoch() {
    let pub_key_bytes = PUB_KEY_BYTES;
    let message = MESSAGE;
    let signature = SIGNATURE;

    assert!(
        bls12381_min_pk_verify(
            &signature,
            &pub_key_bytes,
            &message,
        ),
        0,
    );

    // Make a new committee
    let committee_at_6 = new_bls_committee(6, &vector[
        storage_node::new_for_testing(pub_key_bytes, 10),
    ]);

    // Test fails here
    let _cert = committee_at_6.verify_quorum_in_epoch(signature, vector[0], message);

}
