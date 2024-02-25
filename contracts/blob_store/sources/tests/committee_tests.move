
// editorconfig-checker-disable-file
// Data here autogenerated by python file

// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module blob_store::committee_tests {

    use std::vector;
    use std::string;
    use sui::ed25519;

    use blob_store::committee::{create_committee_cap, create_committee, create_storage_node_info, verify_quorum, Committee, ERROR_NOT_ENOUGH_STAKE, ERROR_SIG_VERIFICATION, ERROR_INCOMPATIBLE_LEN};

    #[test]
    public fun test_basic_ed25519_compat(){
        // See scripts/scripts/ed25519_test_vectors.py to generate this.
        let public_key = vector[39, 178, 38, 240, 6, 209, 9, 207, 103, 143, 56, 7, 24, 131, 24, 85, 182, 113, 226, 150, 196, 68, 1, 207, 241, 136, 103, 204, 41, 172, 13, 93];
        let signature = vector[198, 254, 213, 232, 62, 21, 137, 14, 63, 43, 203, 42, 163, 1, 168, 28, 48, 190, 214, 149, 150, 231, 171, 181, 46, 226, 78, 36, 72, 0, 143, 153, 1, 226, 166, 40, 215, 200, 122, 154, 249, 96, 57, 59, 187, 122, 249, 112, 75, 165, 170, 24, 209, 194, 192, 57, 202, 203, 196, 123, 242, 167, 94, 10];
        let message = vector[72, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 33];

        let verify = ed25519::ed25519_verify(&signature, &public_key, &message);
        assert!(verify == true, 0);
    }

    struct TESTTAG has drop {}

    public fun test_helper_committee() : Committee<TESTTAG> {
        // See scripts/scripts/ed25519_test_vectors.py to generate this.
        let public_key0 = vector[68, 55, 134, 213, 218, 180, 143, 4, 193, 136, 208, 148, 193, 16, 197, 130, 146, 135, 219, 177, 151, 8, 81, 121, 182, 80, 134, 226, 235, 136, 48, 185];
        let public_key1 = vector[216, 112, 157, 99, 123, 125, 125, 14, 124, 62, 104, 135, 209, 21, 143, 60, 50, 147, 89, 218, 166, 253, 55, 93, 35, 204, 51, 20, 148, 183, 134, 95];
        let public_key2 = vector[20, 113, 106, 74, 71, 199, 77, 85, 28, 57, 43, 247, 148, 101, 182, 88, 86, 69, 166, 145, 123, 50, 145, 178, 214, 188, 26, 249, 224, 168, 230, 104];
        let public_key3 = vector[252, 190, 136, 101, 74, 168, 146, 78, 53, 14, 222, 5, 64, 249, 109, 35, 239, 61, 143, 171, 41, 100, 24, 161, 56, 31, 54, 185, 146, 170, 234, 118];
        let public_key4 = vector[165, 59, 230, 200, 4, 110, 33, 146, 236, 0, 179, 79, 248, 231, 13, 101, 234, 195, 35, 85, 144, 55, 95, 110, 206, 204, 142, 92, 24, 238, 241, 45];
        let public_key5 = vector[3, 232, 3, 200, 204, 104, 150, 201, 51, 240, 50, 38, 61, 207, 21, 50, 117, 19, 139, 205, 220, 26, 184, 29, 209, 178, 112, 255, 3, 145, 144, 57];
        let public_key6 = vector[97, 157, 33, 50, 24, 212, 136, 111, 173, 1, 75, 127, 108, 25, 53, 114, 105, 77, 200, 98, 248, 24, 170, 183, 124, 198, 161, 154, 159, 199, 212, 136];
        let public_key7 = vector[175, 230, 214, 17, 146, 119, 210, 162, 106, 189, 133, 101, 19, 74, 226, 217, 115, 175, 231, 184, 228, 10, 243, 206, 30, 106, 63, 83, 127, 60, 94, 101];
        let public_key8 = vector[91, 0, 34, 91, 161, 169, 82, 155, 197, 54, 37, 66, 38, 21, 205, 166, 201, 233, 212, 17, 84, 174, 63, 67, 143, 134, 20, 113, 254, 95, 89, 139];
        let public_key9 = vector[11, 29, 226, 175, 89, 211, 159, 15, 167, 10, 161, 204, 145, 194, 225, 89, 128, 23, 110, 107, 152, 238, 198, 1, 103, 95, 99, 231, 200, 164, 163, 203];

                // Create a create committee capability
        let tag = TESTTAG{};
        let cap = create_committee_cap(tag);

        let node_vec = vector::empty();
        vector::push_back(&mut node_vec, create_storage_node_info(string::utf8(b"node0"), string::utf8(b"addr0"), public_key0, vector[0]));
        vector::push_back(&mut node_vec, create_storage_node_info(string::utf8(b"node1"), string::utf8(b"addr1"), public_key1, vector[1]));
        vector::push_back(&mut node_vec, create_storage_node_info(string::utf8(b"node2"), string::utf8(b"addr2"), public_key2, vector[2]));
        vector::push_back(&mut node_vec, create_storage_node_info(string::utf8(b"node3"), string::utf8(b"addr3"), public_key3, vector[3]));
        vector::push_back(&mut node_vec, create_storage_node_info(string::utf8(b"node4"), string::utf8(b"addr4"), public_key4, vector[4]));
        vector::push_back(&mut node_vec, create_storage_node_info(string::utf8(b"node5"), string::utf8(b"addr5"), public_key5, vector[5]));
        vector::push_back(&mut node_vec, create_storage_node_info(string::utf8(b"node6"), string::utf8(b"addr6"), public_key6, vector[6]));
        vector::push_back(&mut node_vec, create_storage_node_info(string::utf8(b"node7"), string::utf8(b"addr7"), public_key7, vector[7]));
        vector::push_back(&mut node_vec, create_storage_node_info(string::utf8(b"node8"), string::utf8(b"addr8"), public_key8, vector[8]));
        vector::push_back(&mut node_vec, create_storage_node_info(string::utf8(b"node9"), string::utf8(b"addr9"), public_key9, vector[9]));


        // Create a new committee
        let committee = create_committee(&cap, 0, 10, node_vec);
        committee

    }

    #[test]
    public fun test_full_quorum_ok() : Committee<TESTTAG>{
        // See scripts/scripts/ed25519_test_vectors.py to generate this.

        let signature0 = vector[156, 204, 17, 236, 132, 244, 7, 215, 122, 142, 85, 86, 181, 51, 33, 210, 189, 181, 129, 24, 246, 58, 8, 190, 102, 192, 21, 150, 251, 76, 191, 240, 78, 174, 53, 90, 58, 97, 64, 243, 208, 221, 182, 233, 232, 136, 116, 30, 170, 218, 7, 153, 176, 73, 41, 145, 33, 228, 182, 168, 216, 64, 89, 3];
        let signature1 = vector[65, 43, 147, 173, 21, 174, 246, 4, 89, 112, 199, 5, 17, 251, 236, 248, 235, 62, 127, 230, 190, 202, 193, 77, 96, 112, 94, 140, 254, 119, 223, 150, 60, 125, 117, 45, 136, 1, 108, 185, 133, 170, 60, 248, 136, 159, 93, 187, 117, 77, 224, 94, 238, 199, 26, 8, 85, 240, 81, 28, 22, 41, 13, 10];
        let signature2 = vector[29, 11, 192, 105, 57, 197, 106, 23, 59, 8, 21, 154, 241, 91, 123, 70, 189, 207, 104, 48, 177, 30, 33, 182, 124, 182, 52, 144, 76, 185, 98, 174, 149, 210, 152, 246, 184, 53, 176, 16, 74, 179, 250, 26, 154, 204, 77, 99, 204, 35, 40, 167, 189, 15, 128, 129, 145, 89, 76, 201, 210, 7, 133, 9];
        let signature3 = vector[175, 135, 101, 28, 250, 115, 124, 93, 86, 87, 65, 109, 92, 177, 132, 212, 37, 89, 193, 9, 89, 139, 156, 17, 15, 246, 177, 110, 51, 226, 187, 143, 57, 250, 144, 149, 118, 39, 10, 19, 237, 249, 253, 251, 119, 98, 18, 115, 160, 102, 241, 206, 187, 122, 247, 12, 58, 70, 128, 188, 108, 101, 225, 15];
        let signature4 = vector[57, 177, 100, 122, 101, 216, 4, 106, 250, 38, 203, 163, 1, 96, 189, 206, 14, 15, 42, 251, 60, 202, 250, 104, 217, 157, 21, 48, 128, 249, 118, 215, 13, 77, 33, 220, 83, 37, 231, 97, 114, 243, 27, 238, 230, 43, 51, 93, 30, 184, 182, 22, 165, 253, 194, 26, 56, 10, 12, 147, 125, 142, 83, 0];
        let signature5 = vector[201, 229, 101, 40, 122, 95, 9, 247, 131, 66, 11, 89, 2, 242, 5, 149, 100, 148, 8, 16, 47, 122, 78, 97, 157, 81, 165, 180, 47, 77, 54, 29, 84, 114, 24, 39, 114, 38, 38, 116, 191, 190, 215, 113, 213, 223, 207, 200, 170, 225, 117, 45, 159, 147, 89, 184, 238, 103, 176, 54, 244, 4, 8, 9];
        let signature6 = vector[157, 162, 81, 251, 53, 243, 63, 164, 216, 100, 168, 92, 36, 134, 155, 255, 44, 133, 83, 37, 136, 104, 196, 70, 16, 197, 55, 127, 195, 32, 139, 166, 161, 58, 236, 60, 97, 65, 98, 28, 248, 121, 243, 75, 116, 91, 208, 141, 22, 239, 131, 177, 156, 69, 199, 150, 70, 169, 155, 225, 63, 162, 203, 5];
        let signature7 = vector[247, 113, 118, 96, 21, 222, 208, 156, 66, 102, 80, 72, 254, 26, 97, 239, 1, 85, 186, 226, 161, 151, 36, 25, 14, 111, 195, 207, 140, 80, 103, 179, 72, 208, 111, 119, 46, 132, 57, 121, 143, 55, 219, 194, 241, 165, 12, 11, 160, 253, 196, 249, 246, 172, 38, 156, 225, 89, 74, 167, 38, 239, 249, 11];
        let signature8 = vector[234, 38, 20, 214, 67, 49, 72, 141, 21, 206, 154, 95, 6, 249, 141, 158, 197, 126, 179, 192, 81, 2, 17, 244, 241, 15, 72, 51, 189, 142, 47, 31, 26, 139, 226, 234, 141, 184, 142, 139, 228, 151, 123, 45, 31, 238, 166, 165, 111, 23, 24, 112, 184, 240, 152, 192, 104, 52, 255, 83, 153, 184, 125, 8];
        let signature9 = vector[217, 163, 42, 206, 1, 78, 253, 237, 188, 161, 78, 77, 27, 245, 179, 179, 215, 82, 65, 6, 80, 234, 191, 204, 198, 81, 160, 49, 236, 192, 196, 114, 245, 82, 15, 243, 107, 222, 140, 123, 94, 237, 173, 104, 128, 156, 234, 36, 168, 251, 123, 150, 244, 148, 109, 117, 206, 186, 205, 79, 13, 64, 122, 4];

        let message = vector[72, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 33];

        // Create a new committee
        let committee = test_helper_committee();

        let signatures = vector[signature0, signature1, signature2, signature3, signature4, signature5, signature6, signature7, signature8, signature9];
        assert!(verify_quorum(&committee, &message, &vector[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], &signatures) == 10, 0);

        committee
    }

    #[test]
    public fun test_min_quorum_ok() : Committee<TESTTAG>{
        // See scripts/scripts/ed25519_test_vectors.py to generate this.

        let signature0 = vector[156, 204, 17, 236, 132, 244, 7, 215, 122, 142, 85, 86, 181, 51, 33, 210, 189, 181, 129, 24, 246, 58, 8, 190, 102, 192, 21, 150, 251, 76, 191, 240, 78, 174, 53, 90, 58, 97, 64, 243, 208, 221, 182, 233, 232, 136, 116, 30, 170, 218, 7, 153, 176, 73, 41, 145, 33, 228, 182, 168, 216, 64, 89, 3];
        let signature1 = vector[65, 43, 147, 173, 21, 174, 246, 4, 89, 112, 199, 5, 17, 251, 236, 248, 235, 62, 127, 230, 190, 202, 193, 77, 96, 112, 94, 140, 254, 119, 223, 150, 60, 125, 117, 45, 136, 1, 108, 185, 133, 170, 60, 248, 136, 159, 93, 187, 117, 77, 224, 94, 238, 199, 26, 8, 85, 240, 81, 28, 22, 41, 13, 10];
        let signature2 = vector[29, 11, 192, 105, 57, 197, 106, 23, 59, 8, 21, 154, 241, 91, 123, 70, 189, 207, 104, 48, 177, 30, 33, 182, 124, 182, 52, 144, 76, 185, 98, 174, 149, 210, 152, 246, 184, 53, 176, 16, 74, 179, 250, 26, 154, 204, 77, 99, 204, 35, 40, 167, 189, 15, 128, 129, 145, 89, 76, 201, 210, 7, 133, 9];
        let signature3 = vector[175, 135, 101, 28, 250, 115, 124, 93, 86, 87, 65, 109, 92, 177, 132, 212, 37, 89, 193, 9, 89, 139, 156, 17, 15, 246, 177, 110, 51, 226, 187, 143, 57, 250, 144, 149, 118, 39, 10, 19, 237, 249, 253, 251, 119, 98, 18, 115, 160, 102, 241, 206, 187, 122, 247, 12, 58, 70, 128, 188, 108, 101, 225, 15];
        let signature4 = vector[57, 177, 100, 122, 101, 216, 4, 106, 250, 38, 203, 163, 1, 96, 189, 206, 14, 15, 42, 251, 60, 202, 250, 104, 217, 157, 21, 48, 128, 249, 118, 215, 13, 77, 33, 220, 83, 37, 231, 97, 114, 243, 27, 238, 230, 43, 51, 93, 30, 184, 182, 22, 165, 253, 194, 26, 56, 10, 12, 147, 125, 142, 83, 0];
        let signature5 = vector[201, 229, 101, 40, 122, 95, 9, 247, 131, 66, 11, 89, 2, 242, 5, 149, 100, 148, 8, 16, 47, 122, 78, 97, 157, 81, 165, 180, 47, 77, 54, 29, 84, 114, 24, 39, 114, 38, 38, 116, 191, 190, 215, 113, 213, 223, 207, 200, 170, 225, 117, 45, 159, 147, 89, 184, 238, 103, 176, 54, 244, 4, 8, 9];
        let signature6 = vector[157, 162, 81, 251, 53, 243, 63, 164, 216, 100, 168, 92, 36, 134, 155, 255, 44, 133, 83, 37, 136, 104, 196, 70, 16, 197, 55, 127, 195, 32, 139, 166, 161, 58, 236, 60, 97, 65, 98, 28, 248, 121, 243, 75, 116, 91, 208, 141, 22, 239, 131, 177, 156, 69, 199, 150, 70, 169, 155, 225, 63, 162, 203, 5];

        let message = vector[72, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 33];

        // Create a new committee
        let committee = test_helper_committee();

        let signatures = vector[signature0, signature1, signature2, signature3, signature4, signature5, signature6];
        assert!(verify_quorum(&committee, &message, &vector[0, 1, 2, 3, 4, 5, 6], &signatures) == 7, 0);

        committee
    }



    #[test, expected_failure(abort_code = ERROR_NOT_ENOUGH_STAKE)]
    public fun test_quorum_stake_fail() : Committee<TESTTAG>{
        // See scripts/scripts/ed25519_test_vectors.py to generate this.

        let signature0 = vector[156, 204, 17, 236, 132, 244, 7, 215, 122, 142, 85, 86, 181, 51, 33, 210, 189, 181, 129, 24, 246, 58, 8, 190, 102, 192, 21, 150, 251, 76, 191, 240, 78, 174, 53, 90, 58, 97, 64, 243, 208, 221, 182, 233, 232, 136, 116, 30, 170, 218, 7, 153, 176, 73, 41, 145, 33, 228, 182, 168, 216, 64, 89, 3];
        let signature1 = vector[65, 43, 147, 173, 21, 174, 246, 4, 89, 112, 199, 5, 17, 251, 236, 248, 235, 62, 127, 230, 190, 202, 193, 77, 96, 112, 94, 140, 254, 119, 223, 150, 60, 125, 117, 45, 136, 1, 108, 185, 133, 170, 60, 248, 136, 159, 93, 187, 117, 77, 224, 94, 238, 199, 26, 8, 85, 240, 81, 28, 22, 41, 13, 10];
        let signature2 = vector[29, 11, 192, 105, 57, 197, 106, 23, 59, 8, 21, 154, 241, 91, 123, 70, 189, 207, 104, 48, 177, 30, 33, 182, 124, 182, 52, 144, 76, 185, 98, 174, 149, 210, 152, 246, 184, 53, 176, 16, 74, 179, 250, 26, 154, 204, 77, 99, 204, 35, 40, 167, 189, 15, 128, 129, 145, 89, 76, 201, 210, 7, 133, 9];
        let signature3 = vector[175, 135, 101, 28, 250, 115, 124, 93, 86, 87, 65, 109, 92, 177, 132, 212, 37, 89, 193, 9, 89, 139, 156, 17, 15, 246, 177, 110, 51, 226, 187, 143, 57, 250, 144, 149, 118, 39, 10, 19, 237, 249, 253, 251, 119, 98, 18, 115, 160, 102, 241, 206, 187, 122, 247, 12, 58, 70, 128, 188, 108, 101, 225, 15];
        let signature4 = vector[57, 177, 100, 122, 101, 216, 4, 106, 250, 38, 203, 163, 1, 96, 189, 206, 14, 15, 42, 251, 60, 202, 250, 104, 217, 157, 21, 48, 128, 249, 118, 215, 13, 77, 33, 220, 83, 37, 231, 97, 114, 243, 27, 238, 230, 43, 51, 93, 30, 184, 182, 22, 165, 253, 194, 26, 56, 10, 12, 147, 125, 142, 83, 0];
        let signature5 = vector[201, 229, 101, 40, 122, 95, 9, 247, 131, 66, 11, 89, 2, 242, 5, 149, 100, 148, 8, 16, 47, 122, 78, 97, 157, 81, 165, 180, 47, 77, 54, 29, 84, 114, 24, 39, 114, 38, 38, 116, 191, 190, 215, 113, 213, 223, 207, 200, 170, 225, 117, 45, 159, 147, 89, 184, 238, 103, 176, 54, 244, 4, 8, 9];

        let message = vector[72, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 33];

        // Create a new committee
        let committee = test_helper_committee();

        let signatures = vector[signature0, signature1, signature2, signature3, signature4, signature5];
        assert!(verify_quorum(&committee, &message, &vector[0, 1, 2, 3, 4, 5], &signatures) == 6, 0);

        committee

    }

    #[test, expected_failure(abort_code = ERROR_SIG_VERIFICATION)]
    public fun test_quorum_sig_fail() : Committee<TESTTAG>{
        // See scripts/scripts/ed25519_test_vectors.py to generate this.

        let signature0 = vector[156, 204, 17, 236, 132, 244, 7, 215, 122, 142, 85, 86, 181, 51, 33, 210, 189, 181, 129, 24, 246, 58, 8, 190, 102, 192, 21, 150, 251, 76, 191, 240, 78, 174, 53, 90, 58, 97, 64, 243, 208, 221, 182, 233, 232, 136, 116, 30, 170, 218, 7, 153, 176, 73, 41, 145, 33, 228, 182, 168, 216, 64, 89, 3];
        let signature1 = vector[65, 43, 147, 173, 21, 174, 246, 4, 89, 112, 199, 5, 17, 251, 236, 248, 235, 62, 127, 230, 190, 202, 193, 77, 96, 112, 94, 140, 254, 119, 223, 150, 60, 125, 117, 45, 136, 1, 108, 185, 133, 170, 60, 248, 136, 159, 93, 187, 117, 77, 224, 94, 238, 199, 26, 8, 85, 240, 81, 28, 22, 41, 13, 10];
        let signature2 = vector[29, 11, 192, 105, 57, 197, 106, 23, 59, 8, 21, 154, 241, 91, 123, 70, 189, 207, 104, 48, 177, 30, 33, 182, 124, 182, 52, 144, 76, 185, 98, 174, 149, 210, 152, 246, 184, 53, 176, 16, 74, 179, 250, 26, 154, 204, 77, 99, 204, 35, 40, 167, 189, 15, 128, 129, 145, 89, 76, 201, 210, 7, 133, 9];
        let signature3 = vector[175, 135, 101, 28, 250, 115, 124, 93, 86, 87, 65, 109, 92, 177, 132, 212, 37, 89, 193, 9, 89, 139, 156, 17, 15, 246, 177, 110, 51, 226, 187, 143, 57, 250, 144, 149, 118, 39, 10, 19, 237, 249, 253, 251, 119, 98, 18, 115, 160, 102, 241, 206, 187, 122, 247, 12, 58, 70, 128, 188, 108, 101, 225, 15];
        let signature4 = vector[57, 177, 100, 122, 101, 216, 4, 106, 250, 38, 203, 163, 1, 96, 189, 206, 14, 15, 42, 251, 60, 202, 250, 104, 217, 157, 21, 48, 128, 249, 118, 215, 13, 77, 33, 220, 83, 37, 231, 97, 114, 243, 27, 238, 230, 43, 51, 93, 30, 184, 182, 22, 165, 253, 194, 26, 56, 10, 12, 147, 125, 142, 83, 0];
        let signature5 = vector[201, 229, 101, 40, 122, 95, 9, 247, 131, 66, 11, 89, 2, 242, 5, 149, 100, 148, 8, 16, 47, 122, 78, 97, 157, 81, 165, 180, 47, 77, 54, 29, 84, 114, 24, 39, 114, 38, 38, 116, 191, 190, 215, 113, 213, 223, 207, 200, 170, 225, 117, 45, 159, 147, 89, 184, 238, 103, 176, 54, 244, 4, 8, 9];
        let signature6 = vector[157, 162, 81, 251, 53, 243, 63, 164, 216, 100, 168, 92, 36, 134, 155, 255, 44, 133, 83, 37, 136, 104, 196, 70, 16, 197, 55, 127, 195, 32, 139, 166, 161, 58, 236, 60, 97, 65, 98, 28, 248, 121, 243, 75, 116, 91, 208, 141, 22, 239, 131, 177, 156, 69, 199, 150, 70, 169, 155, 225, 63, 162, 203, 5];

        let message = vector[72, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 33];

        // Create a new committee
        let committee = test_helper_committee();

        // Note we switch sig4 and sig5
        let signatures = vector[signature0, signature1, signature2, signature3, signature5, signature4, signature6];
        assert!(verify_quorum(&committee, &message, &vector[0, 1, 2, 3, 4, 5, 6], &signatures) == 6, 0);

        committee
    }

        #[test, expected_failure]
    public fun test_quorum_empty_fail() : Committee<TESTTAG>{
        // See scripts/scripts/ed25519_test_vectors.py to generate this.
        let message = vector[72, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 33];

        let committee = test_helper_committee();
        assert!(verify_quorum(&committee, &message, &vector[], &vector[]) == 6, 0);
        committee
    }

    #[test, expected_failure(abort_code = ERROR_INCOMPATIBLE_LEN)]
    public fun test_quorum_incompatible_len_fail() : Committee<TESTTAG>{
        // See scripts/scripts/ed25519_test_vectors.py to generate this.

        let signature0 = vector[156, 204, 17, 236, 132, 244, 7, 215, 122, 142, 85, 86, 181, 51, 33, 210, 189, 181, 129, 24, 246, 58, 8, 190, 102, 192, 21, 150, 251, 76, 191, 240, 78, 174, 53, 90, 58, 97, 64, 243, 208, 221, 182, 233, 232, 136, 116, 30, 170, 218, 7, 153, 176, 73, 41, 145, 33, 228, 182, 168, 216, 64, 89, 3];
        let signature1 = vector[65, 43, 147, 173, 21, 174, 246, 4, 89, 112, 199, 5, 17, 251, 236, 248, 235, 62, 127, 230, 190, 202, 193, 77, 96, 112, 94, 140, 254, 119, 223, 150, 60, 125, 117, 45, 136, 1, 108, 185, 133, 170, 60, 248, 136, 159, 93, 187, 117, 77, 224, 94, 238, 199, 26, 8, 85, 240, 81, 28, 22, 41, 13, 10];
        let signature2 = vector[29, 11, 192, 105, 57, 197, 106, 23, 59, 8, 21, 154, 241, 91, 123, 70, 189, 207, 104, 48, 177, 30, 33, 182, 124, 182, 52, 144, 76, 185, 98, 174, 149, 210, 152, 246, 184, 53, 176, 16, 74, 179, 250, 26, 154, 204, 77, 99, 204, 35, 40, 167, 189, 15, 128, 129, 145, 89, 76, 201, 210, 7, 133, 9];
        let signature3 = vector[175, 135, 101, 28, 250, 115, 124, 93, 86, 87, 65, 109, 92, 177, 132, 212, 37, 89, 193, 9, 89, 139, 156, 17, 15, 246, 177, 110, 51, 226, 187, 143, 57, 250, 144, 149, 118, 39, 10, 19, 237, 249, 253, 251, 119, 98, 18, 115, 160, 102, 241, 206, 187, 122, 247, 12, 58, 70, 128, 188, 108, 101, 225, 15];
        let signature4 = vector[57, 177, 100, 122, 101, 216, 4, 106, 250, 38, 203, 163, 1, 96, 189, 206, 14, 15, 42, 251, 60, 202, 250, 104, 217, 157, 21, 48, 128, 249, 118, 215, 13, 77, 33, 220, 83, 37, 231, 97, 114, 243, 27, 238, 230, 43, 51, 93, 30, 184, 182, 22, 165, 253, 194, 26, 56, 10, 12, 147, 125, 142, 83, 0];
        let signature5 = vector[201, 229, 101, 40, 122, 95, 9, 247, 131, 66, 11, 89, 2, 242, 5, 149, 100, 148, 8, 16, 47, 122, 78, 97, 157, 81, 165, 180, 47, 77, 54, 29, 84, 114, 24, 39, 114, 38, 38, 116, 191, 190, 215, 113, 213, 223, 207, 200, 170, 225, 117, 45, 159, 147, 89, 184, 238, 103, 176, 54, 244, 4, 8, 9];
        let signature6 = vector[157, 162, 81, 251, 53, 243, 63, 164, 216, 100, 168, 92, 36, 134, 155, 255, 44, 133, 83, 37, 136, 104, 196, 70, 16, 197, 55, 127, 195, 32, 139, 166, 161, 58, 236, 60, 97, 65, 98, 28, 248, 121, 243, 75, 116, 91, 208, 141, 22, 239, 131, 177, 156, 69, 199, 150, 70, 169, 155, 225, 63, 162, 203, 5];

        let message = vector[72, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 33];

        // Create a new committee
        let committee = test_helper_committee();

        let signatures = vector[signature0, signature1, signature2, signature3, signature4, signature5, signature6];
        assert!(verify_quorum(&committee, &message, &vector[0, 1, 2, 3, 4, 5], &signatures) == 7, 0);

        committee

    }

    #[test, expected_failure]
    public fun test_quorum_out_of_bands_fail() : Committee<TESTTAG>{
        // See scripts/scripts/ed25519_test_vectors.py to generate this.

        let signature0 = vector[156, 204, 17, 236, 132, 244, 7, 215, 122, 142, 85, 86, 181, 51, 33, 210, 189, 181, 129, 24, 246, 58, 8, 190, 102, 192, 21, 150, 251, 76, 191, 240, 78, 174, 53, 90, 58, 97, 64, 243, 208, 221, 182, 233, 232, 136, 116, 30, 170, 218, 7, 153, 176, 73, 41, 145, 33, 228, 182, 168, 216, 64, 89, 3];
        let signature1 = vector[65, 43, 147, 173, 21, 174, 246, 4, 89, 112, 199, 5, 17, 251, 236, 248, 235, 62, 127, 230, 190, 202, 193, 77, 96, 112, 94, 140, 254, 119, 223, 150, 60, 125, 117, 45, 136, 1, 108, 185, 133, 170, 60, 248, 136, 159, 93, 187, 117, 77, 224, 94, 238, 199, 26, 8, 85, 240, 81, 28, 22, 41, 13, 10];
        let signature2 = vector[29, 11, 192, 105, 57, 197, 106, 23, 59, 8, 21, 154, 241, 91, 123, 70, 189, 207, 104, 48, 177, 30, 33, 182, 124, 182, 52, 144, 76, 185, 98, 174, 149, 210, 152, 246, 184, 53, 176, 16, 74, 179, 250, 26, 154, 204, 77, 99, 204, 35, 40, 167, 189, 15, 128, 129, 145, 89, 76, 201, 210, 7, 133, 9];
        let signature3 = vector[175, 135, 101, 28, 250, 115, 124, 93, 86, 87, 65, 109, 92, 177, 132, 212, 37, 89, 193, 9, 89, 139, 156, 17, 15, 246, 177, 110, 51, 226, 187, 143, 57, 250, 144, 149, 118, 39, 10, 19, 237, 249, 253, 251, 119, 98, 18, 115, 160, 102, 241, 206, 187, 122, 247, 12, 58, 70, 128, 188, 108, 101, 225, 15];
        let signature4 = vector[57, 177, 100, 122, 101, 216, 4, 106, 250, 38, 203, 163, 1, 96, 189, 206, 14, 15, 42, 251, 60, 202, 250, 104, 217, 157, 21, 48, 128, 249, 118, 215, 13, 77, 33, 220, 83, 37, 231, 97, 114, 243, 27, 238, 230, 43, 51, 93, 30, 184, 182, 22, 165, 253, 194, 26, 56, 10, 12, 147, 125, 142, 83, 0];
        let signature5 = vector[201, 229, 101, 40, 122, 95, 9, 247, 131, 66, 11, 89, 2, 242, 5, 149, 100, 148, 8, 16, 47, 122, 78, 97, 157, 81, 165, 180, 47, 77, 54, 29, 84, 114, 24, 39, 114, 38, 38, 116, 191, 190, 215, 113, 213, 223, 207, 200, 170, 225, 117, 45, 159, 147, 89, 184, 238, 103, 176, 54, 244, 4, 8, 9];
        let signature6 = vector[157, 162, 81, 251, 53, 243, 63, 164, 216, 100, 168, 92, 36, 134, 155, 255, 44, 133, 83, 37, 136, 104, 196, 70, 16, 197, 55, 127, 195, 32, 139, 166, 161, 58, 236, 60, 97, 65, 98, 28, 248, 121, 243, 75, 116, 91, 208, 141, 22, 239, 131, 177, 156, 69, 199, 150, 70, 169, 155, 225, 63, 162, 203, 5];

        let message = vector[72, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 33];

        // Create a new committee
        let committee = test_helper_committee();

        let signatures = vector[signature0, signature1, signature2, signature3, signature4, signature5, signature6];
        assert!(verify_quorum(&committee, &message, &vector[0, 1, 2, 3, 4, 5, 6], &signatures) == 7, 0);

        // -- fail after this --
        assert!(verify_quorum(&committee, &message, &vector[0, 1, 2, 3, 100, 5, 6], &signatures) == 7, 0);


        committee

    }


}
