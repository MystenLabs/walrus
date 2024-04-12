# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

from py_ecc.bls import G2Basic

from py_ecc.optimized_bls12_381 import (
    G1,
    Z1,
    Z2,
    add,
    curve_order,
    final_exponentiate,
    multiply,
    neg,
    pairing,
)

from py_ecc.bls.g2_primitives import (
    G1_to_pubkey,
    G2_to_signature,
    is_inf,
    pubkey_to_G1,
    signature_to_G2,
    subgroup_check,
)

def aggregate_pk(PKs):
    aggregate = Z1  # Seed with the point at infinity
    for pk in PKs:
        pubkey_point = pubkey_to_G1(pk)
        aggregate = add(aggregate, pubkey_point)
    return G1_to_pubkey(aggregate)

# two new public keys
sk1 = 10931
pk1 = G2Basic.SkToPk(sk1)

sk2 = 12345
pk2 = G2Basic.SkToPk(sk2)

message = b"hello"
sig1 = G2Basic.Sign(sk1, message)
sig2 = G2Basic.Sign(sk2, message)

# Aggregate the signatures
agg_sig = G2Basic.Aggregate([sig1, sig2])

# Aggregate the public keys
agg_pk = aggregate_pk([pk1, pk2])

# Verify the aggregated signature
assert G2Basic.Verify(agg_pk, message, agg_sig)

# Verify the individual signatures
assert G2Basic.Verify(pk1, message, sig1)
assert G2Basic.Verify(pk2, message, sig2)

# Verify signature fails for a different message
assert not G2Basic.Verify(agg_pk, b"goodbye", agg_sig)

print("---- BASIC COMPAT ----")

print(f"let pk1 = vector{list(pk1)};")
print(f"let message = vector{list(message)};")
print(f"let sig1 = vector{list(sig1)};")

print("---- 7 out of 10 ----")

# Make 10 public keys and sign with 7 of them
# Aggregate the public keys and signatures

# Generate 10 public keys
pks = [G2Basic.SkToPk(1000+i) for i in range(10)]

# Sign with 7 of the public keys
sigs = [G2Basic.Sign(1000+i, message) for i in range(7)]

# Aggregate the public keys and signatures
agg_pk = aggregate_pk(pks[:7])
agg_sig = G2Basic.Aggregate(sigs)

# Verify the aggregated signature
assert G2Basic.Verify(agg_pk, message, agg_sig)

# Print all keys and aggregare signature
for i in range(10):
    print(f"let pk{i} = vector{list(pks[i])};")

print(f"let message = vector{list(message)};")
# print the aggregate signature
print(f"let agg_sig = vector{list(agg_sig)};")

print("---- Intent ----")

sk1 = 10931
pk1 = G2Basic.SkToPk(sk1)

message = bytes([1, 0, 3, 5, 0, 0, 0, 0, 0, 0, 0] +  list(b"hello"))
sig1 = G2Basic.Sign(sk1, message)

assert G2Basic.Verify(pk1, message, sig1)

print(f"let pub_key_bytes = vector{list(pk1)};")
print(f"let message = vector{list(message)};")
print(f"let signature = vector{list(sig1)};")


print("---- e2e test BLS key ----")

sk1 = 117
pk1 = G2Basic.SkToPk(sk1)

print(f"let secret_key_bytes = {sk1};")
print(f"let pub_key_bytes = vector{list(pk1)};")

# Invalid epoch message
invalid_message = bytes([2, 0, 3, 5, 0, 0, 0, 0, 0, 0, 0] +  [0xAB] * 32)
invalid_message_sig = G2Basic.Sign(sk1, invalid_message)

print("---- Invalid blob message ----")

print(f"let invalid_message = vector[{list(invalid_message)}];")
print(f"let message_signature = vector{list(invalid_message_sig)};")
