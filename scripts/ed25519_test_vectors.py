from nacl.signing import SigningKey

# Generate a new random signing key
signing_key = SigningKey.generate()

# Sign a message with the signing key
msg = b"Hello world!"
signed = signing_key.sign(msg)

# Obtain the verify key for a given signing key
verify_key = signing_key.verify_key

# Serialize the verify key to send it to a third party
verify_key_bytes = verify_key.encode()

print("Compat test\n\n")

print(f"let public_key = vector{list(verify_key_bytes)};")
print(f"let signature = vector{list(signed.signature)};")
print(f"let message = vector{list(msg)};")

print("Many signatures\n\n")

for i in range(10):
    signing_key = SigningKey.generate()
    signed = signing_key.sign(msg)
    verify_key = signing_key.verify_key
    verify_key_bytes = verify_key.encode()
    print(f"let public_key{i} = vector{list(verify_key_bytes)};")
    print(f"let signature{i} = vector{list(signed.signature)};")

print(f"let message{i} = vector{list(msg)};")

