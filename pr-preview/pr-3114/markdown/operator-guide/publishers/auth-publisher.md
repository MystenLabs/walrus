The authenticated publisher requires HTTP requests to store a blob to be authenticated. Use an authenticated publisher as a building block for services that need to store data over HTTP on Walrus Mainnet, where an open publisher is undesirable because of the SUI and WAL cost of publishing to Walrus.

You configure the Walrus publisher to require a JSON Web Token (JWT) with each HTTP request for user authentication. The authentication system ensures that only authorized clients can store blobs and allows fine-grained control over storage parameters through JWT claims.

The authenticated publishing flow works as follows:

1. **Publisher setup:** You fund the publisher wallet with sufficient SUI and WAL, and configure the publisher to only accept authenticated requests. This involves setting the algorithm to authenticate JWTs, the expiration time for JWTs, and the JWT authentication secret.

1. **Authentication channel setup:** You set up a channel through which users can obtain JWT tokens. You can perform this step in any way that produces a valid JWT. This implementation does not provide this channel.

1. **Client authentication:** The client obtains a JWT token from the channel set up in the previous step. The JWT token can specify Walrus-relevant constraints, such as the maximum number of epochs the JWT can be used to store for and the maximum size of the blobs being stored.

1. **Publish request:** The client requests to store a blob using the publisher. The client sends an HTTP PUT request containing the JWT in an Authorization Bearer HTTP header.

1. **Store to Walrus:** The publisher checks the JWT, and checks that the store request complies with the constraints specified in the JWT. For example, the publisher verifies that the blob being stored is smaller than the authorized maximum size.

1. **Asset return (optional):** If specified, the publisher returns the newly created `Blob` object to the Sui address set in the request.

## Understand the authentication flow

The authenticated publisher enables web apps to let users upload files to Walrus without requiring them to have a wallet. The following example demonstrates a typical workflow where users authenticate through the web app using credentials like username and password rather than a blockchain wallet.

### User authentication and upload request

The user logs into the web app and selects a file to upload. The user chooses how many epochs to store the file for. The web app frontend sends the file size and epoch count to the backend.

### Backend authorization and JWT generation

The backend verifies the user is authorized to store this amount of data for the requested duration. It checks the following:

- User quota (and reduces it if needed)
- Cost of the upload in SUI and WAL
- Whether the cost fits within the budget of the user

You can perform this accounting locally or directly on Sui. If authorized, the backend generates a JWT token containing the approved size and epoch limits, then sends it to the frontend.

### Publisher request and verification

The frontend sends the file to the publisher using a PUT request with the JWT in the Authorization Bearer header. The publisher verifies the following:

- Token signature matches the configured secret.
- Token has not expired.
- Token has not been used before (this prevents replay attacks).
- Upload parameters match the JWT claims (if verification is enabled).

### Storage and object return

If all checks pass, the publisher stores the file on Walrus. If configured, the publisher returns the created `Blob` object to the specified Sui address.

:::info

Each JWT token can only be used once. After storing 1 file, the replay suppression system rejects the token. Do not use JWT tokens for accounting purposes.

:::

## Set up the publisher

Configure the publisher at startup using the following command line arguments:

- `--jwt-decode-secret`: The secret key used to verify JWT signatures. If set, the publisher only stores blobs with valid JWTs.
- `--jwt-algorithm`: The algorithm used for JWT verification (defaults to HMAC).
- `--jwt-expiring-sec`: Duration in seconds after which the JWT is considered expired.
- `--jwt-verify-upload`: Enables verification of upload parameters against JWT claims.

### Set the JWT decode secret

The secret can be a hexadecimal string, starting with `0x`. If you do not specify this parameter, authentication is disabled.

All JWT tokens are expected to have the `jti` (JWT ID) set in the claim to a unique value. The publisher uses the JWT ID for replay suppression, to prevent malicious users from storing multiple times using the same JWT. The JWT creator must ensure that this value is unique among all requests to the publisher. Using large nonces to avoid collisions is recommended.

### Choose an authentication algorithm

The following algorithms are supported: HS256, HS384, HS512, ES256, ES384, RS256, RS384, PS256, PS384, PS512, RS512, and EdDSA. The default JWT authentication algorithm is HS256.

### Configure JWT expiration

If this parameter is set and greater than 0, the publisher checks whether the JWT token has expired based on the issued-at (`iat`) value in the JWT token.

### Verify upload parameters

If set, the publisher verifies that the requested upload matches the claims in the JWT. This does not enable or disable the cryptographic authentication of the JWT. It only enables or disables the checks that ensure the contents of the JWT claim match the requested blob upload.

Specifically, the publisher performs the following checks:

- Verifies that the number of `epochs` in the query matches the `epochs` in the JWT, if present.
- Verifies that the `send_object_to` field in the query matches the `send_object_to` in the JWT, if present.
- Verifies the size of the uploaded file.
- Verifies the uniqueness of the `jti` claim.

Disabling parameter verification can be useful when the source of the store requests is trusted, but untrusted sources can also contact the publisher. In that case, the authentication of the JWT is necessary, but the verification of the upload parameters is not.

### Configure replay suppression

The publisher supports replay suppression to prevent the malicious reuse of JWT tokens.

Replay suppression supports the following configuration parameters:

- `--jwt-cache-size`: The maximum size of the JWT cache of the publisher, where the `jti` JWT IDs of used JWTs are kept until their expiration. This is a hard upper bound on the number of entries in the cache, after which additional requests to store are rejected. This hard bound prevents denial-of-service (DoS) attacks on the publisher through the cache.
- `--jwt-cache-refresh-interval`: The interval (in seconds) after which the cache is refreshed and expired JWTs are removed (possibly creating space for additional JWTs).

## Work with JWTs

The current authenticated publisher implementation does not provide a way to generate JWTs and distribute them to clients. You can generate them with any tool (examples on how to create a JWT are given in the next section), as long as they respect the following constraints.

### Mandatory fields

- `exp` (expiration): Timestamp when the token expires.
- `jti` (JWT ID): Unique identifier for the token to prevent replay attacks.

### Optional fields

- `iat` (issued at): Optional timestamp when the token was issued.
- `send_object_to`: Optional Sui address where the newly created `Blob` object should be sent.
- `epochs`: Optional exact number of epochs the blob should be stored for.
- `max_epochs`: Optional maximum number of epochs the blob can be stored for.
- `max_size`: Optional maximum size of the blob in bytes.
- `size`: Optional exact size of the blob in bytes.

You cannot use the `epochs` and `max_epochs` claims together, and you cannot use `size` and `max_size` together. Using both in either case results in token rejection.

The JWT can only encode information about the size and epochs of the blob to store, not about the amount of SUI and WAL the user is allowed to consume. You should handle this on the backend before issuing the JWT.

### Create a valid JWT in the backend

The following examples show how to create JWTs that the authenticated publisher can consume.

#### Rust

In Rust, you can use the [`jsonwebtoken`](https://docs.rs/jsonwebtoken/latest/jsonwebtoken/) crate to create JWTs.

The following struct is used to deserialize the incoming tokens in the publisher (see the [source code](https://github.com/MystenLabs/walrus/blob/main/crates/walrus-service/src/client/daemon/auth.rs) for the complete version):

```rust
pub struct Claim {
    pub iat: Option<i64>,
    pub exp: i64,
    pub jti: String,
    pub send_object_to: Option<SuiAddress>,
    pub epochs: Option<u32>,
    pub max_epochs: Option<u32>,
    pub size: Option<u64>,
    pub max_size: Option<u64>,
}
```

You can use the same struct to create and then encode valid tokens in Rust:

```rust
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

...

let encoding_key = EncodingKey::from_secret("my_secret".as_bytes());
let claim = Claim { /* set here the parameters for the Claim struct above */ };
let jwt = encode(&Header::default(), &claim, &encode_key).expect("a valid claim and key");
```