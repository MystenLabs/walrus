# The authenticated publisher

We now describe the authenticated publisher, which requires the HTTP request to store a blob to be
authenticated. Such authenticated publisher can be used as a building block for services that
require storing over HTTP on Walrus `mainnet`, where an "open" publisher is undesirable (because of
the `SUI` and `WAL` cost of publishing to Walrus).

## Overview

The Walrus Publisher can be configured to require a JWT (JSON web Token) with each HTTP request, for
user authentication. The authentication system ensures that only authorized clients can store blobs
and allows for fine-grained control over storage parameters through JWT claims.

The authenticated publishing flow occurs, at a high level, as follows:

1. **Publisher setup**: The publisher operator:
    - Funds the publisher's wallet with sufficient `SUI` and `WAL`;
    - Configures the publisher to only accept authenticated requests. This entails setting the
      algorithm to authenticate JWTs, the expiration time for JWTs, and the JWT authentication
      secret.
1. **Authentication channel setup**: The publisher operator sets up a channel through which users
   can obtain the JWT tokens. This step can be performed in any way that produces a valid JWT, and
   is not provided in this implementation.
1. **Client authentication**: The client obtains a JWT token from the channel set up in the previous
   step. The JWT token can specify Walrus-relevant constraints, such as the maximum number of epochs
   the JWT can be used to store for, and the maximum size of the blobs being stored.
1. **Publish request**: The client requests to store a blob using the publisher. This is done
   through an HTTP PUT request containing the JWT in an Authorization Bearer HTTP header.
1. **Store to Walrus**: The publisher checks the JWT, and checks that the store request is compliant
   with the constraints specified in the JWT (e.g., the blob being stored is smaller than the
   authorized max size).
1. (Optional) **Asset return:**: If so specified, the publisher returns the newly-created `Blob`
   object to the Sui Address set in the request.

## Publisher setup

The publisher is configured at startup using the following command line arguments:

- `--jwt-decode-secret`: The secret key used to verify JWT signatures. If set, the publisher will
  only store blobs with valid JWTs.
- `--jwt-algorithm`: The algorithm used for JWT verification (defaults to HMAC).
- `--jwt-expiring-sec`: Duration in seconds after which the JWT is considered expired.
- `--jwt-verify-upload`: Enable verification of upload parameters against JWT claims

Additional details follow.

### The JWT decode secret

The secret can be hex string, starting with `0x`. If this parameter is not specified, the
authentication will be disabled.

All JWT tokens are expected to have the `jti` (JWT ID) set in the claim to a unique value. The JWT
is used for replay suppression, i.e., to avoid malicious users storing multiple times using the same
JWT. Therefore, the JWT creator must ensure that this value is unique among all requests to the
publisher. We recommend using large nonces to avoid collisions.

### Authentication algorithm

The following algorithms are supported: "HS256", "HS384", "HS512", "ES256", "ES384", "RS256",
"RS384", "PS256", "PS384", "PS512", "RS512", "EdDSA". The default JWT authentication algorithm will
be HS256.

### JWT Expiration

If the parameter is set and greater than 0, the publisher will check if the JWT token is expired
based on the "issued at" (`iat`) value in the JWT token.

### Upload parameters verification

If set, the publisher will verify that the requested upload matches the claims in the JWT. This
*does not* enable or disable the cryptographic authentication of the JWT; it just enables or
disables the checks that ensure the contents of the JWT claim match the requested blob upload.

Specifically, the publisher will:

- Verify that the number of `epochs` in query is the the same as `epochs` in the JWT, if present;
- Verify that the `send_object_to` field in the query is the same as the `send_object_to` in the
  JWT, if present;
- Verify the size of uploaded file;
- Verify the uniqueness of the `jti` claim.

### Replay-suppression configuration

As mentioned above, the publisher supports replay suppression to avoid the malicious reuse of JWT
tokens.

The replay suppression supports the following configuration parameters:

- `--jwt-cache-size`: The maximum size of the publisher's JWT cache, where the `jti` JWT IDs of the
  "used" JWTs are kept until their expiration. This is a hard upperbound on the number of entries in
  the cache, after which additional requests to store are rejected. This hard bound is introduced to
  avoid DoS attacks on the publisher through the cache.
- `--jwt-cache-refresh-interval`: The interval (in seconds) after which the cache is refreshed, and
  expired JWTs are removed (possibly creating space for additional JWTs to be inserted).

## JWT Fields

The current authenticated publisher implementation does not provide a way to generate the JWTs and
distribute them to the clients. These can be generated with any tool, as long as they respect the
following constraints.

### Mandatory fields

- `exp` (Expiration): timestamp when the token expires;
- `jti` (JWT ID): unique identifier for the token to prevent replay attacks;

### Optional fields

- `iat` (Issued At): Optional timestamp when the token was issued;
- `send_object_to`: Optional Sui address where the newly-created `Blob` object should be sent;
- `epochs`: Optional exact number of epochs the blob should be stored for
- `max_epochs`: Optional maximum number of epochs the blob can be stored for
- `max_size`: Optional maximum size of the blob in bytes
- `size`: Optional exact size of the blob in bytes

Note: The `epochs` and `max_epochs` claims cannot be used together, and neither can `size` and
`max_size`. Using both in either case will result in token rejection.

## Request authentication flow

- The token is sent in the Authorization header as a Bearer token
- The publisher verifies:
  - Token signature using the configured secret
  - Token expiration
  - Token uniqueness (prevents replay attacks)
  - Upload parameters against claims (if enabled)
