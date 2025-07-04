# Walrus S3 Gateway

A S3-compatible API gateway for Walrus storage that translates S3 requests into Walrus operations.

## Features

- **S3 API Compatibility**: Supports core S3 operations like PutObject, GetObject, DeleteObject, ListObjects
- **SIGv4 Authentication**: Full AWS Signature Version 4 authentication support
- **Walrus Integration**: Seamlessly stores and retrieves data from Walrus storage
- **TLS Support**: Optional HTTPS/TLS encryption
- **CORS Support**: Configurable CORS headers for web applications
- **Multipart Upload**: Support for S3 multipart upload (planned)

## Supported S3 Operations

### Bucket Operations
- `ListBuckets` - List all buckets
- `CreateBucket` - Create a new bucket
- `DeleteBucket` - Delete a bucket
- `HeadBucket` - Check if bucket exists
- `GetBucketLocation` - Get bucket region
- `GetBucketVersioning` - Get versioning status
- `GetBucketAcl` - Get bucket ACL

### Object Operations
- `ListObjects` - List objects in a bucket
- `GetObject` - Retrieve an object
- `PutObject` - Store an object
- `DeleteObject` - Delete an object
- `HeadObject` - Get object metadata
- `CopyObject` - Copy an object (planned)

### Multipart Upload (Planned)
- `CreateMultipartUpload` - Initiate multipart upload
- `UploadPart` - Upload a part
- `CompleteMultipartUpload` - Complete multipart upload
- `AbortMultipartUpload` - Abort multipart upload

## Installation

Add this crate to the workspace by adding it to the root `Cargo.toml`:

```toml
[workspace]
members = [
    # ... existing members
    "crates/walrus-s3-gateway"
]
```

## Configuration

Create a configuration file (see `config.example.toml` for reference):

```toml
bind_address = "0.0.0.0:8080"
access_key = "your-access-key"
secret_key = "your-secret-key"
region = "us-east-1"
max_body_size = 67108864
request_timeout = 300
enable_cors = true
enable_tls = false
```

## Usage

### Running the Server

```bash
# Using default configuration
cargo run --bin walrus-s3-gateway

# Using custom configuration file
cargo run --bin walrus-s3-gateway -- --config config.toml

# Command line overrides
cargo run --bin walrus-s3-gateway -- \
    --bind 127.0.0.1:9000 \
    --access-key mykey \
    --secret-key mysecret \
    --region us-west-2
```

### Using with S3 Clients

Once the server is running, you can use any S3-compatible client:

#### AWS CLI

```bash
# Configure AWS CLI
aws configure set aws_access_key_id walrus-access-key
aws configure set aws_secret_access_key walrus-secret-key
aws configure set default.region us-east-1

# Use with custom endpoint
aws --endpoint-url http://localhost:8080 s3 ls
aws --endpoint-url http://localhost:8080 s3 cp file.txt s3://walrus-bucket/
aws --endpoint-url http://localhost:8080 s3 cp s3://walrus-bucket/file.txt downloaded.txt
```

#### Python boto3

```python
import boto3

# Create S3 client
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:8080',
    aws_access_key_id='walrus-access-key',
    aws_secret_access_key='walrus-secret-key',
    region_name='us-east-1'
)

# Upload a file
s3.put_object(Bucket='walrus-bucket', Key='test.txt', Body=b'Hello Walrus!')

# Download a file
response = s3.get_object(Bucket='walrus-bucket', Key='test.txt')
data = response['Body'].read()
```

#### JavaScript/Node.js

```javascript
const AWS = require('aws-sdk');

const s3 = new AWS.S3({
    endpoint: 'http://localhost:8080',
    accessKeyId: 'walrus-access-key',
    secretAccessKey: 'walrus-secret-key',
    region: 'us-east-1',
    s3ForcePathStyle: true
});

// Upload a file
s3.putObject({
    Bucket: 'walrus-bucket',
    Key: 'test.txt',
    Body: 'Hello Walrus!'
}, (err, data) => {
    if (err) console.error(err);
    else console.log('Upload successful:', data);
});
```

## Architecture

The S3 gateway acts as a translation layer between S3 API calls and Walrus operations:

1. **Authentication**: Validates S3 SIGv4 signatures
2. **Request Parsing**: Parses S3 API requests and extracts parameters
3. **Walrus Translation**: Converts S3 operations to Walrus SDK calls
4. **Response Formatting**: Formats Walrus responses as S3-compatible XML/JSON

### Key Mappings

- **S3 Buckets** → Single Walrus namespace (buckets are virtual)
- **S3 Object Keys** → Walrus blob IDs (with mapping layer)
- **S3 ETags** → Walrus blob IDs
- **S3 Metadata** → Stored separately (requires additional storage)

## Implementation Status

### ✅ Completed
- Basic S3 API structure
- SIGv4 authentication
- Core handlers (GET, PUT, DELETE, HEAD)
- Configuration management
- Error handling and XML responses

### 🚧 In Progress
- Walrus client integration
- Object key to blob ID mapping
- Metadata storage

### 📋 Planned
- Multipart upload support
- Object copying
- Bucket policies and ACLs
- Versioning support
- Logging and metrics
- Performance optimizations

## Development

### Testing

```bash
# Run unit tests
cargo test -p walrus-s3-gateway

# Run integration tests (requires running Walrus network)
cargo test -p walrus-s3-gateway --features integration-tests
```

### Adding New Operations

1. Add the handler in `src/handlers/`
2. Update the router in `src/server.rs`
3. Add corresponding error types in `src/error.rs`
4. Update S3 types if needed in `src/s3_types.rs`

## Security Considerations

- **Authentication**: Always use strong access keys and secret keys
- **TLS**: Enable TLS for production deployments
- **Network**: Run behind a reverse proxy (nginx, HAProxy) for production
- **Validation**: All inputs are validated according to S3 specifications

## Limitations

- **Bucket Concept**: Walrus doesn't have buckets, so they are virtualized
- **Deletion**: Walrus doesn't support deletion, so delete operations are simulated
- **Versioning**: Not supported by Walrus
- **Large Objects**: May require chunking for very large files
- **Consistency**: Eventual consistency model inherited from Walrus

## Contributing

1. Fork the repository
2. Create a feature branch
3. Implement your changes
4. Add tests
5. Submit a pull request

## License

Apache-2.0 License
