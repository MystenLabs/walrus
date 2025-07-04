// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! S3 API types and XML serialization.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// S3 object representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Object {
    /// Object key.
    pub key: String,
    
    /// Object size in bytes.
    pub size: u64,
    
    /// Last modified timestamp.
    pub last_modified: DateTime<Utc>,
    
    /// ETag (entity tag).
    pub etag: String,
    
    /// Storage class.
    pub storage_class: String,
    
    /// Content type.
    pub content_type: Option<String>,
    
    /// Custom metadata.
    pub metadata: HashMap<String, String>,
}

impl S3Object {
    /// Create a new S3 object.
    pub fn new(key: String, size: u64, blob_id: String) -> Self {
        Self {
            key,
            size,
            last_modified: Utc::now(),
            etag: format!("\"{}\"", blob_id),
            storage_class: "STANDARD".to_string(),
            content_type: None,
            metadata: HashMap::new(),
        }
    }
    
    /// Convert to XML for ListObjects response.
    pub fn to_xml(&self) -> String {
        format!(
            r#"<Contents>
    <Key>{}</Key>
    <LastModified>{}</LastModified>
    <ETag>{}</ETag>
    <Size>{}</Size>
    <StorageClass>{}</StorageClass>
</Contents>"#,
            self.key,
            self.last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
            self.etag,
            self.size,
            self.storage_class
        )
    }
}

/// S3 bucket representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Bucket {
    /// Bucket name.
    pub name: String,
    
    /// Creation date.
    pub creation_date: DateTime<Utc>,
}

impl S3Bucket {
    /// Create a new S3 bucket.
    pub fn new(name: String) -> Self {
        Self {
            name,
            creation_date: Utc::now(),
        }
    }
    
    /// Convert to XML for ListBuckets response.
    pub fn to_xml(&self) -> String {
        format!(
            r#"<Bucket>
    <Name>{}</Name>
    <CreationDate>{}</CreationDate>
</Bucket>"#,
            self.name,
            self.creation_date.format("%Y-%m-%dT%H:%M:%S%.3fZ")
        )
    }
}

/// List buckets response.
#[derive(Debug, Clone)]
pub struct ListBucketsResponse {
    /// List of buckets.
    pub buckets: Vec<S3Bucket>,
}

impl ListBucketsResponse {
    /// Convert to XML.
    pub fn to_xml(&self) -> String {
        let buckets_xml: String = self.buckets.iter().map(|b| b.to_xml()).collect();
        
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Owner>
        <ID>walrus-owner</ID>
        <DisplayName>Walrus</DisplayName>
    </Owner>
    <Buckets>
        {}
    </Buckets>
</ListAllMyBucketsResult>"#,
            buckets_xml
        )
    }
}

/// List objects response.
#[derive(Debug, Clone)]
pub struct ListObjectsResponse {
    /// Bucket name.
    pub bucket: String,
    
    /// Prefix used for filtering.
    pub prefix: Option<String>,
    
    /// Marker for pagination.
    pub marker: Option<String>,
    
    /// Maximum number of objects to return.
    pub max_keys: i32,
    
    /// Whether the response is truncated.
    pub is_truncated: bool,
    
    /// List of objects.
    pub contents: Vec<S3Object>,
}

impl ListObjectsResponse {
    /// Create a new list objects response.
    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            prefix: None,
            marker: None,
            max_keys: 1000,
            is_truncated: false,
            contents: Vec::new(),
        }
    }
    
    /// Convert to XML.
    pub fn to_xml(&self) -> String {
        let objects_xml: String = self.contents.iter().map(|o| o.to_xml()).collect();
        
        let prefix_xml = self.prefix.as_ref()
            .map(|p| format!("<Prefix>{}</Prefix>", p))
            .unwrap_or_default();
        
        let marker_xml = self.marker.as_ref()
            .map(|m| format!("<Marker>{}</Marker>", m))
            .unwrap_or_default();
        
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Name>{}</Name>
    {}
    {}
    <MaxKeys>{}</MaxKeys>
    <IsTruncated>{}</IsTruncated>
    {}
</ListBucketResult>"#,
            self.bucket,
            prefix_xml,
            marker_xml,
            self.max_keys,
            self.is_truncated,
            objects_xml
        )
    }
}

/// Copy object response.
#[derive(Debug, Clone)]
pub struct CopyObjectResponse {
    /// Last modified timestamp.
    pub last_modified: DateTime<Utc>,
    
    /// ETag of the copied object.
    pub etag: String,
}

impl CopyObjectResponse {
    /// Create a new copy object response.
    pub fn new(etag: String) -> Self {
        Self {
            last_modified: Utc::now(),
            etag,
        }
    }
    
    /// Convert to XML.
    pub fn to_xml(&self) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <LastModified>{}</LastModified>
    <ETag>{}</ETag>
</CopyObjectResult>"#,
            self.last_modified.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
            self.etag
        )
    }
}

/// Delete objects request.
#[derive(Debug, Clone, Deserialize)]
pub struct DeleteObjectsRequest {
    /// List of objects to delete.
    pub objects: Vec<ObjectToDelete>,
    
    /// Whether to suppress errors.
    pub quiet: bool,
}

/// Object to delete.
#[derive(Debug, Clone, Deserialize)]
pub struct ObjectToDelete {
    /// Object key.
    pub key: String,
    
    /// Object version ID (optional).
    pub version_id: Option<String>,
}

/// Delete objects response.
#[derive(Debug, Clone)]
pub struct DeleteObjectsResponse {
    /// Successfully deleted objects.
    pub deleted: Vec<DeletedObject>,
    
    /// Errors encountered during deletion.
    pub errors: Vec<DeleteError>,
}

/// Successfully deleted object.
#[derive(Debug, Clone)]
pub struct DeletedObject {
    /// Object key.
    pub key: String,
    
    /// Object version ID (optional).
    pub version_id: Option<String>,
}

/// Deletion error.
#[derive(Debug, Clone)]
pub struct DeleteError {
    /// Object key.
    pub key: String,
    
    /// Error code.
    pub code: String,
    
    /// Error message.
    pub message: String,
}

impl DeleteObjectsResponse {
    /// Create a new delete objects response.
    pub fn new() -> Self {
        Self {
            deleted: Vec::new(),
            errors: Vec::new(),
        }
    }
    
    /// Convert to XML.
    pub fn to_xml(&self) -> String {
        let deleted_xml: String = self.deleted.iter().map(|d| {
            format!(
                r#"<Deleted>
    <Key>{}</Key>
</Deleted>"#,
                d.key
            )
        }).collect();
        
        let errors_xml: String = self.errors.iter().map(|e| {
            format!(
                r#"<Error>
    <Key>{}</Key>
    <Code>{}</Code>
    <Message>{}</Message>
</Error>"#,
                e.key, e.code, e.message
            )
        }).collect();
        
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    {}
    {}
</DeleteResult>"#,
            deleted_xml,
            errors_xml
        )
    }
}

/// Complete multipart upload request.
#[derive(Debug, Clone, Deserialize)]
pub struct CompleteMultipartUploadRequest {
    /// List of parts.
    pub parts: Vec<CompletedPart>,
}

/// Completed part.
#[derive(Debug, Clone, Deserialize)]
pub struct CompletedPart {
    /// Part number.
    pub part_number: i32,
    
    /// ETag of the part.
    pub etag: String,
}
