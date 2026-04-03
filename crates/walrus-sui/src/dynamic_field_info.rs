// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Local types for dynamic field listings, decoupled from `sui_json_rpc_types`.

use anyhow::Context;
use bytes::Bytes;
use sui_rpc::proto::sui::rpc::v2::dynamic_field::DynamicFieldKind as ProtoDynamicFieldKind;
use sui_types::{TypeTag, base_types::ObjectID};

/// The kind of a dynamic field (field or object).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DynamicFieldKind {
    /// A regular dynamic field.
    Field,
    /// A dynamic object field.
    Object,
}

/// Information about a single dynamic field.
#[derive(Debug, Clone)]
pub struct DynamicFieldInfo {
    /// The dynamic field wrapper object ID.
    pub field_id: ObjectID,
    /// The Move type of the name.
    pub name_type: TypeTag,
    /// BCS-encoded name value.
    pub bcs_name: Vec<u8>,
    /// Whether this is a field or object field.
    pub kind: DynamicFieldKind,
    /// The type of the value (or child object type for dynamic object fields).
    pub value_type: String,
}

/// A page of dynamic field results with an opaque cursor for pagination.
#[derive(Debug, Clone)]
pub struct DynamicFieldPage {
    /// The dynamic fields in this page.
    pub fields: Vec<DynamicFieldInfo>,
    /// Opaque cursor for fetching the next page.
    pub next_cursor: Option<Bytes>,
    /// Whether there are more pages to fetch.
    pub has_next_page: bool,
}

/// Convert a gRPC `DynamicField` proto message into a local `DynamicFieldInfo`.
pub(crate) fn dynamic_field_info_from_grpc(
    field: sui_rpc::proto::sui::rpc::v2::DynamicField,
) -> anyhow::Result<DynamicFieldInfo> {
    let field_id: ObjectID = field
        .field_id
        .context("missing field_id in DynamicField")?
        .parse()
        .context("parsing field_id")?;

    let name_bcs = field.name.context("missing name in DynamicField")?;
    let name_type: TypeTag = name_bcs
        .name
        .context("missing name type in DynamicField name")?
        .parse()
        .context("parsing name type")?;
    let bcs_name = name_bcs
        .value
        .context("missing name value in DynamicField name")?
        .to_vec();

    let kind_i32 = field.kind.context("missing kind in DynamicField")?;
    let proto_kind = ProtoDynamicFieldKind::try_from(kind_i32)
        .map_err(|_| anyhow::anyhow!("unknown DynamicFieldKind: {kind_i32}"))?;
    let kind = match proto_kind {
        ProtoDynamicFieldKind::Field => DynamicFieldKind::Field,
        ProtoDynamicFieldKind::Object => DynamicFieldKind::Object,
        other => anyhow::bail!("unsupported DynamicFieldKind: {}", other.as_str_name()),
    };

    let value_type = field
        .value_type
        .context("missing value_type in DynamicField")?;

    Ok(DynamicFieldInfo {
        field_id,
        name_type,
        bcs_name,
        kind,
        value_type,
    })
}
