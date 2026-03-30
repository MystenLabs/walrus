// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Local types for dynamic field listings, decoupled from `sui_json_rpc_types`.

use anyhow::Context;
use bytes::Bytes;
use sui_sdk::rpc_types::DynamicFieldInfo as SuiDynamicFieldInfo;
use sui_types::{TypeTag, base_types::ObjectID, dynamic_field::DynamicFieldType};

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

impl From<SuiDynamicFieldInfo> for DynamicFieldInfo {
    fn from(info: SuiDynamicFieldInfo) -> Self {
        Self {
            field_id: info.object_id,
            name_type: info.name.type_,
            bcs_name: info.bcs_name.into_bytes(),
            kind: match info.type_ {
                DynamicFieldType::DynamicField => DynamicFieldKind::Field,
                DynamicFieldType::DynamicObject => DynamicFieldKind::Object,
            },
            value_type: info.object_type,
        }
    }
}

impl From<DynamicFieldKind> for DynamicFieldType {
    fn from(kind: DynamicFieldKind) -> Self {
        match kind {
            DynamicFieldKind::Field => DynamicFieldType::DynamicField,
            DynamicFieldKind::Object => DynamicFieldType::DynamicObject,
        }
    }
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
    let kind = match kind_i32 {
        1 => DynamicFieldKind::Field,
        2 => DynamicFieldKind::Object,
        other => anyhow::bail!("unknown DynamicFieldKind: {other}"),
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
