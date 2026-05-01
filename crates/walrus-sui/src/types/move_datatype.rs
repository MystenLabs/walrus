// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Protocol-agnostic Move datatype descriptors.
//!
//! Mirrors the subset of the Sui gRPC `MovePackageService.GetDatatype` response that Walrus
//! actually walks. Conversion from the proto types lives here so the rest of `walrus-sui`
//! never imports `DatatypeDescriptor`, `FieldDescriptor`, or `OpenSignatureBody` directly.

use anyhow::Context as _;
use sui_rpc::proto::sui::rpc::v2::{
    DatatypeDescriptor,
    FieldDescriptor,
    OpenSignatureBody,
    open_signature_body,
};

/// A Move struct or enum together with the fields Walrus inspects.
#[derive(Debug, Clone)]
pub struct MoveDatatype {
    /// Name of the datatype.
    pub name: String,
    /// Fields in declaration order.
    pub fields: Vec<MoveField>,
}

/// A single field of a Move datatype.
#[derive(Debug, Clone)]
pub struct MoveField {
    /// Field name.
    pub name: String,
    /// Field type.
    pub ty: MoveOpenSignatureBody,
}

/// A subset of the proto `OpenSignatureBody` variants used by Walrus.
#[derive(Debug, Clone)]
pub enum MoveOpenSignatureBody {
    /// Datatype reference: `type_name` is the fully-qualified `<addr>::<module>::<name>`,
    /// `type_parameters` are the instantiated type arguments.
    Datatype {
        /// Fully-qualified type name.
        type_name: String,
        /// Instantiated type arguments.
        type_parameters: Vec<MoveOpenSignatureBody>,
    },
    /// Catch-all for primitives, vectors, type-parameter refs — variants we do not
    /// need today. Extend as future callers require.
    Other,
}

impl TryFrom<DatatypeDescriptor> for MoveDatatype {
    type Error = anyhow::Error;

    fn try_from(descriptor: DatatypeDescriptor) -> Result<Self, Self::Error> {
        let name = descriptor
            .name
            .context("missing name in DatatypeDescriptor")?;
        let fields = descriptor
            .fields
            .into_iter()
            .map(MoveField::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { name, fields })
    }
}

impl TryFrom<FieldDescriptor> for MoveField {
    type Error = anyhow::Error;

    fn try_from(field: FieldDescriptor) -> Result<Self, Self::Error> {
        let name = field.name.context("missing name in FieldDescriptor")?;
        let ty = field
            .r#type
            .context("missing type in FieldDescriptor")?
            .into();
        Ok(Self { name, ty })
    }
}

impl From<OpenSignatureBody> for MoveOpenSignatureBody {
    fn from(body: OpenSignatureBody) -> Self {
        let kind = body
            .r#type
            .and_then(|raw| open_signature_body::Type::try_from(raw).ok());
        match kind {
            Some(open_signature_body::Type::Datatype) => {
                let type_name = body.type_name.unwrap_or_default();
                let type_parameters = body
                    .type_parameter_instantiation
                    .into_iter()
                    .map(MoveOpenSignatureBody::from)
                    .collect();
                MoveOpenSignatureBody::Datatype {
                    type_name,
                    type_parameters,
                }
            }
            _ => MoveOpenSignatureBody::Other,
        }
    }
}

#[cfg(test)]
mod tests {
    use sui_rpc::proto::sui::rpc::v2::{
        DatatypeDescriptor,
        FieldDescriptor,
        OpenSignatureBody,
        open_signature_body,
    };

    use super::*;

    fn datatype_signature(
        type_name: &str,
        type_parameters: Vec<OpenSignatureBody>,
    ) -> OpenSignatureBody {
        OpenSignatureBody::default()
            .with_type(open_signature_body::Type::Datatype)
            .with_type_name(type_name.to_owned())
            .with_type_parameter_instantiation(type_parameters)
    }

    #[test]
    fn extracts_wal_type_from_staked_wal_descriptor() {
        let wal_signature = datatype_signature("0xCAFE::wal::WAL", vec![]);
        let balance_signature = datatype_signature("0x2::balance::Balance", vec![wal_signature]);
        let principal_field = FieldDescriptor::default()
            .with_name("principal".to_owned())
            .with_position(0)
            .with_type(balance_signature);
        let descriptor = DatatypeDescriptor::default()
            .with_name("StakedWal".to_owned())
            .with_fields(vec![principal_field]);

        let datatype = MoveDatatype::try_from(descriptor).expect("conversion should succeed");
        assert_eq!(datatype.name, "StakedWal");
        let principal = datatype
            .fields
            .iter()
            .find(|f| f.name == "principal")
            .expect("principal field");
        let MoveOpenSignatureBody::Datatype {
            type_name,
            type_parameters,
        } = &principal.ty
        else {
            panic!("expected Datatype, got {:?}", principal.ty);
        };
        assert_eq!(type_name, "0x2::balance::Balance");
        let wal_type = type_parameters
            .first()
            .expect("balance has at least one type parameter");
        let MoveOpenSignatureBody::Datatype { type_name, .. } = wal_type else {
            panic!("expected Datatype for WAL type, got {wal_type:?}");
        };
        assert_eq!(type_name, "0xCAFE::wal::WAL");
    }

    #[test]
    fn primitive_signatures_map_to_other() {
        let signature = OpenSignatureBody::default().with_type(open_signature_body::Type::U64);
        assert!(matches!(
            MoveOpenSignatureBody::from(signature),
            MoveOpenSignatureBody::Other
        ));
    }
}
