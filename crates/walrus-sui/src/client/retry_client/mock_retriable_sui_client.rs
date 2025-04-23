// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;
use rand::rngs::StdRng;
use rand::SeedableRng;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sui_sdk::types::base_types::{ObjectID, SuiAddress};
use sui_sdk::types::transaction::{Transaction, TransactionKind};
use sui_sdk::types::TypeTag;
use sui_sdk::SuiClientResult;
use backoff::{ExponentialBackoff, ExponentialBackoffBuilder};

use crate::client::retry_client::RetriableSuiClientTrait;
use crate::types::{Subsidies, SuiTransactionBlockResponse, TypeOriginMap};

pub struct MockRetriableSuiClient {
    rng: StdRng,
    backoff: ExponentialBackoff,
}

impl MockRetriableSuiClient {
    pub fn new() -> Self {
        let rng = StdRng::from_entropy();
        let backoff = ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(Duration::from_secs(60)))
            .build();

        Self { rng, backoff }
    }
}

impl Default for MockRetriableSuiClient {
    fn default() -> Self {
        Self::new()
    }
}

impl RetriableSuiClientTrait for MockRetriableSuiClient {
    async fn get_dynamic_field<K, V>(
        &self,
        _parent: ObjectID,
        _key_type: TypeTag,
        _key: K,
    ) -> SuiClientResult<V>
    where
        K: DeserializeOwned + Serialize,
        V: DeserializeOwned,
    {
        unimplemented!("Mock implementation - get_dynamic_field")
    }

    async fn get_system_package_id_from_system_object(
        &self,
        _system_object_id: ObjectID,
    ) -> SuiClientResult<ObjectID> {
        // Return a mock ObjectID
        Ok(ObjectID::random())
    }

    async fn get_subsidies_object(
        &self,
        _subsidies_object_id: ObjectID,
    ) -> SuiClientResult<Subsidies> {
        unimplemented!("Mock implementation - get_subsidies_object")
    }

    async fn get_package_id_from_object(
        &self,
        _object_id: ObjectID,
    ) -> SuiClientResult<ObjectID> {
        Ok(ObjectID::random())
    }

    async fn has_register_blob_in_subsidies(
        &self,
        _subsidies_package_id: ObjectID,
    ) -> SuiClientResult<bool> {
        Ok(true)
    }

    async fn type_origin_map_for_package(
        &self,
        _package_id: ObjectID,
    ) -> SuiClientResult<TypeOriginMap> {
        Ok(TypeOriginMap::default())
    }

    async fn wal_type_from_package(
        &self,
        _package_id: ObjectID,
    ) -> SuiClientResult<String> {
        Ok("mock_wal_type".to_string())
    }

    async fn estimate_gas_budget(
        &self,
        _signer: SuiAddress,
        _kind: TransactionKind,
        _gas_price: u64,
    ) -> SuiClientResult<u64> {
        Ok(1000000) // Return a mock gas budget
    }

    async fn execute_transaction(
        &self,
        _transaction: Transaction,
        _method: &str,
    ) -> SuiClientResult<SuiTransactionBlockResponse> {
        unimplemented!("Mock implementation - execute_transaction")
    }

    fn get_strategy(&self) -> ExponentialBackoff<StdRng> {
        ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(Duration::from_secs(60)))
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_client_basic_operations() {
        let client = MockRetriableSuiClient::new();

        // Test has_register_blob_in_subsidies
        let result = client.has_register_blob_in_subsidies(ObjectID::random()).await.unwrap();
        assert!(result);

        // Test wal_type_from_package
        let wal_type = client.wal_type_from_package(ObjectID::random()).await.unwrap();
        assert_eq!(wal_type, "mock_wal_type");

        // Test estimate_gas_budget
        let gas_budget = client
            .estimate_gas_budget(
                SuiAddress::random_for_testing_only(),
                TransactionKind::ProgrammableTransaction(Default::default()),
                1,
            )
            .await
            .unwrap();
        assert_eq!(gas_budget, 1000000);
    }
}
