// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod tests {

    use sui_sdk::{
        rpc_types::{EventFilter, SuiTransactionBlockResponseOptions},
        types::{base_types::ObjectID, quorum_driver_types::ExecuteTransactionRequestType},
        SuiClientBuilder,
    };
    use test_cluster::TestClusterBuilder;
    use tokio;

    use sui_move_build::{
        build_from_resolution_graph, gather_published_ids, BuildConfig, PackageDependencies,
    };

    use std::path::PathBuf;

    /// Taken from the Hummingbird codebase
    pub fn compile_package(package_path: PathBuf) -> (PackageDependencies, Vec<Vec<u8>>) {
        let mut build_config = BuildConfig::default();
        build_config.config.install_dir = Some(package_path.clone());
        build_config.config.lock_file = Some(package_path.join("Move.lock"));
        let resolution_graph = build_config
            .resolution_graph(&package_path)
            .expect("Resolution failed");
        let (_, dependencies) = gather_published_ids(&resolution_graph);
        let compiled_package =
            build_from_resolution_graph(package_path, resolution_graph, false, false).expect("");
        let compiled_modules = compiled_package.get_package_bytes(false);
        (dependencies, compiled_modules)
    }

    #[tokio::test]
    async fn test_e2e_blob_storage_contract_calls() -> anyhow::Result<()> {
        let mut test_cluster = TestClusterBuilder::new().build().await;
        let context = &mut test_cluster.wallet;
        let sender = context.active_address().unwrap();

        let mut current_dir = std::env::current_dir()?;
        // navigate to ../../contracts/blob_store
        current_dir.pop();
        current_dir.pop();
        current_dir.push("contracts");
        current_dir.push("blob_store");

        let (dependencies, compiled_modules) = compile_package(current_dir);

        let sui = SuiClientBuilder::default()
            .build(&test_cluster.fullnode_handle.rpc_url)
            .await
            .expect("Failed to get client.");

        println!("Test address: {:?}", sender);
        let dep_ids: Vec<ObjectID> = dependencies.published.values().cloned().collect();

        // Build a publish transaction
        let publish_tx = sui
            .transaction_builder()
            .publish(sender, compiled_modules, dep_ids, None, 10000000000)
            .await?;

        // Get a signed transaction
        let transaction = test_cluster.wallet.sign_transaction(&publish_tx);

        // Submit the transaction
        let transaction_response = sui
            .quorum_driver_api()
            .execute_transaction_block(
                transaction,
                SuiTransactionBlockResponseOptions::full_content(),
                Some(ExecuteTransactionRequestType::WaitForLocalExecution),
            )
            .await?;

        assert!(
            transaction_response.status_ok() == Some(true),
            "Status not ok"
        );

        // Read the system creation event

        let events = sui
            .event_api()
            .query_events(EventFilter::Sender(sender), None, None, false)
            .await?;

        assert!(
            events
                .data
                .iter()
                .any(|e| e.type_.name.clone().into_string() == "EpochChangeSync"),
            "No EpochChangeSync event found"
        );

        assert!(
            events
                .data
                .iter()
                .any(|e| e.type_.name.clone().into_string() == "EpochChangeDone"),
            "No EpochChangeDone event found"
        );

        // Print events
        println!("Events: {:?}", events);

        Ok(())
    }
}
