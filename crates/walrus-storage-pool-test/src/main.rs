// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! End-to-end test for StoragePool operations on Walrus Private Testnet.
//!
//! Tests the full lifecycle on a single pool: create, register, upload, certify, read back,
//! capacity enforcement, increase capacity, delete + reuse, double-certify rejection,
//! destroy non-empty rejection, extend, destroy pool with end_epoch verification.

use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};
use clap::Parser;
use walrus_core::{
    EncodingType, SuiObjectId,
    encoding::{BlobEncoder, EncodingFactory as _, OwnedOrBorrowedBlob, Primary},
    messages::{BlobPersistenceType, ConfirmationCertificate, SignedStorageConfirmation},
};
use walrus_sdk::{config::load_configuration, node_client::WalrusNodeClient};
use walrus_storage_node_client::{StorageNodeClient, UploadIntent};
use walrus_sui::{
    client::{BlobObjectMetadata, BlobPersistence, ReadClient},
    coin::CoinType,
    config::WalletConfig,
};

#[derive(Parser, Debug)]
#[command(name = "walrus-storage-pool-test")]
struct Args {
    #[arg(long, default_value = "crates/walrus-storage-pool-test/client_config.yaml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    println!("=== Walrus StoragePool E2E Test ===\n");

    // --- Setup ---
    println!("--- Setup ---");
    let client_config =
        load_configuration(Some(&args.config), None).context("failed to load client config")?;

    let load_wallet = || {
        WalletConfig::load_wallet(
            client_config.wallet_config.as_ref(),
            client_config.communication_config.sui_client_request_timeout,
        )
        .context("failed to load wallet")
    };

    let sui_client = client_config
        .new_contract_client(load_wallet()?, None)
        .await
        .context("failed to create contract client")?;

    let wallet_address = sui_client.address();
    let wal_balance = sui_client.total_balance(CoinType::Wal).await?;
    let sui_balance = sui_client.total_balance(CoinType::Sui).await?;
    println!("  Wallet: {wallet_address}");
    println!("  WAL: {} | SUI: {}", wal_balance / 1_000_000_000, sui_balance / 1_000_000_000);

    let sui_client_for_reader = client_config
        .new_contract_client(load_wallet()?, None)
        .await?;
    let walrus_client =
        WalrusNodeClient::new_contract_client_with_refresher(client_config.clone(), sui_client_for_reader)
            .await
            .context("failed to create walrus client")?;

    let encoding_config = walrus_client.encoding_config();
    let n_shards = encoding_config.n_shards();
    let encoding_enum = encoding_config.get_for_type(EncodingType::RS2);

    // Pre-encode all test blobs.
    let blob_data_1 = b"Hello, Walrus Storage Pool! Blob 1 for E2E testing.".to_vec();
    let blob_data_2 = b"Second blob - capacity enforcement test.".to_vec();
    let blob_data_3 = b"Third blob - delete and reuse capacity.".to_vec();

    let encoded_size_1 = encoding_enum
        .encoded_blob_length(blob_data_1.len() as u64)
        .context("blob 1 too large")?;
    let encoded_size_2 = encoding_enum
        .encoded_blob_length(blob_data_2.len() as u64)
        .context("blob 2 too large")?;

    let encoder_1 = BlobEncoder::new(
        encoding_config.get_for_type(EncodingType::RS2),
        OwnedOrBorrowedBlob::Owned(blob_data_1.clone()),
    )?;
    let (sliver_pairs_1, metadata_1) = encoder_1.encode_with_metadata();
    let blob_id_1 = *metadata_1.blob_id();
    let blob_meta_1 = BlobObjectMetadata::try_from(&metadata_1)?;

    let encoder_2 = BlobEncoder::new(
        encoding_config.get_for_type(EncodingType::RS2),
        OwnedOrBorrowedBlob::Owned(blob_data_2.clone()),
    )?;
    let (_, metadata_2) = encoder_2.encode_with_metadata();
    let blob_id_2 = *metadata_2.blob_id();
    let blob_meta_2 = BlobObjectMetadata::try_from(&metadata_2)?;

    let encoder_3 = BlobEncoder::new(
        encoding_config.get_for_type(EncodingType::RS2),
        OwnedOrBorrowedBlob::Owned(blob_data_3.clone()),
    )?;
    let (_, metadata_3) = encoder_3.encode_with_metadata();
    let blob_id_3 = *metadata_3.blob_id();
    let blob_meta_3 = BlobObjectMetadata::try_from(&metadata_3)?;

    println!("  Encoded sizes: blob1={encoded_size_1}, blob2={encoded_size_2}");
    println!("  N shards: {n_shards}\n");

    // ========================================================================
    // Test 1: Create pool with capacity for exactly 1 blob
    // ========================================================================
    println!("--- Test 1: Create pool (capacity = 1 blob) ---");
    let epochs_ahead = 2;
    let pool_id = sui_client
        .create_storage_pool(encoded_size_1, epochs_ahead)
        .await
        .context("failed to create pool")?;
    println!("  PASS: Pool {pool_id}\n");

    // ========================================================================
    // Test 2: Register blob 1 (deletable) — fills the pool
    // ========================================================================
    println!("--- Test 2: Register blob 1 (deletable) ---");
    let pooled_blob_1_id = sui_client
        .register_pooled_blob(pool_id, blob_meta_1.clone(), true)
        .await
        .context("failed to register blob 1")?;
    println!("  PASS: Registered blob 1 ({blob_id_1}), PooledBlob={pooled_blob_1_id}\n");

    // ========================================================================
    // Test 3: Upload slivers for blob 1
    // ========================================================================
    println!("--- Test 3: Upload slivers ---");
    let committee = sui_client.read_client().current_committee().await?;
    let members = committee.members();
    let mut ok = 0_usize;
    let mut fail = 0_usize;

    for member in members {
        if member.network_address.0.contains("no.url") || member.network_address.0.is_empty() {
            continue;
        }
        let nc = match StorageNodeClient::for_storage_node(
            member.network_address.0.as_str(),
            &member.network_public_key,
        ) {
            Ok(c) => c,
            Err(_) => { fail += member.shard_ids.len(); continue; }
        };
        let _ = nc.store_metadata_with_intent(&metadata_1, UploadIntent::Pending).await;
        for &shard in &member.shard_ids {
            let pi = shard.to_pair_index(n_shards, &blob_id_1);
            let pair = &sliver_pairs_1[pi.as_usize()];
            let p = nc.store_sliver_with_intent(&blob_id_1, pi, &pair.primary, UploadIntent::Pending).await;
            let s = nc.store_sliver_with_intent(&blob_id_1, pi, &pair.secondary, UploadIntent::Pending).await;
            if p.is_ok() && s.is_ok() { ok += 1; } else { fail += 1; }
        }
    }
    println!("  PASS: {ok} ok, {fail} failed\n");

    // ========================================================================
    // Test 4: Collect confirmations
    // ========================================================================
    println!("--- Test 4: Collect confirmations ---");
    let quorum = n_shards.get() as usize * 2 / 3 + 1;
    println!("  Waiting 5s for event processing...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    let persistence = BlobPersistenceType::Deletable {
        object_id: SuiObjectId::from(pooled_blob_1_id),
    };
    let mut confirmations: Vec<(u16, SignedStorageConfirmation)> = Vec::new();
    let mut weight = 0_usize;

    for (idx, member) in members.iter().enumerate() {
        if weight >= quorum { break; }
        if member.network_address.0.contains("no.url") || member.network_address.0.is_empty() {
            continue;
        }
        let nc = match StorageNodeClient::for_storage_node(
            member.network_address.0.as_str(),
            &member.network_public_key,
        ) {
            Ok(c) => c,
            Err(_) => continue,
        };
        match nc.get_confirmation(&blob_id_1, &persistence, Some(Duration::from_secs(10))).await {
            Ok(c) => { confirmations.push((idx as u16, c)); weight += member.shard_ids.len(); }
            Err(e) => println!("  WARN: {}: {e}", member.name),
        }
    }
    println!("  Weight: {weight}/{quorum}");
    if weight < quorum { bail!("FAIL: quorum not reached"); }

    let signer_indices: Vec<u16> = confirmations.iter().map(|(i, _)| *i).collect();
    let signed: Vec<_> = confirmations.into_iter().map(|(_, c)| c).collect();
    let certificate = ConfirmationCertificate::from_signed_messages_and_indices(signed, signer_indices)?;
    println!("  PASS: Certificate built\n");

    // ========================================================================
    // Test 5: Certify blob 1
    // ========================================================================
    println!("--- Test 5: Certify blob 1 ---");
    sui_client
        .certify_pooled_blob(pool_id, &blob_id_1, &certificate)
        .await
        .context("failed to certify blob 1")?;
    println!("  PASS\n");

    // ========================================================================
    // Test 6: Read blob 1 back
    // ========================================================================
    println!("--- Test 6: Read blob 1 ---");
    let read = walrus_client
        .read_blob_retry_committees::<Primary>(
            &blob_id_1,
            walrus_core::encoding::ConsistencyCheckType::Default,
        )
        .await
        .context("failed to read blob 1")?;
    if read != blob_data_1 { bail!("FAIL: content mismatch"); }
    println!("  PASS: Content matches ({} bytes)\n", read.len());

    let mut failures: Vec<String> = Vec::new();

    // ========================================================================
    // Test 7: Double-certify → should fail
    // ========================================================================
    println!("--- Test 7: Double-certify blob 1 ---");
    match sui_client.certify_pooled_blob(pool_id, &blob_id_1, &certificate).await {
        Ok(_) => {
            let msg = "Test 7: Double-certify unexpectedly succeeded";
            println!("  FAIL: {msg}");
            failures.push(msg.to_string());
        }
        Err(_) => println!("  PASS: Double-certify correctly rejected"),
    }
    println!();

    // ========================================================================
    // Test 8: Register blob 2 → should fail (pool full)
    // ========================================================================
    println!("--- Test 8: Register blob 2 (pool full) ---");
    match sui_client.register_pooled_blob(pool_id, blob_meta_2.clone(), true).await {
        Ok(_) => {
            let msg = "Test 8: Registration unexpectedly succeeded on full pool";
            println!("  FAIL: {msg}");
            failures.push(msg.to_string());
        }
        Err(_) => println!("  PASS: Registration correctly rejected (EInsufficientCapacity)"),
    }
    println!();

    // ========================================================================
    // Test 9: Increase pool capacity
    // ========================================================================
    println!("--- Test 9: Increase pool capacity ---");
    sui_client
        .increase_storage_pool_capacity(pool_id, encoded_size_2, epochs_ahead)
        .await
        .context("failed to increase capacity")?;
    println!("  PASS: Increased capacity by {encoded_size_2}\n");

    // ========================================================================
    // Test 10: Register blob 2 again → should succeed now
    // ========================================================================
    println!("--- Test 10: Register blob 2 (after capacity increase) ---");
    sui_client
        .register_pooled_blob(pool_id, blob_meta_2, true)
        .await
        .context("failed to register blob 2 after capacity increase")?;
    println!("  PASS: Registered blob 2 ({blob_id_2})\n");

    // ========================================================================
    // Test 11: Destroy non-empty pool → should fail
    // ========================================================================
    println!("--- Test 11: Destroy non-empty pool ---");
    match sui_client.destroy_storage_pool(pool_id).await {
        Ok(_) => {
            let msg = "Test 11: Destroy unexpectedly succeeded on non-empty pool";
            println!("  FAIL: {msg}");
            failures.push(msg.to_string());
        }
        Err(_) => println!("  PASS: Destroy correctly rejected (EPoolNotEmpty)"),
    }
    println!();

    // ========================================================================
    // Test 12: Delete blob 2 (uncertified), freeing capacity
    // ========================================================================
    println!("--- Test 12: Delete blob 2 (uncertified) ---");
    sui_client
        .delete_pooled_blob(pool_id, &blob_id_2)
        .await
        .context("failed to delete blob 2")?;
    println!("  PASS: Deleted blob 2\n");

    // ========================================================================
    // Test 13: Register blob 3 in freed capacity (reuse test)
    // ========================================================================
    println!("--- Test 13: Register blob 3 in freed capacity ---");
    sui_client
        .register_pooled_blob(pool_id, blob_meta_3, true)
        .await
        .context("failed to register blob 3 in freed capacity")?;
    println!("  PASS: Registered blob 3 ({blob_id_3}) — capacity reuse works\n");

    // ========================================================================
    // Test 14: Delete blob 3 and blob 1, verify double-delete rejected
    // ========================================================================
    println!("--- Test 14: Delete all blobs ---");
    sui_client.delete_pooled_blob(pool_id, &blob_id_3).await
        .context("failed to delete blob 3")?;
    println!("  Deleted blob 3");

    sui_client.delete_pooled_blob(pool_id, &blob_id_1).await
        .context("failed to delete blob 1")?;
    println!("  Deleted blob 1");

    match sui_client.delete_pooled_blob(pool_id, &blob_id_1).await {
        Ok(_) => {
            let msg = "Test 14: Double-delete unexpectedly succeeded";
            println!("  FAIL: {msg}");
            failures.push(msg.to_string());
        }
        Err(_) => println!("  PASS: Double-delete correctly rejected"),
    }
    println!();

    // ========================================================================
    // Test 15: Extend pool
    // ========================================================================
    println!("--- Test 15: Extend pool by 3 epochs ---");
    let extend_epochs = 3;
    sui_client
        .extend_storage_pool(
            pool_id,
            extend_epochs,
            encoded_size_1 + encoded_size_2, // current total capacity
        )
        .await
        .context("failed to extend pool")?;
    println!("  PASS\n");

    // ========================================================================
    // Test 16: Destroy empty pool and verify end_epoch
    // ========================================================================
    println!("--- Test 16: Destroy pool, verify end_epoch ---");
    let storage = sui_client
        .destroy_storage_pool(pool_id)
        .await
        .context("failed to destroy pool")?;

    let expected_end = storage.start_epoch + epochs_ahead + extend_epochs;
    assert_eq!(
        storage.end_epoch, expected_end,
        "end_epoch: expected {expected_end} (start={} + {epochs_ahead} + {extend_epochs}), got {}",
        storage.start_epoch, storage.end_epoch
    );
    println!(
        "  Storage: start={}, end={} (expected {expected_end}), size={}",
        storage.start_epoch, storage.end_epoch, storage.storage_size
    );
    println!("  PASS: end_epoch verified\n");

    // ========================================================================
    // Test 17: Reuse returned Storage to register the same blob_id as a regular blob
    // ========================================================================
    println!("--- Test 17: Reuse Storage for regular blob (same blob_id as pooled blob 1) ---");
    // Re-register blob 1 (same content, same blob_id) but now as a regular blob
    // using the Storage object returned from the destroyed pool.
    let blobs = sui_client
        .register_blobs(vec![(blob_meta_1, storage)], BlobPersistence::Deletable)
        .await
        .context("failed to register regular blob with returned storage")?;
    assert_eq!(blobs.len(), 1, "expected 1 blob registered");
    let blob_obj_id = blobs[0].id;
    println!("  Registered regular blob with same blob_id as pooled blob 1: {blob_obj_id}");
    println!("  blob_id: {blob_id_1}");

    sui_client
        .delete_blob(blob_obj_id)
        .await
        .context("failed to delete regular blob")?;
    println!("  Deleted regular blob");
    println!("  PASS: Same blob_id works as both pooled and regular\n");

    // ========================================================================
    // Summary
    // ========================================================================
    let wal_after = sui_client.total_balance(CoinType::Wal).await?;
    let sui_after = sui_client.total_balance(CoinType::Sui).await?;

    if failures.is_empty() {
        println!("=== All 17 tests PASSED ===");
    } else {
        println!("=== FAILURES ({}) ===", failures.len());
        for f in &failures {
            println!("  - {f}");
        }
    }
    println!("  WAL spent: {}", wal_balance.saturating_sub(wal_after));
    println!("  SUI spent: {}", sui_balance.saturating_sub(sui_after));
    println!("  Pool: {pool_id}");

    if !failures.is_empty() {
        bail!("{} test(s) failed", failures.len());
    }

    Ok(())
}
