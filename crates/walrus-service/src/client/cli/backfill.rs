// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The idea here is to have a two-stage pipeline. The upstream task's job is to pull blobs from an
//! archive, write them to a local folder, then emit the name of the blob to stdout.
//! The downstream task's job is to read the blob names from stdin, read the blobs from the local
//! folder, and then send them to the appropriate nodes for backfilling.
//!
//! We also provide for a mechanism to retain state about which blobs have
//! been downloaded so far (the `pulled_state` file), so that if the process is interrupted, it can
//! be continued with similar input.
//!
//! The upstream task can be run like this:
//! ```
//! # First fetch all the blob IDs from the archive. Note that this can take 25 minutes and results
//! in an 869MB file with 12248420 lines.
//!
//! gcloud storage ls gs://walrus-backup-mainnet >all-blobs.txt
//!
//! grep -E '^0' all-blobs.txt \
//!   | walrus pull-archive-blobs \
//!       --pulled-state downloaded-blobs.txt \
//!       --gcs-bucket walrus-backup-mainnet \
//!       --backfill-dir ./backfill \
//!   | walrus blob-backfill --backfill-dir ./blob ...
//! ```
//!
//! Note that with this relatively simple file-based approach, we can do a one-time pull of all
//! of the blob IDs from the known archive (`all-blobs.txt`), then partition the problem
//! across worker machines as needed.
//!
//! The thing to stress here is that for a multi-day backfill, it's super helpful to know how to
//! stop and restart without losing all prior progress.
//!

use std::{collections::HashSet, fs::File, io::Write as _, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use axum::body::Bytes;
use object_store::{
    ObjectStore,
    gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder},
};
use walrus_core::{BlobId, EncodingType};
use walrus_sdk::{ObjectID, client::Client, config::ClientConfig};
use walrus_sui::client::{SuiReadClient, retry_client::RetriableSuiClient};

const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(15 * 60);

fn get_blob_ids_from_file(filename: &PathBuf) -> HashSet<BlobId> {
    if filename.exists() {
        if let Ok(blob_list) = std::fs::read_to_string(filename) {
            blob_list
                .lines()
                .filter_map(|line| line.trim().parse().ok())
                .collect::<HashSet<_>>()
        } else {
            Default::default()
        }
    } else {
        Default::default()
    }
}

pub(crate) async fn pull_archive_blobs(
    gcs_bucket: String,
    prefix: Option<String>,
    backfill_dir: String,
    pulled_state: PathBuf,
) -> Result<()> {
    tracing::info!(
        gcs_bucket,
        ?prefix,
        backfill_dir,
        ?pulled_state,
        "pulling archive blobs from GCS bucket"
    );
    let store = GoogleCloudStorageBuilder::from_env()
        .with_client_options(object_store::ClientOptions::default().with_timeout(DOWNLOAD_TIMEOUT))
        .with_bucket_name(gcs_bucket)
        .build()?;

    std::fs::create_dir_all(&backfill_dir).context("creating backfill directory")?;

    // Read the pulled state file, if it exists, to avoid pulling the same blobs again.
    let mut pulled_blobs = get_blob_ids_from_file(&pulled_state);

    // Open an appendable file to track pulled blobs and avoid dupes.
    let mut pulled_state = File::options()
        .create(true)
        .append(true)
        .open(&pulled_state)
        .context("opening pulled state file")?;

    // Loop over lines of stdin, which should contain blob IDs to pull.
    // The format of each line should be a single BlobId.
    for line in std::io::stdin().lines() {
        let line = line?;
        let line = line.trim();
        let likely_blob_id = line.rsplit('/').next().unwrap_or(line);
        tracing::info!(?likely_blob_id, "Processing line from stdin");
        let _ = pull_archive_blob(
            &store,
            likely_blob_id,
            &backfill_dir,
            &mut pulled_blobs,
            &mut pulled_state,
            prefix.as_deref(),
        )
        .await
        .inspect_err(|e| {
            tracing::error!(?e, line, "Failed to process line. Continuing...");
        });
    }
    Ok(())
}

async fn pull_archive_blob(
    store: &GoogleCloudStorage,
    blob_id: &str,
    backfill_dir: &str,
    pulled_blobs: &mut HashSet<BlobId>,
    pulled_state: &mut File,
    prefix: Option<&str>,
) -> Result<()> {
    if prefix.is_some_and(|prefix| !blob_id.starts_with(prefix)) {
        tracing::trace!(?blob_id, "Blob ID does not match prefix, skipping");
        return Ok(());
    }
    let blob_id: BlobId = blob_id.parse()?;

    // Check if the blob has already been pulled.
    if pulled_blobs.contains(&blob_id) {
        tracing::info!(?blob_id, "Blob already exists, skipping download");
        // Emit the blob ID to stdout for further downstream processing.
        println!("{}", blob_id);
        return Ok(());
    }

    // Pull the blob from GCS.
    match store.get(&blob_id.to_string().into()).await {
        Ok(object) => {
            // Write the blob to the specified backfill directory.
            let blob_filename = PathBuf::from(&backfill_dir).join(blob_id.to_string());
            let mut file = File::create(&blob_filename)?;
            let bytes: Bytes = object.bytes().await?;
            file.write_all(&bytes)?;
            drop(file);

            // Emit the blob ID to stdout for further downstream processing.
            println!("{}", blob_id);
            pulled_blobs.insert(blob_id);

            tracing::info!(?blob_id, ?blob_filename, "Blob pulled successfully");
        }
        Err(e) => {
            tracing::error!(?e, ?blob_id, "Failed to pull blob from GCS");
            return Ok(());
        }
    }

    // Update the pulled state file.
    pulled_state.write_all(format!("{}\n", blob_id).as_bytes())?;
    pulled_state.flush()?;
    Ok(())
}

async fn get_backfill_client(config: ClientConfig) -> Result<Client<SuiReadClient>> {
    tracing::debug!(?config, "loaded client config");
    let retriable_sui_client = RetriableSuiClient::new_for_rpc_urls(
        &config.rpc_urls,
        config.backoff_config().clone(),
        None,
    )
    .await?;
    let sui_read_client = config.new_read_client(retriable_sui_client).await?;
    let refresh_handle = config
        .refresh_config
        .build_refresher_and_run(sui_read_client.clone())
        .await?;
    Ok(Client::new_read_client(config, refresh_handle, sui_read_client).await?)
}

pub(crate) async fn run_blob_backfill(
    backfill_dir: PathBuf,
    node_ids: Vec<ObjectID>,
    pushed_state: PathBuf,
) -> Result<()> {
    std::fs::create_dir_all(&backfill_dir).context("creating backfill directory")?;
    tracing::info!(
        ?backfill_dir,
        ?node_ids,
        ?pushed_state,
        "running blob backfill"
    );
    let config: ClientConfig = walrus_sdk::config::load_configuration(
        // Just use default config locations for now.
        Option::<PathBuf>::None,
        None,
    )?;
    tracing::info!(?config, "loaded config");
    let client = get_backfill_client(config.clone()).await?;

    tracing::info!("instantiated client");

    // Read the pushed state file, if it exists, to avoid pushing the same blobs again.
    let mut pushed_blobs = get_blob_ids_from_file(&pushed_state);

    // Open an appendable file to track pushed blobs.
    let mut pushed_state = File::options()
        .create(true)
        .append(true)
        .open(&pushed_state)
        .context("opening pushed state file")?;

    // Ingest via stdin, then process the blob_ids that have been stored in `backfill_dir`.
    for line in std::io::stdin().lines() {
        let line = line?;
        let blob_id: BlobId = line.trim().parse()?;
        let blob_filename = backfill_dir.join(blob_id.to_string());

        match std::fs::read(&blob_filename) {
            Ok(blob) => {
                let _ = backfill_blob(
                    &client,
                    &node_ids,
                    blob_id,
                    &blob,
                    &mut pushed_blobs,
                    &mut pushed_state,
                )
                .await
                .inspect_err(|e| {
                    tracing::error!(?e, ?blob_id, "Failed to push blob. Continuing...");
                });
                // Discard the blob file if we've pushed it, successfully or not.
                let _ = std::fs::remove_file(blob_filename);
            }
            Err(error) => {
                tracing::error!(
                    ?error,
                    ?backfill_dir,
                    ?blob_id,
                    "error reading blob from disk. skipping..."
                );
                // Discard the blob file if it cannot be read.
                let _ = std::fs::remove_file(blob_filename);
                continue;
            }
        }
    }
    Ok(())
}

async fn backfill_blob(
    client: &Client<SuiReadClient>,
    node_ids: &[ObjectID],
    blob_id: BlobId,
    blob: &[u8],
    pushed_blobs: &mut HashSet<BlobId>,
    pushed_state: &mut File,
) -> Result<()> {
    if pushed_blobs.contains(&blob_id) {
        tracing::info!(?blob_id, "Blob already pushed, skipping");
        return Ok(());
    }
    // Send this blob to appropriate nodes.
    match client
        .backfill_blob_to_nodes(
            blob,
            node_ids.iter().copied(),
            EncodingType::RS2,
            Some(blob_id),
        )
        .await
    {
        Ok(node_results) => {
            pushed_state.write_all(format!("{}\n", blob_id).as_bytes())?;
            pushed_state.flush()?;
            pushed_blobs.insert(blob_id);
            tracing::info!(?node_results, ?blob_id, "backfill_blob_to_nodes succeeded");
        }
        Err(error) => {
            tracing::error!(
                ?error,
                ?blob_id,
                "backfill_blob_to_nodes failed. skipping..."
            );
        }
    }
    Ok(())
}
