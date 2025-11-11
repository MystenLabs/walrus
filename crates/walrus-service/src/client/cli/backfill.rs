// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Functionality for pulling blobs from an archive, encoding them, and backfilling them to nodes
//! that are missing slivers.
//!
//! The backfill is implemented as a 2-stage pipeline. The upstream task's job is to pull blobs from
//! an archive, and write them to a local folder. The downstream task's job is to list the blob
//! names from the folder, read the blobs, and then send them to the appropriate nodes for
//! backfilling; finally, it deletes the uploaded files. No retries are implemented for failures, as
//! this procedure is intended to be a help to the node recovery process.
//!
//! The upstream pull task uses the number of files in the local folder as a backpressure signal:
//! If the number of files exceeds a certain threshold, it will wait for a while before pulling more
//! blobs. This is to avoid filling up the disk space excessively, and to allow the downstream task
//! to catch up with processing the files. In general, the downstream task is much slower -- because
//! of encoding and sliver transmission -- than the upstream task.
//!
//! We also provide for a mechanism to retain state about which blobs have been downloaded so far
//! (the `pulled_state` file), so that if the process is interrupted, it can be continued with
//! similar input.
//!
//! Note that with this relatively simple file-based approach, we can do a one-time pull of all of
//! the blob IDs from the known archive (`all-blobs.txt`), then partition the problem across worker
//! machines as needed.

// Implementation note: the `pushed_state` and `pulled_state` files may become a bottleneck in the
// operation of the backfill if they become too large.
//
// In that case, we may want to implement state-keeping using a watermark instead of a full file. If
// the input blobs are sorted by blob ID, then we can keep track of the last blob ID that was
// processed, and resume from that point on the next run.

use std::{
    collections::HashSet,
    fs::File,
    io::Write as _,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use anyhow::{Context, Result};
use axum::body::Bytes;
use object_store::{
    ObjectStore,
    gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder},
};
use walrus_core::{BlobId, EncodingType};
use walrus_sdk::{ObjectID, client::WalrusNodeClient, config::ClientConfig};
use walrus_sui::client::{SuiReadClient, retry_client::RetriableSuiClient};

const TOMBSTONE_FILENAME: &str = "tombstone";
const DOWNLOAD_TIMEOUT: Duration = Duration::from_mins(15);

// TODO: Possibly make this configurable.
const DOWNLOAD_BATCH_SIZE: usize = 10;
const MAX_IN_FLIGHT_BACKFILLS: usize = 100;
const BACKPRESSURE_WAIT_TIME: Duration = Duration::from_secs(1);

fn get_blob_ids_from_file(filename: &PathBuf) -> HashSet<BlobId> {
    if filename.exists() {
        tracing::info!(?filename, "reading blob_ids file");
        let start = tokio::time::Instant::now();
        let ret = std::fs::read_to_string(filename)
            .map(|blob_list| {
                blob_list
                    .lines()
                    .filter_map(|line| line.trim().parse().ok())
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();
        tracing::info!(
            ?filename,
            count = ret.len(),
            duration = ?(tokio::time::Instant::now() - start),
            "finished reading blob_ids file");
        ret
    } else {
        HashSet::new()
    }
}

/// Pulls the blobs archived in a GCS bucket, and writes them to the specified backfill directory.
///
/// This function reads blob IDs from stdin, which should be in the format of a single `BlobId` per
/// line. It pulls the blobs from the GCS bucket specified by `gcs_bucket`, and writes them to the
/// `backfill_dir` directory. It also maintains a state file at `pulled_state`, which tracks the
/// blobs that have already been pulled to avoid duplicates.
///
/// The function applies backpressure if the number of files in the `backfill_dir` exceeds
/// `MAX_IN_FLIGHT_BACKFILLS`. It waits for `BACKPRESSURE_WAIT_TIME` before checking again.
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

    // Loop over lines of stdin, which should contain blob IDs to pull. The format of each line
    // should be a single BlobId.
    //
    // We process the blob IDs in batches of `DOWNLOAD_BATCH_SIZE`. Before each batch, we check the
    // number of files present in the `backfill_dir`. If this number is above the
    // `MAX_IN_FLIGHT_BACKFILLS`, we pause for `BACKPRESSURE_WAIT_TIME` and check again.
    let mut batch_counter = 0;
    for line in std::io::stdin().lines() {
        if batch_counter == 0 {
            check_and_apply_backpressure(&backfill_dir).await?;
        }

        process_line(
            &store,
            &backfill_dir,
            &mut pulled_blobs,
            &mut pulled_state,
            prefix.as_deref(),
            &line?,
        )
        .await;
        batch_counter = (batch_counter + 1) % DOWNLOAD_BATCH_SIZE;
    }
    std::fs::write(
        Path::new(&backfill_dir).join(TOMBSTONE_FILENAME),
        format!("{:?}\n", SystemTime::now()).as_bytes(),
    )
    .context("creating tombstone")?;
    Ok(())
}

/// Applies backpressure to the blob download process if needed.
///
/// Checks if the number of files in the backfill directory is greater than the
/// `MAX_IN_FLIGHT_BACKFILLS` limit. If so, it waits for `BACKPRESSURE_WAIT_TIME` and checks again,
/// until the number of files is less than the limit.
async fn check_and_apply_backpressure(backfill_dir: &str) -> Result<()> {
    loop {
        // We have processed a full batch. Now check if we need to backpressure.
        let num_files = std::fs::read_dir(backfill_dir)?.count();
        if num_files > MAX_IN_FLIGHT_BACKFILLS {
            tracing::info!(
                num_files,
                MAX_IN_FLIGHT_BACKFILLS,
                ?BACKPRESSURE_WAIT_TIME,
                DOWNLOAD_BATCH_SIZE,
                "backpressure needed, waiting before next batch"
            );
            tokio::time::sleep(BACKPRESSURE_WAIT_TIME).await;
        } else {
            tracing::debug!(
                num_files,
                MAX_IN_FLIGHT_BACKFILLS,
                DOWNLOAD_BATCH_SIZE,
                "backpressure not needed, continuing"
            );
            return Ok(());
        }
    }
}

/// Processes a line from stdin, extracting the blob ID and pulling the blob from the archive.
async fn process_line(
    store: &GoogleCloudStorage,
    backfill_dir: &str,
    pulled_blobs: &mut HashSet<BlobId>,
    pulled_state: &mut File,
    prefix: Option<&str>,
    line: &str,
) {
    let line = line.trim();
    let likely_blob_id = line.rsplit('/').next().unwrap_or(line);
    tracing::trace!(?likely_blob_id, "processing line from stdin");
    let _ = pull_archive_blob(
        store,
        likely_blob_id,
        backfill_dir,
        pulled_blobs,
        pulled_state,
        prefix,
    )
    .await
    .inspect_err(|e| {
        tracing::error!(?e, line, "failed to process line. Continuing...");
    });
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

            pulled_blobs.insert(blob_id);
            tracing::info!(?blob_id, ?blob_filename, "Blob pulled successfully");
        }
        Err(e) => {
            tracing::error!(?e, ?blob_id, "Failed to pull blob from GCS");
            return Ok(());
        }
    }

    // Update the pulled state file.
    pulled_state.write_all(format!("{blob_id}\n").as_bytes())?;
    pulled_state.flush()?;
    Ok(())
}

async fn get_backfill_client(config: ClientConfig) -> Result<WalrusNodeClient<SuiReadClient>> {
    tracing::debug!(?config, "loaded client config");
    let retriable_sui_client = RetriableSuiClient::new_for_rpc_urls(
        &config.rpc_urls,
        config.backoff_config().clone(),
        None,
    )
    .await?;
    let sui_read_client = config.new_read_client(retriable_sui_client).await?;
    let refresh_handle = config
        .build_refresher_and_run(sui_read_client.clone())
        .await?;
    Ok(WalrusNodeClient::new_read_client(
        config,
        refresh_handle,
        sui_read_client,
    )?)
}

/// Runs the blob backfill process.
///
/// This function initializes the backfill process by creating the backfill directory. Then, it
/// continuously reads the files in the backfill directory, processes them, and backfills the blobs
/// to the specified nodes.
///
/// It also reads the pushed state file to avoid pushing the same blobs again.
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
    let client = loop {
        match get_backfill_client(config.clone()).await {
            Ok(client) => break client,
            Err(e) => {
                tracing::warn!(
                    ?e,
                    ?backfill_dir,
                    "failed to instantiate client; retrying..."
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    };

    tracing::info!("instantiated client");

    // Read the pushed state file, if it exists, to avoid pushing the same blobs again.
    let mut pushed_blobs = get_blob_ids_from_file(&pushed_state);

    // Open an appendable file to track pushed blobs.
    let mut pushed_state = File::options()
        .create(true)
        .append(true)
        .open(&pushed_state)
        .context("opening pushed state file")?;

    // Read all the files in the backfill dir, and try to backfill all of them before reading the
    // directory again.
    loop {
        let dir_entries = match std::fs::read_dir(&backfill_dir) {
            Err(error) => {
                tracing::error!(
                    ?backfill_dir,
                    ?error,
                    "failed to read backfill directory; exiting backfill loop"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            Ok(entries) => entries,
        };

        let entries = dir_entries
            .filter_map(|entry| {
                entry
                    .inspect_err(|error| {
                        tracing::error!(?error, "error reading directory entry; skipping...")
                    })
                    .ok()
                    .map(|entry| entry.path())
            })
            .filter(|path| {
                // Skip tombstones.
                path.file_name()
                    .is_some_and(|path| path != TOMBSTONE_FILENAME)
            })
            .collect::<Vec<_>>();

        if entries.is_empty()
            && std::fs::exists(backfill_dir.join(TOMBSTONE_FILENAME)).unwrap_or(false)
        {
            // Stop if there are no more files to process and a tombstone file exists.
            tracing::info!("tombstone file found; exiting backfill loop");
            break Ok(());
        }
        for blob_filename in entries.iter() {
            process_file_and_backfill(
                &client,
                &node_ids,
                blob_filename,
                &mut pushed_blobs,
                &mut pushed_state,
            )
            .await;
        }

        // Small delay to avoid constant reads if the directory is empty.
        if entries.is_empty() {
            tracing::info!("no more blobs to backfill; sleeping for 500ms");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

/// Processes the file at the given path, extracting the blob ID and backfilling
/// the blob to the nodes.
async fn process_file_and_backfill(
    client: &WalrusNodeClient<SuiReadClient>,
    node_ids: &[ObjectID],
    blob_filename: &Path,
    pushed_blobs: &mut HashSet<BlobId>,
    pushed_state: &mut File,
) {
    match std::fs::read(blob_filename) {
        Ok(blob) => {
            if let Ok(blob_id) = blob_id_from_path(blob_filename) {
                let _ = backfill_blob(client, node_ids, blob_id, &blob, pushed_blobs, pushed_state)
                    .await
                    .inspect(|_| {
                        tracing::debug!(?blob_id, ?blob_filename, "successfully pushed blob");
                    })
                    .inspect_err(|e| {
                        tracing::error!(?e, ?blob_id, "failed to push blob; continuing...");
                    });
            } else {
                tracing::error!(?blob_filename, "cannot get blob ID from path; skipping...");
            };
        }
        Err(error) => {
            tracing::error!(
                ?error,
                ?blob_filename,
                "error reading blob from disk. skipping..."
            );
        }
    }

    // Remove the blob file after processing, even if the backfill fails. Since the backpressure
    // signal is based on the number of files in the directory, we need to ensure that -- even if
    // the backfill fails -- we do not keep the file around indefinitely. Otherwise, the directory
    // could fill up with files that are never processed, and stall the backfill process.
    //
    // Note that transient failures would be fine, as the process would read the files in the
    // directory again later, and try again. However, if the files cannot be backfilled at all for
    // permanent reasons, we do not want to keep them around indefinitely.
    let _ = std::fs::remove_file(blob_filename);
}

/// Extracts the blob ID from the blob file path.
fn blob_id_from_path(blob_path: &Path) -> Result<BlobId> {
    // Get the file path of the path
    let filename = blob_path
        .file_name()
        .ok_or(anyhow::anyhow!("cannot get the file name"))?;
    let blob_id: BlobId = filename.to_str().unwrap_or_default().parse()?;
    Ok(blob_id)
}

async fn backfill_blob(
    client: &WalrusNodeClient<SuiReadClient>,
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
            pushed_state.write_all(format!("{blob_id}\n").as_bytes())?;
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
