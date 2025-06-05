use std::path::PathBuf;

use anyhow::Result;
use walrus_core::{BlobId, encoding::EncodingConfigTrait as _};
use walrus_sdk::{ObjectID, client::Client, config::ClientConfig};
use walrus_sui::client::{SuiReadClient, retry_client::RetriableSuiClient};

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
) -> Result<()> {
    let config: ClientConfig = walrus_sdk::config::load_configuration(
        // Just use default config locations for now.
        Option::<PathBuf>::None,
        None,
    )?;
    let client = get_backfill_client(config.clone()).await?;

    // TODO: I think we need to create NodeWriteCommunication instances for each of the nodes in
    // node_ids here. Not sure the best path to take to get there. Below are some scraps I've
    // collected from elsewhere in the codebase that seem related.
    // let committees_and_state = client.sui_client().get_committees_and_state().await?;

    //let communication_factory: NodeCommunicationFactory = NodeCommunicationFactory::new(
    //    config.communication_config.clone(),
    //    Arc::new(EncodingConfig::new(committees_and_state.current.n_shards())),
    //    None,
    //)?;
    //let active_committees =
    //    ActiveCommittees::new(committees_and_state.current, committees_and_state.previous);
    //
    //  Then create a NodeReadCommunication, and convert it to a NodeWriteCommunication, per
    //  node_id?

    // Ingest blob_ids that have been stored locally via stdin, and look for them in the given
    // folder.
    for line in std::io::stdin().lines() {
        let line = line?;
        let blob_id: BlobId = line.trim().parse()?;
        let blob_filename = blob_id.to_string();

        match std::fs::read(backfill_dir.join(&blob_filename)) {
            Ok(blob) => {
                // Encode the blob.
                let (_sliver_pairs, metadata) = client
                    .encoding_config()
                    // TODO: Encoding type configuration.
                    .get_for_type(walrus_sdk::core::EncodingType::RS2)
                    .encode_with_metadata(&blob)?;

                if *metadata.blob_id() != blob_id {
                    tracing::error!(
                        ?blob_id,
                        "blob file contents do not match blob id! skipping.."
                    );
                    continue;
                }

                // Send this blob to appropriate shards.
                // TODO: encode and send the blob to the nodes.
                tracing::info!(?node_ids, "sending blob to nodes");
            }
            Err(error) => {
                tracing::error!(
                    ?error,
                    ?backfill_dir,
                    ?blob_id,
                    "error reading blob from disk. skipping..."
                );
                continue;
            }
        }
    }
    Ok(())
}
