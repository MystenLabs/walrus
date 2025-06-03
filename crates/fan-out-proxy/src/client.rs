// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! A client for the fanout proxy.

use std::num::NonZeroU16;
#[cfg(feature = "test-client")]
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use fastcrypto::hash::{Digest, HashFunction, Sha256};
use rand::{Rng, SeedableRng, rngs::StdRng};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use sui_types::{
    base_types::SuiAddress,
    digests::TransactionDigest,
    transaction::{Transaction, TransactionData, TransactionKind},
};
use walrus_sdk::{
    ObjectID,
    client::Client as WalrusClient,
    config::ClientConfig as WalrusConfig,
    core::{
        BlobId,
        EncodingType,
        EpochCount,
        encoding::EncodingConfigTrait,
        messages::ConfirmationCertificate,
        metadata::{BlobMetadataApi, VerifiedBlobMetadataWithId},
    },
    sui::{
        client::{
            BlobPersistence,
            CoinType,
            SuiContractClient,
            transaction_builder::WalrusPtbBuilder,
        },
        config::WalletConfig,
        wallet::Wallet,
    },
};

use crate::{
    TipConfig,
    controller::{BLOB_FAN_OUT_ROUTE, ResponseType, TIP_CONFIG_ROUTE},
    utils::compute_blob_digest_sha256,
};

const DIGEST_LEN: usize = 32;

/// The authentication structure for a blob store request.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) struct AuthPackage {
    /// The SHA256 hash of the blob data.
    pub blob_digest: [u8; DIGEST_LEN],
    /// Random bytes known only to the client. Shared with fan-out proxy to prevent malicious replay
    /// attacks by other users.
    pub nonce: [u8; DIGEST_LEN],
    /// The timestamp just prior to blob registration as known to the client user. This is used to
    /// expire fan-out requests after some time.
    pub timestamp_ms: u64,
    /// The blob encoding type.
    // TODO: pull this directly from the ptb.
    pub encoding_type: EncodingType,
    /// The blob persistence type.
    // TODO: pull this directly from the ptb.
    pub blob_persistence: BlobPersistence,
}

impl AuthPackage {
    /// Creates an authentication package for the blob.
    pub(crate) fn new(
        blob: &[u8],
        encoding_type: EncodingType,
        blob_persistence: BlobPersistence,
    ) -> Result<Self> {
        let std_rng = StdRng::from_rng(&mut rand::thread_rng())?;
        let mut rng = std_rng;
        let nonce: [u8; 32] = rng.r#gen();

        let blob_digest = compute_blob_digest_sha256(blob);
        let timestamp_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("the clocks are set in the present")
                .as_millis(),
        )?;

        Ok(Self {
            blob_digest: blob_digest.into(),
            nonce,
            timestamp_ms,
            encoding_type,
            blob_persistence,
        })
    }

    /// Returns the BCS-encoded bytes for the auth package.
    pub(crate) fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(bcs::to_bytes(self)?)
    }

    /// Returns the digest (SHA256) for the authentication package.
    pub(crate) fn to_digest(&self) -> Result<Digest<DIGEST_LEN>> {
        let mut auth_hash = Sha256::new();
        auth_hash.update(self.to_bytes()?);
        Ok(auth_hash.finalize())
    }
}

/// Runs the test client.
pub(crate) async fn run_client(
    file: PathBuf,
    context: Option<String>,
    walrus_config: Option<PathBuf>,
    server_url: Url,
    epochs: EpochCount,
    wallet: Option<PathBuf>,
    gas_budget: Option<u64>,
) -> Result<()> {
    let blob = fs::read(file)?;
    let fanout =
        FanOutClient::from_args(context, walrus_config, server_url, wallet, gas_budget).await?;

    let metadata = fanout.compute_metadata(&blob)?;
    let computed_blob_id = *metadata.blob_id();

    let encoded_size = fanout
        .encoded_size(metadata.metadata().unencoded_length())
        .unwrap();

    let auth_package = AuthPackage::new(&blob, EncodingType::RS2, BlobPersistence::Permanent)?;
    let auth_digest = auth_package.to_digest()?;

    // Transaction creation.
    let (fanout, signed_tx) = fanout
        .with_pt_builder()?
        .add_pure_input(auth_digest)?
        .add_buy_and_register(&metadata, epochs, encoded_size)
        .await?
        .add_tip(encoded_size)
        .await?
        .finish_and_sign()
        .await?;

    ////////////////////////////////////////////////////////////////////////////////
    // New stuff
    let response = fanout
        .walrus_client
        .sui_client()
        .sui_client()
        .execute_transaction(signed_tx, "execute_transaction")
        .await?;

    anyhow::ensure!(
        matches!(response.status_ok(), Some(true)),
        "transaction execution failed"
    );
    let tx_id = response.digest;

    ////////////////////////////////////////////////////////////////////////////////
    // Send the actual blob, along with the associated Auth and Tip information.
    let response = fanout
        .send_to_proxy(tx_id, auth_package, blob, computed_blob_id)
        .await?;
    ////////////////////////////////////////////////////////////////////////////////

    anyhow::ensure!(
        computed_blob_id == response.blob_id,
        "the computed blob ID does not match the blob ID in the response from the proxy \
        computed={} response={}",
        computed_blob_id,
        response.blob_id
    );

    let (fanout, transaction) = fanout
        .with_pt_builder()?
        .certify_blob(response.blob_object, &response.confirmation_certificate)
        .await?
        .finish_and_sign()
        .await?;

    let result = fanout
        .walrus_client
        .sui_client()
        .sui_client()
        .execute_transaction(transaction, "certify_blob")
        .await?;

    anyhow::ensure!(
        matches!(result.status_ok(), Some(true)),
        "certification failed {:?}",
        result
    );

    println!(
        "Blob with ID {} and encoded size {} successfully stored",
        computed_blob_id, encoded_size
    );
    Ok(())
}

/// Gets the tip configuration from the specified fanout proxy.
async fn get_tip_config(server_url: &Url) -> Result<TipConfig> {
    Ok(reqwest::get(server_url.join(TIP_CONFIG_ROUTE)?)
        .await?
        .json()
        .await?)
}

/// Gets a Walrus contract client from the configuration.
async fn contract_client_from_args(
    walrus_config: Option<impl AsRef<Path>>,
    context: Option<&str>,
    wallet: Option<impl AsRef<Path>>,
    gas_budget: Option<u64>,
) -> Result<WalrusClient<SuiContractClient>> {
    // NOTE: This is taken from `walrus_sdk::client::cli::ClientCommandRunner::new`.
    let walrus_config = walrus_sdk::config::load_configuration(walrus_config.as_ref(), context);

    let wallet_config = wallet.map(WalletConfig::from_path).or(walrus_config
        .as_ref()
        .ok()
        .and_then(|config| config.wallet_config.clone()));

    let wallet = WalletConfig::load_wallet(
        wallet_config.as_ref(),
        walrus_config
            .as_ref()
            .ok()
            .and_then(|config| config.communication_config.sui_client_request_timeout),
    );
    get_contract_client(walrus_config?, wallet, gas_budget).await
}

/// Creates a [`Client<SuiContractClient>`](`WalrusClient<SuiContractClient>`) based on the provided
/// [`WalrusConfig`] with write access to Sui.
#[cfg(feature = "test-client")]
pub async fn get_contract_client(
    walrus_config: WalrusConfig,
    wallet: Result<Wallet>,
    gas_budget: Option<u64>,
) -> Result<WalrusClient<SuiContractClient>> {
    let sui_client = walrus_config
        .new_contract_client(wallet?, gas_budget)
        .await?;

    let refresh_handle = walrus_config
        .refresh_config
        .build_refresher_and_run(sui_client.read_client().clone())
        .await?;
    let client =
        WalrusClient::new_contract_client(walrus_config, refresh_handle, sui_client).await?;

    Ok(client)
}

/// A client to send blobs and tips to the fan-out proxy.
pub(crate) struct FanOutClient<T = ()> {
    /// The walrus client.
    pub(crate) walrus_client: WalrusClient<SuiContractClient>,
    /// n_shards for this network.
    n_shards: NonZeroU16,
    /// The URL of the proxy.
    server_url: Url,
    /// The tip configuration
    tip_config: TipConfig,
    /// The gas budget.
    gas_budget: Option<u64>,
    /// The transaction builder.
    pt_builder: T,
}

impl FanOutClient {
    pub(crate) async fn from_args(
        context: Option<String>,
        walrus_config: Option<PathBuf>,
        server_url: Url,
        wallet: Option<PathBuf>,
        gas_budget: Option<u64>,
    ) -> Result<Self> {
        let tip_config = get_tip_config(&server_url).await?;
        let walrus_client = contract_client_from_args(
            walrus_config.as_ref(),
            context.as_deref(),
            wallet.as_ref(),
            gas_budget,
        )
        .await?;
        let n_shards = walrus_client.get_committees().await?.n_shards();
        tracing::info!(?tip_config, "fan-out client created");
        Ok(Self {
            walrus_client,
            n_shards,
            server_url,
            tip_config,
            gas_budget,
            pt_builder: (),
        })
    }

    /// Initializes the client with a new programmable transaction builder.
    pub(crate) fn with_pt_builder(mut self) -> Result<FanOutClient<WalrusPtbBuilder>> {
        let pt_builder = self.new_transaction_builder()?;
        let Self {
            walrus_client,
            n_shards,
            server_url,
            tip_config,
            gas_budget,
            ..
        } = self;
        Ok(FanOutClient {
            walrus_client,
            n_shards,
            server_url,
            tip_config,
            gas_budget,
            pt_builder,
        })
    }
}

impl<T> FanOutClient<T> {
    /// Encodes the blob.
    pub(crate) fn compute_metadata(&self, blob: &[u8]) -> Result<VerifiedBlobMetadataWithId> {
        Ok(self
            .walrus_client
            .encoding_config()
            // TODO: Make configurable.
            .get_for_type(EncodingType::RS2)
            .compute_metadata(blob)?)
    }

    /// Returns a new [`WalrusPtbBuilder`] for the client.
    pub(crate) fn new_transaction_builder(&mut self) -> Result<WalrusPtbBuilder> {
        Ok(WalrusPtbBuilder::new(
            Arc::new(self.walrus_client.sui_client().read_client().clone()),
            self.active_address()?,
        ))
    }

    pub(crate) fn active_address(&mut self) -> Result<SuiAddress> {
        self.walrus_client
            .sui_client_mut()
            .wallet_mut()
            .active_address()
    }

    pub(crate) fn encoded_size(&self, unencoded_length: u64) -> Option<u64> {
        self.walrus_client
            .encoding_config()
            // TODO: configurable.
            .get_for_type(EncodingType::RS2)
            .encoded_blob_length(unencoded_length)
    }

    /// Sends the blob and the transaction to the fan-out proxy.
    pub(crate) async fn send_to_proxy(
        &self,
        tx_id: TransactionDigest,
        auth_package: AuthPackage,
        blob: Vec<u8>,
        blob_id: BlobId,
    ) -> Result<ResponseType> {
        let post_url = fan_out_blob_url(&self.server_url, tx_id, auth_package, blob_id)?;

        tracing::debug!(?post_url, %blob_id, "sending request to the fan-out proxy");
        let client = reqwest::Client::new();
        let response = client.post(post_url).body(blob).send().await?;
        tracing::debug!(?response, "response received");

        let res = response.json().await?;
        Ok(res)
    }
}

impl FanOutClient<WalrusPtbBuilder> {
    /// Adds a transaction to buy and register the blob to the PTB builder.
    // NOTE: for now, this _always_ buys new storage, and registers a new non-deletable blob.
    pub(crate) async fn add_buy_and_register(
        mut self,
        metadata: &VerifiedBlobMetadataWithId,
        epochs_ahead: EpochCount,
        encoded_size: u64,
    ) -> Result<Self> {
        // TODO: add possibility to use subsidies.
        let storage_argument = self
            .pt_builder
            .reserve_space_without_subsidies(encoded_size, epochs_ahead)
            .await?;
        self.pt_builder
            .register_blob(
                storage_argument.into(),
                metadata.try_into()?,
                BlobPersistence::Permanent,
            )
            .await?;

        Ok(self)
    }

    /// Adds a call to send a tip to the fanout proxy.
    pub(crate) async fn add_tip(mut self, encoded_size: u64) -> Result<Self> {
        if let TipConfig::SendTip { address, kind } = &self.tip_config {
            let tip_amount = kind
                .compute_tip(self.n_shards, encoded_size, EncodingType::RS2)
                .expect("tip should be computable");
            self.pt_builder.pay_sui(*address, tip_amount).await?;
        }
        Ok(self)
    }

    /// Adds an input without using it for anything.
    pub(crate) fn add_pure_input<T: Serialize>(mut self, pure: T) -> Result<Self> {
        self.pt_builder.add_pure_input(pure)?;
        Ok(self)
    }

    /// Adds a call to certify a blob
    pub(crate) async fn certify_blob(
        mut self,
        blob_object: ObjectID,
        certificate: &ConfirmationCertificate,
    ) -> Result<Self> {
        self.pt_builder
            .certify_blob(blob_object.into(), certificate)
            .await?;

        Ok(self)
    }

    /// Finishes and signs the ptb.
    pub(crate) async fn finish_and_sign(mut self) -> Result<(FanOutClient, Transaction)> {
        let address = self.active_address()?;
        let Self {
            mut walrus_client,
            server_url,
            n_shards,
            tip_config,
            gas_budget,
            pt_builder,
        } = self;

        #[allow(deprecated)]
        let (programmable_transaction, sui_cost) = pt_builder.finish().await?;

        let gas_price = walrus_client
            .sui_client()
            .read_client()
            .get_reference_gas_price()
            .await?;

        // TODO: clean up the gas budget estimation and use the WalrusClient directly.
        let computed_gas_budget = if let Some(gas_budget) = gas_budget {
            gas_budget
        } else {
            let tx_kind =
                TransactionKind::ProgrammableTransaction(programmable_transaction.clone());
            walrus_client
                .sui_client()
                .sui_client()
                .estimate_gas_budget(address, tx_kind, gas_price)
                .await?
        };
        let min_gas_coin_balance = computed_gas_budget + sui_cost;

        let transaction = TransactionData::new_programmable(
            address,
            walrus_client
                .sui_client()
                .read_client()
                .get_coins_with_total_balance(address, CoinType::Sui, min_gas_coin_balance, vec![])
                .await?
                .into_iter()
                .map(|coin| coin.object_ref())
                .collect(),
            programmable_transaction,
            computed_gas_budget,
            gas_price,
        );

        let signed_transaction = walrus_client
            .sui_client_mut()
            .wallet_mut()
            .sign_transaction(&transaction);

        Ok((
            FanOutClient {
                walrus_client,
                n_shards,
                server_url,
                tip_config,
                gas_budget,
                pt_builder: (),
            },
            signed_transaction,
        ))
    }
}

pub(crate) fn fan_out_blob_url(
    server_url: &Url,
    tx_id: TransactionDigest,
    auth_package: AuthPackage,
    blob_id: BlobId,
) -> Result<Url> {
    let mut url = server_url.join(BLOB_FAN_OUT_ROUTE)?;
    url.query_pairs_mut()
        .append_pair("blob_id", &blob_id.to_string())
        .append_pair("tx_id", &tx_id.to_string())
        .append_pair(
            "auth_package",
            &URL_SAFE_NO_PAD.encode(auth_package.to_bytes()?),
        )
        .append_pair("nonce", &URL_SAFE_NO_PAD.encode(auth_package.nonce));
    Ok(url)
}
