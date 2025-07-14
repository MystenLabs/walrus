// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
#![allow(unused)]
//! A client for the walrus upload relay.
use std::{
    fs,
    num::NonZeroU16,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use reqwest::Url;
use serde::Serialize;
use sui_sdk::rpc_types::SuiTransactionBlockResponse;
use sui_types::{
    base_types::SuiAddress,
    digests::TransactionDigest,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{Argument, Command, Transaction, TransactionData, TransactionKind},
};
use walrus_sui::client::transaction_builder::build_transaction_data_with_min_gas_balance;

use crate::{
    ObjectID,
    client::Client as WalrusClient,
    config::{ClientConfig as WalrusConfig, load_configuration},
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
        types::{BlobEvent, BlobRegistered},
        wallet::Wallet,
    },
    upload_relay::{
        ResponseType,
        TIP_CONFIG_ROUTE,
        blob_upload_relay_url,
        params::{AuthPackage, Params},
        tip_config::{TipConfig, TipKind},
    },
};

/// Runs the upload relay client.
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
    let upload_relay_client =
        WalrusUploadRelayClient::from_args(context, walrus_config, server_url, wallet, gas_budget)
            .await?;

    let metadata = upload_relay_client.compute_metadata(&blob)?;
    let computed_blob_id = *metadata.blob_id();

    let encoded_size = upload_relay_client
        .encoded_size(metadata.metadata().unencoded_length())
        .unwrap();

    let auth_package = AuthPackage::new(&blob);

    // Transaction creation.
    let (upload_relay_client, signed_tx) = if upload_relay_client.tip_config.requires_payment() {
        upload_relay_client
            .with_pt_builder()?
            .add_pure_input(auth_package.to_hashed_nonce())?
            .add_buy_and_register(&metadata, epochs, encoded_size)
            .await?
            .add_tip(encoded_size)
            .await?
            .finish_and_sign()
            .await?
    } else {
        upload_relay_client
            .with_pt_builder()?
            .add_buy_and_register(&metadata, epochs, encoded_size)
            .await?
            .finish_and_sign()
            .await?
    };

    let response = upload_relay_client
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
    let blob_object_id = blob_registration_from_response(response, computed_blob_id)?.object_id;

    // Only add the nonce and the tx_id to the URL if necessary.
    let (nonce, tx_id) = if upload_relay_client.tip_config.requires_payment() {
        (Some(auth_package.nonce), Some(tx_id))
    } else {
        (None, None)
    };

    let params = Params {
        blob_id: computed_blob_id,
        nonce,
        tx_id,
        deletable_blob_object: None,
        encoding_type: None,
    };

    let response = upload_relay_client.send_to_proxy(blob, params).await?;

    anyhow::ensure!(
        computed_blob_id == response.blob_id,
        "the computed blob ID does not match the blob ID in the response from the proxy \
        computed={} response={}",
        computed_blob_id,
        response.blob_id
    );

    let (upload_relay_client, transaction) = upload_relay_client
        .with_pt_builder()?
        .certify_blob(blob_object_id, &response.confirmation_certificate)
        .await?
        .finish_and_sign()
        .await?;

    let result = upload_relay_client
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

    println!("Blob with ID {computed_blob_id} and encoded size {encoded_size} successfully stored");
    Ok(())
}

pub(crate) struct UploadRelayTipClient {
    /// The number of shards for this network.
    n_shards: NonZeroU16,
    /// The upload relay url.
    upload_relay: Url,
    /// The tip configuration.
    tip_config: TipConfig,
    /// The gas budget for the tip payment.
    gas_budget: Option<u64>,
}

impl UploadRelayTipClient {
    /// Fetches the tip configuration from the upload relay and creates a new upload relay tip
    /// client.
    pub(crate) async fn new(
        n_shards: NonZeroU16,
        upload_relay: Url,
        gas_budget: Option<u64>,
    ) -> Result<Self> {
        tracing::debug!(
            ?upload_relay,
            "fetching the tip confign and creating upload relay tip client"
        );
        let tip_config = get_tip_config(&upload_relay).await?;
        Ok(Self {
            n_shards,
            upload_relay,
            tip_config,
            gas_budget,
        })
    }

    /// Pays the tip to the upload relay if required.
    ///
    /// Optionally returns the transaction ID of the payment transaction.
    pub(crate) async fn pay_tip_if_required(
        &self,
        sui_client: &mut SuiContractClient,
        blob: &[u8],
        encoding_type: EncodingType,
    ) -> Result<Option<TransactionDigest>> {
        if let TipConfig::SendTip { address, kind } = &self.tip_config {
            let auth_package = AuthPackage::new(blob);
            let tx_id = self
                .pay_tip(
                    sui_client,
                    *address,
                    kind,
                    &auth_package,
                    blob.len().try_into().expect("32 or 64 bit arch"),
                    encoding_type,
                )
                .await?;
            Ok(Some(tx_id))
        } else {
            Ok(None)
        }
    }

    /// Pays the tip to the upload relay, based on the blob's unencoded length.
    ///
    /// Returns the transaction ID of the payment transaction.
    pub(crate) async fn pay_tip(
        &self,
        sui_client: &mut SuiContractClient,
        relay_address: SuiAddress,
        kind: &TipKind,
        auth_package: &AuthPackage,
        unencoded_length: u64,
        encoding_type: EncodingType,
    ) -> Result<TransactionDigest> {
        let tip_amount = kind
            .compute_tip(self.n_shards, unencoded_length, encoding_type)
            .ok_or(anyhow::anyhow!(
                "could not compute the tip amount for the given blob size \
                ({} bytes, n_shards: {}, encoding_type: {:?})",
                unencoded_length,
                self.n_shards,
                encoding_type
            ))?;
        let mut pt_builder = ProgrammableTransactionBuilder::new();

        // The first input is the authentication package.
        pt_builder.pure(auth_package.to_hashed_nonce())?;

        // Pay the tip.
        let amount_arg = pt_builder.pure(tip_amount)?;
        let split_coin =
            pt_builder.command(Command::SplitCoins(Argument::GasCoin, vec![amount_arg]));
        pt_builder.transfer_arg(relay_address, split_coin);

        // Sign and execute.
        let owner_address = sui_client.wallet_mut().active_address()?;
        let gas_price = sui_client.read_client().get_reference_gas_price().await?;
        let transaction_data = build_transaction_data_with_min_gas_balance(
            pt_builder.finish(),
            gas_price,
            sui_client.read_client(),
            owner_address,
            self.gas_budget,
            0, // No additional gas budget.
            tip_amount,
        )
        .await?;

        let signed_transaction = sui_client.wallet_mut().sign_transaction(&transaction_data);
        let response = sui_client
            .sui_client()
            .execute_transaction(signed_transaction, "pay_tip")
            .await?;

        Ok(response.digest)
    }
}

/// Gets the tip configuration from the specified Walrus Upload Relay.
pub async fn get_tip_config(server_url: &Url) -> Result<TipConfig> {
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
    let walrus_config = load_configuration(walrus_config.as_ref(), context);

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

/// A client to send blobs and tips to the walrus upload relay.
pub(crate) struct WalrusUploadRelayClient<T = ()> {
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

impl WalrusUploadRelayClient {
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
        tracing::info!(?tip_config, "walrus upload relay client created");
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
    pub(crate) fn with_pt_builder(mut self) -> Result<WalrusUploadRelayClient<WalrusPtbBuilder>> {
        let pt_builder = self.new_transaction_builder()?;
        let Self {
            walrus_client,
            n_shards,
            server_url,
            tip_config,
            gas_budget,
            ..
        } = self;
        Ok(WalrusUploadRelayClient {
            walrus_client,
            n_shards,
            server_url,
            tip_config,
            gas_budget,
            pt_builder,
        })
    }
}

impl<T> WalrusUploadRelayClient<T> {
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

    /// Sends the blob and the transaction to the walrus upload relay.
    pub(crate) async fn send_to_proxy(
        &self,
        blob: Vec<u8>,
        params: Params,
    ) -> Result<ResponseType> {
        let post_url = blob_upload_relay_url(&self.server_url, &params)?;

        tracing::debug!(
            ?post_url,
            ?params,
            "sending request to the walrus upload relay"
        );
        let client = reqwest::Client::new();
        let response = client.post(post_url).body(blob).send().await?;
        tracing::debug!(?response, "response received");

        let res = response.json().await?;
        Ok(res)
    }
}

impl WalrusUploadRelayClient<WalrusPtbBuilder> {
    /// Adds a transaction to buy and register the blob to the PTB builder.
    // NOTE: for now, this _always_ buys new storage, and registers a new non-deletable blob.
    pub(crate) async fn add_buy_and_register(
        mut self,
        metadata: &VerifiedBlobMetadataWithId,
        epochs_ahead: EpochCount,
        encoded_size: u64,
    ) -> Result<Self> {
        // NB: this does not support usage subsidies as for now this module is primarily used for
        // manual testing and development.
        let storage_argument = self
            .pt_builder
            .reserve_space(encoded_size, epochs_ahead)
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

    /// Adds a call to send a tip to the Walrus Upload Relay.
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
    pub(crate) async fn finish_and_sign(
        mut self,
    ) -> Result<(WalrusUploadRelayClient, Transaction)> {
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
        // REVIEW(will): unclear what needs to be cleaned up.
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
            WalrusUploadRelayClient {
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

/// Returns the blob registration for the given blob ID.
///
/// If the expected registration is found, the function will return the registration
/// information. Otherwise, it will return an error.
pub(crate) fn blob_registration_from_response(
    response: SuiTransactionBlockResponse,
    expected_blob_id: BlobId,
) -> anyhow::Result<BlobRegistered> {
    let registrations = blob_registrations_from_response(response);

    registrations
        .into_iter()
        .find(|reg| reg.blob_id == expected_blob_id)
        .ok_or(anyhow::anyhow!(
            "could not find the registration for the given blob ID"
        ))
}

/// Returns all the blob events contained in the response.
pub(crate) fn blob_registrations_from_response(
    response: SuiTransactionBlockResponse,
) -> Vec<BlobRegistered> {
    let Some(events) = response.events else {
        return vec![];
    };

    events
        .data
        .into_iter()
        .filter_map(|sui_event| {
            sui_event.try_into().ok().and_then(|blob_event| {
                if let BlobEvent::Registered(registration) = blob_event {
                    Some(registration)
                } else {
                    None
                }
            })
        })
        .collect::<Vec<_>>()
}
