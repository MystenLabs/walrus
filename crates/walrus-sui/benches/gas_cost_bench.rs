// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Allowing `unwrap`s in benchmarks.
#![allow(clippy::unwrap_used)]

//! Gas cost benchmark for `walrus-sui`.

use std::{io::Write, num::NonZeroU16, ops::Range, path::PathBuf, str::FromStr, time::Duration};

use anyhow::{anyhow, bail};
use clap::Parser;
use sui_sdk::rpc_types::{
    SuiTransactionBlockDataAPI,
    SuiTransactionBlockEffectsAPI,
    SuiTransactionBlockResponseOptions,
    SuiTransactionBlockResponseQuery,
};
use sui_types::gas::GasCostSummary;
use walrus_core::{
    BlobId,
    EncodingType,
    encoding::{EncodingConfig, EncodingConfigTrait},
    ensure,
    merkle::Node,
};
use walrus_sui::{
    client::{
        BlobObjectMetadata,
        BlobPersistence,
        CertifyAndExtendBlobParams,
        PostStoreAction,
        SuiContractClient,
    },
    test_utils::system_setup::initialize_contract_and_wallet_for_testing,
};

const MEGA_SUI: u64 = 1_000_000_000_000_000; // 1M Sui

#[derive(Debug, Clone)]
struct InputRange {
    range: Range<u32>,
    step: usize,
    exp: bool,
}

impl FromStr for InputRange {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut split = s.split(':');
        let start = split.next().map(|s| s.parse()).transpose()?.unwrap_or(1);
        let end = split
            .next()
            .ok_or_else(|| anyhow!("end of range must be provided"))?
            .parse()?;
        let step = split.next().map(|s| s.parse()).transpose()?.unwrap_or(1);
        let exp = match split.next() {
            None => false,
            Some("exp") => true,
            Some("lin") => false,
            Some(_) => bail!("invalid specifier for exponential range"),
        };
        Ok(Self {
            range: start..end,
            step,
            exp,
        })
    }
}

impl InputRange {
    fn iter<'a>(&'a self) -> impl Iterator<Item = u32> + 'a {
        self.range
            .clone()
            .step_by(self.step.clone())
            .map(|val| if self.exp { 2u32.pow(val) } else { val })
    }
}

#[derive(Debug)]
struct GasCostSummaryWithPrice {
    pub summary: GasCostSummary,
    pub price: u64,
}

#[derive(clap::Parser, Debug)]
#[command(rename_all = "kebab-case")]
struct Args {
    /// The file name of the output csv file.
    #[arg(short, long, default_value = "contract_cost.csv")]
    out: PathBuf,
    /// The number of committee members in the Walrus committee.
    ///
    /// Only matters when benchmarking the `certify` calls.
    #[arg(long, default_value = "1")]
    committee_size: NonZeroU16,
    /// Number of non-signers in the committee.
    ///
    /// Must be less than 1/3 of the committee size. Only matters when benchmarking the
    /// `certify` calls.
    #[arg(long, default_value_t = 0)]
    non_signers: u16,
    /// Benchmark the calls to `certify`. Otherwise, only `reserve_and_register` is benchmarked.
    #[arg(long)]
    with_certify: bool,
    /// Call the system contract directly, excluding the subsidies contract.
    #[arg(long)]
    no_subsidies: bool,
    /// Benchmark the calls to `extend`. This uses the epoch range for the extension instead of
    /// the initial reservation (for which one epoch will be used and no output is produced).
    #[arg(long)]
    extend: bool,
    /// The range to use for the number of blobs per call.
    #[arg(long, default_value = "1:8:1:exp")]
    n_blobs_range: InputRange,
    /// The range to use for the number of epochs to store blobs.
    #[arg(long, default_value = "1:54:8:lin")]
    n_epochs_range: InputRange,
    /// Argument passed when running `cargo bench`, is ignored.
    #[arg(long, default_value_t = true)]
    bench: bool,
}

async fn gas_cost_for_contract_calls(args: Args) -> anyhow::Result<()> {
    let mut out_file = std::fs::File::create(args.out)?;
    out_file.write_all(
        format!(
            "{},{},{},{},{},{},{},{},{}\n",
            "label",
            "epochs",
            "n_blobs",
            "gas_used",
            "net_gas_used",
            "computation_cost",
            "storage_cost",
            "storage_rebate",
            "gas_price",
        )
        .as_bytes(),
    )?;

    let (sui_cluster_handle, walrus_client, _, test_node_keys) =
        initialize_contract_and_wallet_for_testing(
            Duration::from_secs(3600),
            !args.no_subsidies,
            args.committee_size.get() as usize,
        )
        .await?;
    // Make sure that we have enough sui to pay for gas.
    sui_cluster_handle
        .lock()
        .await
        .fund_addresses_with_sui(vec![walrus_client.as_ref().address()], Some(MEGA_SUI))
        .await?;
    let contract_config = walrus_client.as_ref().read_client().contract_config();
    tracing::info!(?contract_config, "walrus client config");

    let blob_metadata = prepare_blob_metadata();

    let min_epochs_ahead = args.n_epochs_range.range.start;

    let epochs_and_blobs_per_call = args.n_epochs_range.iter().flat_map(|epoch| {
        args.n_blobs_range
            .iter()
            .map(move |n_blobs| (epoch, n_blobs))
    });

    ensure!(
        args.non_signers * 3 < args.committee_size.get(),
        "too many non-signers to certify messages"
    );
    let n_signers = args.committee_size.get() - args.non_signers;
    let signers: Vec<_> = (0..n_signers).collect();

    // loop through different number of epochs and blobs per call
    for (epochs_ahead, blobs_per_call) in epochs_and_blobs_per_call {
        tracing::info!(epochs_ahead, blobs_per_call, "running next iteration");
        let blob_metadata_vec =
            std::iter::repeat_n(blob_metadata.clone(), blobs_per_call as usize).collect();

        let reserve_epochs = if args.extend { 1 } else { epochs_ahead };

        let blob_objs = walrus_client
            .as_ref()
            .reserve_and_register_blobs(
                reserve_epochs,
                blob_metadata_vec,
                BlobPersistence::Permanent,
            )
            .await?;

        if !args.extend {
            write_data_entry(
                &mut out_file,
                "reserve_and_register",
                reserve_epochs,
                blobs_per_call,
                gas_cost_summary_for_last_tx(walrus_client.as_ref()).await?,
            )?;
        }

        // The number of epochs are irrelevant for `certify`, only benchmark for the lowest number.
        if (args.with_certify && epochs_ahead == min_epochs_ahead) || args.extend {
            let certificate =
                test_node_keys.blob_certificate_for_signers(&signers, blob_metadata.blob_id, 1)?;
            let blob_objs_with_certs: Vec<_> = blob_objs
                .iter()
                .map(|blob| (blob, certificate.clone()))
                .collect();

            walrus_client
                .as_ref()
                .certify_blobs(&blob_objs_with_certs, PostStoreAction::Keep)
                .await?;
            if args.with_certify {
                write_data_entry(
                    &mut out_file,
                    "certify",
                    epochs_ahead,
                    blobs_per_call,
                    gas_cost_summary_for_last_tx(walrus_client.as_ref()).await?,
                )?;
            }

            if args.extend {
                let epochs_extended = Some(epochs_ahead);
                let certify_and_extend_params: Vec<CertifyAndExtendBlobParams> = blob_objs
                    .iter()
                    .map(|blob| CertifyAndExtendBlobParams {
                        blob,
                        certificate: None,
                        epochs_extended,
                    })
                    .collect();
                walrus_client
                    .as_ref()
                    .certify_and_extend_blobs(&certify_and_extend_params, PostStoreAction::Keep)
                    .await?;
                write_data_entry(
                    &mut out_file,
                    "extend",
                    epochs_ahead,
                    blobs_per_call,
                    gas_cost_summary_for_last_tx(walrus_client.as_ref()).await?,
                )?;
            }
        }
    }

    Ok(())
}

fn prepare_blob_metadata() -> BlobObjectMetadata {
    let encoding_config = EncodingConfig::new(NonZeroU16::new(1000).unwrap());
    let encoding_type = EncodingType::RS2;
    let size = 10_000;
    let resource_size = encoding_config
        .get_for_type(encoding_type)
        .encoded_blob_length(size)
        .unwrap();

    #[rustfmt::skip]
    let root_hash = Node::from([
        1, 2, 3, 4, 5, 6, 7, 8,
        1, 2, 3, 4, 5, 6, 7, 8,
        1, 2, 3, 4, 5, 6, 7, 8,
        1, 2, 3, 4, 5, 6, 7, 8,
    ]);

    let blob_id = BlobId::from_metadata(root_hash.clone(), encoding_type, size);
    BlobObjectMetadata {
        blob_id,
        root_hash,
        unencoded_size: size,
        encoded_size: resource_size,
        encoding_type,
    }
}

fn write_data_entry(
    out_file: &mut impl Write,
    transaction_type: &str,
    epochs_ahead: u32,
    blobs_per_call: u32,
    gas_cost_summary: GasCostSummaryWithPrice,
) -> anyhow::Result<()> {
    let out = format!(
        "{},{},{},{},{},{},{},{},{}\n",
        transaction_type,
        epochs_ahead,
        blobs_per_call,
        gas_cost_summary.summary.gas_used(),
        gas_cost_summary.summary.net_gas_usage(),
        gas_cost_summary.summary.computation_cost,
        gas_cost_summary.summary.storage_cost,
        gas_cost_summary.summary.storage_rebate,
        gas_cost_summary.price,
    );
    out_file.write_all(out.as_bytes())?;
    Ok(())
}

async fn gas_cost_summary_for_last_tx(
    walrus_client: &SuiContractClient,
) -> anyhow::Result<GasCostSummaryWithPrice> {
    let address = walrus_client.address();
    let query = SuiTransactionBlockResponseQuery::new(
        Some(sui_sdk::rpc_types::TransactionFilter::FromAddress(address)),
        Some(
            SuiTransactionBlockResponseOptions::new()
                .with_effects()
                .with_input(),
        ),
    );
    let transaction = walrus_client
        .sui_client()
        .query_transaction_blocks(query.clone(), None, None, true)
        .await?
        .data
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("no transaction received from RPC"))?;
    let gas_price = transaction
        .transaction
        .ok_or_else(|| anyhow!("transaction block response does not contain input"))?
        .data
        .gas_data()
        .price;
    let gas_cost_summary = transaction
        .effects
        .ok_or_else(|| anyhow!("transaction block response does not contain effects"))?
        .gas_cost_summary()
        .to_owned();
    Ok(GasCostSummaryWithPrice {
        summary: gas_cost_summary,
        price: gas_price,
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    gas_cost_for_contract_calls(args).await
}
