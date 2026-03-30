// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl WalrusPtbBuilder {
    /// Adds a call to create a new shared blob from the blob.
    pub async fn new_shared_blob(
        &mut self,
        blob_object: ArgumentOrOwnedObject,
    ) -> SuiClientResult<()> {
        let blob_arg = self.argument_from_arg_or_obj(blob_object).await?;
        self.walrus_move_call(contracts::shared_blob::new, vec![blob_arg])?;
        self.mark_arg_as_consumed(&blob_arg);
        Ok(())
    }

    /// Adds a call to create a new shared blob and fund it.
    pub async fn new_funded_shared_blob(
        &mut self,
        blob_object: ArgumentOrOwnedObject,
        amount: u64,
    ) -> SuiClientResult<()> {
        let blob_arg = self.argument_from_arg_or_obj(blob_object).await?;
        self.fill_wal_balance(amount).await?;
        let split_main_coin_arg = self.wal_coin_arg()?;
        let split_amount_arg = self.pt_builder.pure(amount)?;
        let split_coin = self.pt_builder.command(Command::SplitCoins(
            split_main_coin_arg,
            vec![split_amount_arg],
        ));
        self.walrus_move_call(
            contracts::shared_blob::new_funded,
            vec![blob_arg, split_coin],
        )?;
        self.mark_arg_as_consumed(&blob_arg);
        self.reduce_wal_balance(amount)?;
        Ok(())
    }

    /// Adds a call to fund a shared blob.
    pub async fn fund_shared_blob(
        &mut self,
        shared_blob_object_id: ObjectID,
        amount: u64,
    ) -> SuiClientResult<()> {
        let shared_blob_arg = self.pt_builder.obj(
            self.read_client
                .object_arg_for_shared_obj(shared_blob_object_id, SharedObjectMutability::Mutable)
                .await?,
        )?;
        self.fill_wal_balance(amount).await?;
        let split_main_coin_arg = self.wal_coin_arg()?;
        let split_amount_arg = self.pt_builder.pure(amount)?;
        let split_coin = self.pt_builder.command(Command::SplitCoins(
            split_main_coin_arg,
            vec![split_amount_arg],
        ));

        let args = vec![shared_blob_arg, split_coin];
        self.walrus_move_call(contracts::shared_blob::fund, args)?;
        self.reduce_wal_balance(amount)?;
        Ok(())
    }

    /// Adds a call to extend a shared blob.
    pub async fn extend_shared_blob(
        &mut self,
        shared_blob_object_id: ObjectID,
        epochs_extended: EpochCount,
    ) -> SuiClientResult<()> {
        let shared_blob_arg = self.pt_builder.obj(
            self.read_client
                .object_arg_for_shared_obj(shared_blob_object_id, SharedObjectMutability::Mutable)
                .await?,
        )?;
        let args = vec![
            shared_blob_arg,
            self.system_arg(SharedObjectMutability::Mutable)?,
            self.pt_builder.pure(epochs_extended)?,
        ];
        self.walrus_move_call(contracts::shared_blob::extend, args)?;
        Ok(())
    }
}
