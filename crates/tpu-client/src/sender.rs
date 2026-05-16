use {
    crate::{
        config::TpuSenderConfig,
        core::{
            ConnectionEvictionStrategy, DirectTpuConnection, DirectTpuConnectionCache,
            LeaderTpuInfoService, TpuSenderDriverSpawner, TpuSenderIdentityUpdater,
            TpuSenderResponseCallback, TpuSenderSessionContext, TpuSenderTxn,
            UpcomingLeaderPredictor, ValidatorStakeInfoService,
        },
    },
    solana_keypair::Keypair,
    solana_pubkey::Pubkey,
    std::{net::SocketAddr, sync::Arc},
    tokio::sync::{Mutex, mpsc},
};

///
/// A TPU sender that can send transactions and update its identity.
///
/// Note: The TPU sender is thread-safe, a cheap `Clone` implementation is provided to allow multiple tasks to share the same TPU sender.
/// The API of the TPU uses `&mut self` to protect against bug or errors during identity updates.
///
#[derive(Clone)]
pub struct TpuSender {
    // The identity updater shared with the TPU sender task.
    // The [`TpuSenderIdentityUpdater`] cannot be cloned or called concurrently, so we wrap it in a Mutex.
    // We do this pre-cautionarily to avoid potential issues with miss-managed identity updates.
    identity_updated: Arc<Mutex<TpuSenderIdentityUpdater>>,
    txn_tx: mpsc::Sender<TpuSenderTxn>,
    direct_connection_cache: DirectTpuConnectionCache,
}

#[derive(Debug, thiserror::Error)]
#[error("disconnected")]
pub struct TpuSenderError(TpuSenderTxn);

#[derive(Debug, thiserror::Error)]
pub enum TpuSenderTrySendError {
    #[error("transaction queue full")]
    Full(TpuSenderTxn),
    #[error("disconnected")]
    Closed(TpuSenderTxn),
}

impl TpuSender {
    ///
    /// Sends a transaction to the TPU sender task.
    ///
    pub async fn send_txn(&mut self, txn: TpuSenderTxn) -> Result<(), TpuSenderError> {
        // I put &mut self here to indicate that the caller should not be sending txns concurrently from multiple tasks.
        // This is the be consistent with the rest of the API which uses &mut self for updating identity.
        self.txn_tx.send(txn).await.map_err(|e| TpuSenderError(e.0))
    }

    ///
    /// Attempts to send a transaction to the TPU sender task without awaiting queue capacity.
    ///
    pub fn try_send_txn(&self, txn: TpuSenderTxn) -> Result<(), TpuSenderTrySendError> {
        self.txn_tx.try_send(txn).map_err(|err| match err {
            mpsc::error::TrySendError::Full(txn) => TpuSenderTrySendError::Full(txn),
            mpsc::error::TrySendError::Closed(txn) => TpuSenderTrySendError::Closed(txn),
        })
    }

    pub(crate) fn direct_connection(
        &self,
        remote_peer: &Pubkey,
        expected_addr: SocketAddr,
    ) -> Option<DirectTpuConnection> {
        let guard = self
            .direct_connection_cache
            .lock()
            .expect("direct connection cache mutex poisoned");
        let conn = guard.get(remote_peer)?;
        if conn.remote_peer_addr != expected_addr {
            return None;
        }
        Some(conn.clone())
    }

    ///
    /// Updates the identity used by the TPU sender.
    ///
    pub async fn update_identity(&mut self, new_identity: Keypair) {
        self.identity_updated
            .lock()
            .await
            .update_identity(new_identity)
            .await;
    }
}

///
/// Base factory function to create a TPU sender and its response receiver.
///
/// # Arguments
///
/// * `config` - Configuration for the TPU sender.
/// * `initial_identity` - The initial identity keypair for the TPU sender.
/// * `tpu_info_service` - Service to get TPU gossip info of leaders.
/// * `stake_map_service` - Service to get stake info of validators.
/// * `eviction_strategy` - Strategy to evict connections when needed.
/// * `leader_schedule_predictor` - Predictor for upcoming leaders.
/// * `txn_capacity` - Capacity of the transaction sender channel.
///
/// # Returns
///
/// A tuple containing the created `TpuSender` and a receiver for `TpuSenderResponse`.
/// You can drop the receiver if you don't need to handle responses.
///
/// Note: This function is `async` because it requires spawning async tasks for the TPU sender driver.
/// This function is a building block for higher-level TPU client factories.
///
#[allow(clippy::too_many_arguments)]
pub async fn create_base_tpu_client<CB>(
    config: TpuSenderConfig,
    initial_identity: Keypair,
    tpu_info_service: Arc<dyn LeaderTpuInfoService + Send + Sync>,
    stake_map_service: Arc<dyn ValidatorStakeInfoService + Send + Sync>,
    eviction_strategy: Arc<dyn ConnectionEvictionStrategy + Send + Sync>,
    leader_schedule_predictor: Arc<dyn UpcomingLeaderPredictor + Send + Sync>,
    callback: Option<CB>,
    txn_capacity: usize,
) -> TpuSender
where
    CB: TpuSenderResponseCallback,
{
    let spawner = TpuSenderDriverSpawner {
        stake_info_map: stake_map_service,
        leader_tpu_info_service: tpu_info_service,
        driver_tx_channel_capacity: txn_capacity,
    };

    let session = spawner.spawn(
        initial_identity,
        config,
        eviction_strategy,
        leader_schedule_predictor,
        callback,
    );

    let TpuSenderSessionContext {
        identity_updater,
        driver_tx_sink,
        driver_join_handle: _,
    } = session;
    let direct_connection_cache = Arc::clone(&identity_updater.direct_connection_cache);

    TpuSender {
        identity_updated: Arc::new(Mutex::new(identity_updater)),
        txn_tx: driver_tx_sink,
        direct_connection_cache,
    }
}
