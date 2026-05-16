use {
    bytes::Bytes,
    clap::Parser,
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_commitment_config::CommitmentConfig,
    solana_hash::Hash,
    solana_keypair::{Keypair, Signature},
    solana_message::{VersionedMessage, v0},
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    solana_system_interface::instruction::transfer,
    solana_transaction::versioned::VersionedTransaction,
    std::{
        env,
        io::{self, IsTerminal as _},
        num::NonZeroUsize,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicU64, AtomicUsize, Ordering},
        },
        time::Duration,
        vec,
    },
    tokio::task::JoinHandle,
    tracing::level_filters::LevelFilter,
    tracing_subscriber::{EnvFilter, layer::SubscriberExt as _, util::SubscriberInitExt},
    yellowstone_jet_tpu_client::{
        config::TpuPortKind,
        core::TpuSenderResponse,
        yellowstone_grpc::sender::{
            Endpoints, NewYellowstoneTpuSender, SendError, YellowstoneTpuSender,
            YellowstoneTpuSenderConfig, create_yellowstone_tpu_sender_with_callback,
        },
    },
};

#[derive(Debug, Clone)]
struct SentSignature {
    slot: u64,
    signature: Signature,
}

#[derive(Debug, Default)]
struct RunStats {
    scheduled: AtomicU64,
    wire_sent: AtomicU64,
    wire_failed: AtomicU64,
    wire_disallowed: AtomicU64,
    wire_dropped: AtomicU64,
    status_batches: AtomicU64,
    status_checked: AtomicU64,
    status_landed: AtomicU64,
    status_landed_with_error: AtomicU64,
    status_unconfirmed: AtomicU64,
    status_missing: AtomicU64,
    status_rpc_errors: AtomicU64,
}

#[derive(Clone)]
struct StatusClients {
    clients: Arc<Vec<Arc<RpcClient>>>,
    next: Arc<AtomicUsize>,
}

impl StatusClients {
    fn new(urls: Vec<String>) -> Self {
        let clients = urls
            .into_iter()
            .map(|url| {
                Arc::new(RpcClient::new_with_commitment(
                    url,
                    CommitmentConfig::confirmed(),
                ))
            })
            .collect::<Vec<_>>();

        assert!(
            !clients.is_empty(),
            "at least one status RPC must be configured"
        );

        Self {
            clients: Arc::new(clients),
            next: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn len(&self) -> usize {
        self.clients.len()
    }

    fn next(&self) -> Arc<RpcClient> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        Arc::clone(&self.clients[idx])
    }
}

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
enum TpuPortArg {
    Normal,
    Forwards,
}

impl From<TpuPortArg> for TpuPortKind {
    fn from(value: TpuPortArg) -> Self {
        match value {
            TpuPortArg::Normal => TpuPortKind::Normal,
            TpuPortArg::Forwards => TpuPortKind::Forwards,
        }
    }
}

fn setup_tracing() {
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    let subscriber = tracing_subscriber::registry().with(env_filter);
    let is_atty = io::stdout().is_terminal() && io::stderr().is_terminal();
    let io_layer = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_ansi(is_atty);
    subscriber.with(io_layer).try_init().expect("try_init");
}

async fn send_lamports(
    tpu_sender: &mut YellowstoneTpuSender,
    identity: &Keypair,
    recipient: &Pubkey,
    lamports: u64,
    latest_blockhash: Hash,
    send_fanout_slots: u64,
    fast_wire: bool,
) -> Result<Signature, SendError> {
    let instructions = vec![transfer(&identity.pubkey(), recipient, lamports)];

    let transaction = VersionedTransaction::try_new(
        VersionedMessage::V0(
            v0::Message::try_compile(&identity.pubkey(), &instructions, &[], latest_blockhash)
                .expect("try_compile"),
        ),
        &[identity],
    )
    .expect("try_new");
    let signature = transaction.signatures[0];
    let bincoded_txn = bincode::serialize(&transaction).expect("bincode::serialize");

    if fast_wire {
        tpu_sender
            .send_wire_transaction_fanout_slots_direct_fast(
                signature,
                Bytes::from(bincoded_txn),
                send_fanout_slots,
            )
            .await?;
    } else if send_fanout_slots == 0 {
        tpu_sender.send_txn(signature, bincoded_txn).await?;
    } else {
        tpu_sender
            .send_txn_fanout_slots(signature, bincoded_txn, send_fanout_slots)
            .await?;
    }
    Ok(signature)
}

fn spawn_status_batch(
    status_clients: StatusClients,
    stats: Arc<RunStats>,
    batch_id: u64,
    batch: Vec<SentSignature>,
    wait_secs: u64,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(wait_secs)).await;

        let signatures = batch
            .iter()
            .map(|entry| entry.signature)
            .collect::<Vec<_>>();
        let mut last_error = None;

        for _ in 0..status_clients.len() {
            let rpc_client = status_clients.next();
            match rpc_client.get_signature_statuses(&signatures).await {
                Ok(statuses) => {
                    let mut landed = 0_u64;
                    let mut landed_with_error = 0_u64;
                    let mut unconfirmed = 0_u64;
                    let mut missing = 0_u64;

                    for (entry, maybe_status) in batch.iter().zip(statuses.value.into_iter()) {
                        match maybe_status {
                            Some(status)
                                if status.satisfies_commitment(CommitmentConfig::confirmed()) =>
                            {
                                if let Some(error) = status.err {
                                    landed_with_error += 1;
                                    tracing::warn!(
                                        "batch {batch_id}: slot {} transaction {} landed in slot {} with error {error:?}",
                                        entry.slot,
                                        entry.signature,
                                        status.slot
                                    );
                                } else {
                                    landed += 1;
                                }
                            }
                            Some(status) => {
                                unconfirmed += 1;
                                tracing::debug!(
                                    "batch {batch_id}: slot {} transaction {} visible in slot {}, not confirmed yet",
                                    entry.slot,
                                    entry.signature,
                                    status.slot
                                );
                            }
                            None => {
                                missing += 1;
                            }
                        }
                    }

                    stats.status_batches.fetch_add(1, Ordering::Relaxed);
                    stats
                        .status_checked
                        .fetch_add(batch.len() as u64, Ordering::Relaxed);
                    stats.status_landed.fetch_add(landed, Ordering::Relaxed);
                    stats
                        .status_landed_with_error
                        .fetch_add(landed_with_error, Ordering::Relaxed);
                    stats
                        .status_unconfirmed
                        .fetch_add(unconfirmed, Ordering::Relaxed);
                    stats.status_missing.fetch_add(missing, Ordering::Relaxed);

                    tracing::info!(
                        "batch {batch_id}: checked {} signatures after {wait_secs}s; landed={landed}, landed_with_error={landed_with_error}, unconfirmed={unconfirmed}, missing={missing}",
                        batch.len()
                    );
                    return;
                }
                Err(error) => {
                    last_error = Some(error);
                }
            }
        }

        stats.status_rpc_errors.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            "batch {batch_id}: failed to check {} signatures with all status RPCs: {last_error:?}",
            batch.len()
        );
    })
}

fn queue_status_batch(
    status_tasks: &mut Vec<JoinHandle<()>>,
    status_clients: &StatusClients,
    stats: &Arc<RunStats>,
    next_batch_id: &mut u64,
    pending_batch: &mut Vec<SentSignature>,
    wait_secs: u64,
) {
    if pending_batch.is_empty() {
        return;
    }

    let batch_id = *next_batch_id;
    *next_batch_id += 1;
    let batch = std::mem::take(pending_batch);
    tracing::info!(
        "batch {batch_id}: queued {} signatures for status check in {wait_secs}s",
        batch.len()
    );
    status_tasks.push(spawn_status_batch(
        status_clients.clone(),
        Arc::clone(stats),
        batch_id,
        batch,
        wait_secs,
    ));
}

async fn wait_for_status_tasks(status_tasks: Vec<JoinHandle<()>>) {
    for task in status_tasks {
        if let Err(error) = task.await {
            tracing::warn!("status batch task failed: {error:?}");
        }
    }
}

fn log_summary(stats: &RunStats) {
    let scheduled = stats.scheduled.load(Ordering::Relaxed);
    let landed = stats.status_landed.load(Ordering::Relaxed);
    let landed_with_error = stats.status_landed_with_error.load(Ordering::Relaxed);
    let checked = stats.status_checked.load(Ordering::Relaxed);
    let landing_pct = if scheduled == 0 {
        0.0
    } else {
        (landed as f64 / scheduled as f64) * 100.0
    };

    tracing::info!(
        "SUMMARY scheduled={scheduled}, checked={checked}, landed={landed}, landed_with_error={landed_with_error}, landing_pct={landing_pct:.2}, missing={}, unconfirmed={}, status_batches={}, status_rpc_errors={}, wire_sent={}, wire_failed={}, wire_disallowed={}, wire_dropped={}",
        stats.status_missing.load(Ordering::Relaxed),
        stats.status_unconfirmed.load(Ordering::Relaxed),
        stats.status_batches.load(Ordering::Relaxed),
        stats.status_rpc_errors.load(Ordering::Relaxed),
        stats.wire_sent.load(Ordering::Relaxed),
        stats.wire_failed.load(Ordering::Relaxed),
        stats.wire_disallowed.load(Ordering::Relaxed),
        stats.wire_dropped.load(Ordering::Relaxed),
    );
}

fn nonzero_arg(value: usize, name: &'static str) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or_else(|| panic!("{name} must be greater than zero"))
}

fn split_rpc_urls(values: &[String]) -> Vec<String> {
    values
        .iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn status_rpc_urls(configured_urls: &[String], fallback_url: &str) -> Vec<String> {
    let mut urls = split_rpc_urls(configured_urls);
    if urls.is_empty() {
        if let Ok(env_urls) = env::var("STATUS_RPC_ENDPOINTS") {
            urls = split_rpc_urls(&[env_urls]);
        }
    }
    if urls.is_empty() {
        urls.push(fallback_url.to_owned());
    }
    urls
}

#[tokio::main]
async fn main() {
    setup_tracing();

    let args = Args::parse();
    if let Ok(env_path) = args.dotenv.unwrap_or("./.env".into()).canonicalize() {
        if dotenvy::from_path(env_path).is_err() {
            tracing::warn!("Failed to load .env file");
        }
    } else {
        tracing::warn!("Failed to canonicalize .env file path");
    }

    let rpc_endpoint = args.rpc.unwrap_or_else(|| {
        env::var("RPC_ENDPOINT").expect("RPC_ENDPOINT must be set in dotenv file or environment")
    });
    let grpc_endpoint = args.grpc.unwrap_or_else(|| {
        env::var("GRPC_ENDPOINT").expect("GRPC_ENDPOINT must be set in dotenv file or environment")
    });
    let grpc_x_token = args
        .x_token
        .or_else(|| env::var("GRPC_X_TOKEN").ok())
        .or_else(|| {
            tracing::warn!("GRPC_X_TOKEN not set in dotenv file or environment");
            None
        });
    let status_rpc_urls = status_rpc_urls(&args.status_rpcs, &rpc_endpoint);
    let status_clients = StatusClients::new(status_rpc_urls);
    let status_batch_size = nonzero_arg(args.status_batch_size, "--status-batch-size").get();
    let stats = Arc::new(RunStats::default());

    let identity = match args.identity {
        Some(path) => {
            solana_keypair::read_keypair_file(path).expect("Failed to read identity keypair file")
        }
        None => {
            let identity_path =
                env::var("IDENTITY").expect("IDENTITY must be set in dotenv file or environment");
            solana_keypair::read_keypair_file(identity_path)
                .expect("Failed to read identity keypair file from ENV")
        }
    };
    let recipient_pubkey = args
        .recipient
        .map(|recipient| recipient.parse().expect("Failed to parse recipient pubkey"))
        .unwrap_or_else(|| identity.pubkey());

    tracing::info!("using identity {}", identity.pubkey());
    tracing::info!("using recipient {}", recipient_pubkey);
    tracing::info!("using {} status RPC endpoint(s)", status_clients.len());

    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        rpc_endpoint.clone(),
        CommitmentConfig::confirmed(),
    ));
    let endpoints = Endpoints {
        rpc: rpc_endpoint.clone(),
        grpc: grpc_endpoint,
        grpc_x_token,
    };
    let mut sender_config = YellowstoneTpuSenderConfig::default();
    sender_config.tpu.num_endpoints = nonzero_arg(args.endpoint_count, "--endpoint-count");
    sender_config.tpu.max_send_attempt = nonzero_arg(args.max_send_attempts, "--max-send-attempts");
    sender_config.tpu.leader_prediction_lookahead = NonZeroUsize::new(args.leader_lookahead);
    sender_config.tpu.tpu_port = args.tpu_port.into();
    sender_config.tpu.send_fanout_slots = args.send_fanout_slots;
    tracing::info!(
        "TPU config: endpoint_count={}, max_send_attempts={}, leader_lookahead={}, tpu_port={:?}, send_fanout_slots={}, fast_wire={}",
        sender_config.tpu.num_endpoints,
        sender_config.tpu.max_send_attempt,
        args.leader_lookahead,
        sender_config.tpu.tpu_port,
        args.send_fanout_slots,
        args.fast_wire
    );

    let (callback_tx, mut callback_rx) = tokio::sync::mpsc::unbounded_channel();
    let callback_stats = Arc::clone(&stats);
    let verbose_transactions = args.verbose_transactions;
    tokio::spawn(async move {
        while let Some(response) = callback_rx.recv().await {
            match response {
                TpuSenderResponse::TxSent(resp) => {
                    callback_stats.wire_sent.fetch_add(1, Ordering::Relaxed);
                    if verbose_transactions {
                        tracing::info!(
                            "wire sent transaction {} to validator {} at {}",
                            resp.tx_sig,
                            resp.remote_peer_identity,
                            resp.remote_peer_addr
                        );
                    }
                }
                TpuSenderResponse::TxFailed(resp) => {
                    callback_stats.wire_failed.fetch_add(1, Ordering::Relaxed);
                    if resp.failure_reason.to_string().contains("disallowed") {
                        callback_stats
                            .wire_disallowed
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    if verbose_transactions {
                        tracing::warn!(
                            "wire failed transaction {} to validator {} at {}: {}",
                            resp.tx_sig,
                            resp.remote_peer_identity,
                            resp.remote_peer_addr,
                            resp.failure_reason
                        );
                    }
                }
                TpuSenderResponse::TxDrop(resp) => {
                    callback_stats
                        .wire_dropped
                        .fetch_add(resp.dropped_tx_vec.len() as u64, Ordering::Relaxed);
                    if verbose_transactions {
                        for (txn, attempt) in resp.dropped_tx_vec {
                            tracing::warn!(
                                "dropped transaction {} for validator {}, attempt {}, reason {}",
                                txn.tx_sig,
                                resp.remote_peer_identity,
                                attempt,
                                resp.drop_reason
                            );
                        }
                    }
                }
            }
        }
    });

    let NewYellowstoneTpuSender {
        mut sender,
        related_objects_jh,
    } = create_yellowstone_tpu_sender_with_callback(
        sender_config,
        identity.insecure_clone(),
        endpoints,
        callback_tx,
    )
    .await
    .expect("tpu-sender");

    let mut related_objects_jh = Box::pin(related_objects_jh);
    let mut ctrlc = Box::pin(tokio::signal::ctrl_c());
    let mut interval = tokio::time::interval(Duration::from_millis(args.poll_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut last_slot = sender.current_slot().expect("current_slot");
    tracing::info!("starting per-slot sender at slot {last_slot}");

    let mut sent_count = 0_u64;
    let mut pending_batch = Vec::<SentSignature>::with_capacity(status_batch_size);
    let mut status_tasks = Vec::<JoinHandle<()>>::new();
    let mut next_batch_id = 1_u64;

    'send_loop: loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = &mut ctrlc => {
                tracing::info!("received Ctrl-C, stopping per-slot sender");
                break 'send_loop;
            }
            result = &mut related_objects_jh => {
                tracing::error!("Yellowstone TPU sender dependency task exited: {result:?}");
                break 'send_loop;
            }
        }

        let current_slot = sender.current_slot().expect("current_slot");
        if current_slot <= last_slot {
            continue;
        }

        for slot in (last_slot + 1)..=current_slot {
            if let Some(max_slots) = args.slots {
                if sent_count >= max_slots {
                    tracing::info!("sent requested {max_slots} slots, stopping");
                    break 'send_loop;
                }
            }

            let latest_blockhash = rpc_client
                .get_latest_blockhash()
                .await
                .expect("get_latest_blockhash");
            let lamports = args.lamports_base + (slot % args.lamports_jitter);

            match send_lamports(
                &mut sender,
                &identity,
                &recipient_pubkey,
                lamports,
                latest_blockhash,
                args.send_fanout_slots,
                args.fast_wire,
            )
            .await
            {
                Ok(signature) => {
                    sent_count += 1;
                    stats.scheduled.fetch_add(1, Ordering::Relaxed);
                    if args.verbose_transactions {
                        tracing::info!(
                            "slot {slot}: scheduled transaction {signature} with {lamports} lamports ({sent_count} sent)"
                        );
                    }
                    pending_batch.push(SentSignature { slot, signature });
                    if pending_batch.len() >= status_batch_size {
                        queue_status_batch(
                            &mut status_tasks,
                            &status_clients,
                            &stats,
                            &mut next_batch_id,
                            &mut pending_batch,
                            args.status_batch_wait_secs,
                        );
                    }
                }
                Err(error) => {
                    tracing::warn!("slot {slot}: failed to schedule transaction: {error:?}");
                }
            }
        }

        last_slot = current_slot;
    }

    queue_status_batch(
        &mut status_tasks,
        &status_clients,
        &stats,
        &mut next_batch_id,
        &mut pending_batch,
        args.status_batch_wait_secs,
    );
    wait_for_status_tasks(status_tasks).await;
    log_summary(&stats);
}

#[derive(clap::Parser, Debug)]
struct Args {
    /// Path to .env file to load
    #[clap(long, short)]
    dotenv: Option<PathBuf>,

    /// Endpoint to RPC service
    #[clap(long, short)]
    rpc: Option<String>,

    /// Endpoint to Yellowstone gRPC service
    #[clap(long, short)]
    grpc: Option<String>,

    /// X-Token for Yellowstone gRPC service
    #[clap(long)]
    x_token: Option<String>,

    /// Path to identity keypair file
    #[clap(long, short)]
    identity: Option<PathBuf>,

    /// Recipient pubkey. Defaults to the identity pubkey for self-transfers.
    #[clap(long)]
    recipient: Option<String>,

    /// Stop after this many slots. Omit to run until Ctrl-C.
    #[clap(long)]
    slots: Option<u64>,

    /// Base lamports for the self-transfer.
    #[clap(long, default_value_t = 1000)]
    lamports_base: u64,

    /// Slot-based lamport jitter used to make each transaction unique.
    #[clap(long, default_value_t = 1000)]
    lamports_jitter: u64,

    /// Slot polling interval in milliseconds.
    #[clap(long, default_value_t = 50)]
    poll_ms: u64,

    /// RPC endpoint(s) used for batched getSignatureStatuses checks. Repeat or comma-separate.
    #[clap(long = "status-rpc", value_delimiter = ',')]
    status_rpcs: Vec<String>,

    /// Number of signatures per getSignatureStatuses batch.
    #[clap(long, default_value_t = 100)]
    status_batch_size: usize,

    /// Seconds to wait after the last signature in a batch before checking statuses.
    #[clap(long, default_value_t = 10)]
    status_batch_wait_secs: u64,

    /// Number of local QUIC endpoints. Keep low when probing with unstaked identity.
    #[clap(long, default_value_t = 1)]
    endpoint_count: usize,

    /// TPU QUIC contact port to use.
    #[clap(long, value_enum, default_value_t = TpuPortArg::Normal)]
    tpu_port: TpuPortArg,

    /// Fanout slot window for sending. Zero uses the Yellowstone current/next-boundary fanout.
    #[clap(long, default_value_t = 8)]
    send_fanout_slots: u64,

    /// Use the non-async fixed-buffer fast path for serialized transaction bytes.
    #[clap(long)]
    fast_wire: bool,

    /// Log every scheduled transaction and per-validator wire result.
    #[clap(long)]
    verbose_transactions: bool,

    /// Per-validator send retries after connection/stream failures.
    #[clap(long, default_value_t = 1)]
    max_send_attempts: usize,

    /// Number of upcoming leaders to pre-connect to. Zero disables prediction.
    #[clap(long, default_value_t = 2)]
    leader_lookahead: usize,
}
