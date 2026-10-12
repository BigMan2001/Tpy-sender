use {
    futures::future::join,
    serde::Deserialize,
    solana_client::{client_error, nonblocking::rpc_client::RpcClient},
    solana_clock::DEFAULT_SLOTS_PER_EPOCH,
    solana_leader_schedule::NUM_CONSECUTIVE_LEADER_SLOTS,
    solana_pubkey::Pubkey,
    std::{
        str::FromStr,
        sync::{
            Arc, RwLock,
            atomic::{AtomicBool, Ordering},
        },
    },
    tokio::task::JoinHandle,
};

pub const DEFAULT_AUTO_LEADER_SCHEDULE_CHECK_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(30);
pub const DEFAULT_MISSING_NEXT_SCHEDULE_RETRY_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(5 * 60);

///
/// A compact representation of the leader schedule for an epoch,
/// where each leader pubkey appears once for every 4 consecutive slots they lead.
///
#[derive(Clone, Debug)]
pub struct CompactSortedSchedule {
    pub first_slot: u64,
    schedule: Vec<Pubkey>,
}

impl CompactSortedSchedule {
    fn empty_for_epoch(epoch: u64) -> Self {
        Self {
            first_slot: epoch.saturating_mul(DEFAULT_SLOTS_PER_EPOCH),
            schedule: Vec::new(),
        }
    }

    fn epoch(&self) -> u64 {
        self.first_slot / DEFAULT_SLOTS_PER_EPOCH
    }

    fn is_complete(&self) -> bool {
        self.schedule.len() == schedule_round_count()
    }

    #[inline]
    pub fn last_slot(&self) -> u64 {
        self.first_slot + DEFAULT_SLOTS_PER_EPOCH - 1
    }

    ///
    /// Get the leader pubkey for a given slot in the epoch
    ///
    pub fn get(&self, slot: &u64) -> Option<&Pubkey> {
        let first_slot = self.first_slot;
        let max_slot = first_slot + DEFAULT_SLOTS_PER_EPOCH;
        if slot < &first_slot || slot >= &max_slot {
            return None;
        }
        let slot_idx = slot.saturating_sub(first_slot);
        let slot_idx = slot_idx / NUM_CONSECUTIVE_LEADER_SLOTS.get() as u64;
        // Get the nearest consecutive-leader boundary.
        self.schedule.get(slot_idx as usize)
    }

    ///
    /// Iterator over (slot, leader_pubkey) pairs for the entire epoch
    ///
    pub fn iter_unnested_schedule(&self) -> impl Iterator<Item = (u64, &Pubkey)> {
        let first_slot = self.first_slot;
        self.schedule
            .iter()
            .enumerate()
            .flat_map(move |(slot_idx, pubk)| {
                (0..NUM_CONSECUTIVE_LEADER_SLOTS.get()).map(move |i| {
                    let slot = first_slot
                        + (slot_idx as u64 * NUM_CONSECUTIVE_LEADER_SLOTS.get() as u64)
                        + i as u64;
                    (slot, pubk)
                })
            })
    }
}

fn schedule_round_count() -> usize {
    let consecutive = NUM_CONSECUTIVE_LEADER_SLOTS.get();
    (DEFAULT_SLOTS_PER_EPOCH as usize).div_ceil(consecutive)
}

pub fn unnest_rpc_get_leader_schedule_resp<'a, 'iter>(
    slot: u64,
    resp: impl IntoIterator<Item = (&'iter String, &'iter Vec<usize>)>,
) -> Option<CompactSortedSchedule>
where
    'a: 'iter,
{
    let consecutive = NUM_CONSECUTIVE_LEADER_SLOTS.get();
    let mut rounds = vec![None; schedule_round_count()];

    for (pubkey_str, sorted_slot_idx) in resp {
        let pubkey = Pubkey::from_str(pubkey_str.as_str()).ok()?;
        for slot_idx in sorted_slot_idx {
            if *slot_idx >= DEFAULT_SLOTS_PER_EPOCH as usize {
                return None;
            }
            if *slot_idx % consecutive != 0 {
                continue;
            }
            let round_idx = *slot_idx / consecutive;
            match rounds.get_mut(round_idx)? {
                slot @ None => *slot = Some(pubkey),
                Some(existing) if *existing == pubkey => {}
                Some(_) => return None,
            }
        }
    }

    let schedule = rounds.into_iter().collect::<Option<Vec<_>>>()?;
    let epoch = slot / DEFAULT_SLOTS_PER_EPOCH;

    Some(CompactSortedSchedule {
        first_slot: epoch * DEFAULT_SLOTS_PER_EPOCH,
        schedule,
    })
}

#[async_trait::async_trait]
pub trait ScheduleExt {
    ///
    /// Get the leader schedule for the epoch containing `slot_ctx`,
    /// unnesting the RPC response into a [`CompactSortedSchedule`].
    ///
    async fn get_unnested_leader_schedule(
        &self,
        slot_ctx: Option<u64>,
    ) -> Result<Option<CompactSortedSchedule>, client_error::ClientError>;
}

#[async_trait::async_trait]
impl ScheduleExt for RpcClient {
    async fn get_unnested_leader_schedule(
        &self,
        slot_ctx: Option<u64>,
    ) -> Result<Option<CompactSortedSchedule>, client_error::ClientError> {
        let referenced_slot = match slot_ctx {
            Some(slot) => slot,
            None => {
                let info = self.get_epoch_info().await?;
                info.absolute_slot
            }
        };

        Ok(self
            .get_leader_schedule(Some(referenced_slot))
            .await?
            .and_then(|nested| {
                let parsed = unnest_rpc_get_leader_schedule_resp(referenced_slot, nested.iter());
                if parsed.is_none() {
                    tracing::warn!(
                        referenced_slot,
                        "TPU leader schedule RPC response was incomplete or invalid"
                    );
                }
                parsed
            }))
    }
}

#[derive(Debug)]
struct InnerManagedLeaderSchedule {
    double_buffer: [CompactSortedSchedule; 2],
    fail: AtomicBool,
}

///
/// A managed leader schedule that automatically updates as epochs progress.
///
/// See [`spawn_managed_leader_schedule`] for spawning the background update task.
///
/// # Safety
///
/// This struct uses internal synchronization to allow concurrent access from multiple tasks.
///
/// # Clone
///
/// You can clone (cheaply) the `ManagedLeaderSchedule` to share it across multiple tasks.
///
#[derive(Clone)]
pub struct ManagedLeaderSchedule {
    inner: Arc<RwLock<InnerManagedLeaderSchedule>>,
}

///
/// Error indicating that the AutoLeaderSchedule background update task has failed.
///
#[derive(Debug, thiserror::Error)]
#[error("auto leader schedule poisoned")]
pub struct PoisonError;

#[derive(Debug, thiserror::Error)]
pub enum ManagedLeaderScheduleError {
    #[error(transparent)]
    Rpc(#[from] client_error::ClientError),
    #[error("leader schedule for the current epoch is unavailable or incomplete")]
    CurrentScheduleUnavailable,
}

impl ManagedLeaderSchedule {
    ///
    /// Get the leader for a given slot.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(Pubkey))` if the leader for the slot is found.
    /// - `Ok(None)` if the slot is out of range of the current schedules.
    /// - `Err(PoisonError)` if the background update task has failed.
    ///
    /// # Errors
    ///
    /// Returns `PoisonError` if the background update task has failed.
    ///
    pub fn get_leader(&self, slot: u64) -> Result<Option<Pubkey>, PoisonError> {
        let schedules = self.inner.read().map_err(|_| PoisonError)?;
        // Relaxed ordering is sufficient here since fail does not protect any data.
        // We already use RwLock to protect the double_buffer data.
        if schedules.fail.load(Ordering::Relaxed) {
            return Err(PoisonError);
        }
        let schedule = if slot >= schedules.double_buffer[1].first_slot {
            &schedules.double_buffer[1]
        } else {
            &schedules.double_buffer[0]
        };
        Ok(schedule.get(&slot).cloned())
    }
}

fn matching_complete_schedule(
    schedule: Option<CompactSortedSchedule>,
    epoch: u64,
) -> Option<CompactSortedSchedule> {
    schedule.filter(|schedule| schedule.epoch() == epoch && schedule.is_complete())
}

fn installed_schedule_for_epoch(
    schedules: &InnerManagedLeaderSchedule,
    epoch: u64,
) -> Option<CompactSortedSchedule> {
    schedules
        .double_buffer
        .iter()
        .find(|schedule| schedule.epoch() == epoch && schedule.is_complete())
        .cloned()
}

/// Atomically installs a coherent current/next pair. A successfully prefetched
/// next schedule is promoted immediately on an epoch transition even when RPC
/// is temporarily unavailable. Missing data is represented by an empty
/// schedule for the correct epoch, never by a schedule from the wrong epoch.
fn install_schedule_refresh(
    schedules: &mut InnerManagedLeaderSchedule,
    epoch: u64,
    current: Option<CompactSortedSchedule>,
    next: Option<CompactSortedSchedule>,
) -> (bool, bool) {
    let next_epoch = epoch.saturating_add(1);
    let current = matching_complete_schedule(current, epoch)
        .or_else(|| installed_schedule_for_epoch(schedules, epoch))
        .unwrap_or_else(|| CompactSortedSchedule::empty_for_epoch(epoch));
    let next = matching_complete_schedule(next, next_epoch)
        .or_else(|| installed_schedule_for_epoch(schedules, next_epoch))
        .unwrap_or_else(|| CompactSortedSchedule::empty_for_epoch(next_epoch));
    let current_complete = current.is_complete();
    let next_complete = next.is_complete();
    schedules.double_buffer = [current, next];
    (current_complete, next_complete)
}

async fn auto_leader_schedule_loop(
    config: ManagedLeaderScheduleConfig,
    shared: Arc<RwLock<InnerManagedLeaderSchedule>>,
    rpc_client: Arc<RpcClient>,
    cancellation_token: tokio_util::sync::CancellationToken,
) {
    pub struct OnDrop {
        shared: Option<Arc<RwLock<InnerManagedLeaderSchedule>>>,
    }

    impl Drop for OnDrop {
        fn drop(&mut self) {
            if let Some(shared) = self.shared.take() {
                let Ok(schedules) = shared.read() else {
                    return;
                };
                // Since we are already synchronously dropping (RwLock), it's safe to use Relaxed ordering here.
                // This trick have been taking from std::sync::poison implementation!
                schedules.fail.store(true, Ordering::Relaxed);
            }
        }
    }

    let _poison_on_drop = OnDrop {
        shared: Some(Arc::clone(&shared)),
    };
    let mut last_observed_epoch = shared
        .read()
        .map(|schedules| schedules.double_buffer[0].epoch())
        .unwrap_or_default();
    // Startup already attempted to fetch the next epoch. If it is not
    // published yet, do not hammer getLeaderSchedule on every cheap epoch
    // check. A newly observed epoch gets one immediate prefetch attempt for
    // its successor; otherwise a missing next schedule is retried at this
    // bounded cadence.
    let mut next_retry_epoch = last_observed_epoch.saturating_add(1);
    let mut next_retry_not_before =
        tokio::time::Instant::now() + DEFAULT_MISSING_NEXT_SCHEDULE_RETRY_INTERVAL;

    loop {
        tokio::select! {
            _ = tokio::time::sleep(config.check_interval) => { }
            _ = cancellation_token.cancelled() => {
                tracing::info!("AutoLeaderSchedule: cancellation requested, exiting loop");
                break;
            }
        }

        let epoch = match rpc_client.get_epoch_info().await {
            Ok(info) => info.epoch,
            Err(error) => {
                tracing::warn!(
                    ?error,
                    "[TPU_SCHEDULE][REFRESH_FAILED] get_epoch_info failed; retaining installed schedules"
                );
                continue;
            }
        };

        let epoch_changed = epoch != last_observed_epoch;
        if epoch_changed {
            tracing::info!(
                "AutoLeaderSchedule: detected epoch change {} -> {}",
                last_observed_epoch,
                epoch
            );
            last_observed_epoch = epoch;
        }

        let first_slot_current_epoch = epoch.saturating_mul(DEFAULT_SLOTS_PER_EPOCH);
        let next_epoch = epoch.saturating_add(1);
        let next_epoch_first_slot = next_epoch.saturating_mul(DEFAULT_SLOTS_PER_EPOCH);
        let (needs_current, needs_next) = match shared.read() {
            Ok(schedules) => (
                installed_schedule_for_epoch(&schedules, epoch).is_none(),
                installed_schedule_for_epoch(&schedules, next_epoch).is_none(),
            ),
            Err(_) => {
                tracing::error!("[TPU_SCHEDULE][REFRESH_FAILED] schedule lock poisoned");
                break;
            }
        };

        // Leader schedules are immutable for an epoch. Poll epoch info often,
        // but only download the large complete schedule response for a missing
        // epoch. This makes transition recovery fast without continuously
        // transferring two full schedules.
        if next_retry_epoch != next_epoch {
            next_retry_epoch = next_epoch;
            next_retry_not_before = tokio::time::Instant::now();
        }
        let fetch_next = needs_next && tokio::time::Instant::now() >= next_retry_not_before;
        if fetch_next {
            next_retry_not_before =
                tokio::time::Instant::now() + DEFAULT_MISSING_NEXT_SCHEDULE_RETRY_INTERVAL;
        }

        let (current_result, next_result) = join(
            async {
                if needs_current {
                    Some(
                        rpc_client
                            .get_unnested_leader_schedule(Some(first_slot_current_epoch))
                            .await,
                    )
                } else {
                    None
                }
            },
            async {
                if fetch_next {
                    Some(
                        rpc_client
                            .get_unnested_leader_schedule(Some(next_epoch_first_slot))
                            .await,
                    )
                } else {
                    None
                }
            },
        )
        .await;
        let refresh_attempted = current_result.is_some() || next_result.is_some();

        let current_schedule = match current_result {
            Some(Ok(schedule)) => schedule,
            Some(Err(error)) => {
                tracing::warn!(
                    epoch,
                    ?error,
                    "[TPU_SCHEDULE][CURRENT_REFRESH_FAILED] retaining installed schedule"
                );
                None
            }
            None => None,
        };
        let next_schedule = match next_result {
            Some(Ok(schedule)) => schedule,
            Some(Err(error)) => {
                tracing::warn!(
                    next_epoch,
                    ?error,
                    "[TPU_SCHEDULE][NEXT_REFRESH_FAILED] retaining installed schedule"
                );
                None
            }
            None => None,
        };

        if !epoch_changed && !refresh_attempted {
            continue;
        }

        let Ok(mut schedules) = shared.write() else {
            tracing::error!("[TPU_SCHEDULE][REFRESH_FAILED] schedule lock poisoned");
            break;
        };
        let (current_complete, next_complete) =
            install_schedule_refresh(&mut schedules, epoch, current_schedule, next_schedule);
        tracing::info!(
            epoch,
            next_epoch,
            current_complete,
            next_complete,
            "[TPU_SCHEDULE][REFRESHED] installed complete schedule state"
        );
        if !current_complete {
            tracing::error!(
                epoch,
                "[TPU_SCHEDULE][CURRENT_UNAVAILABLE] no complete current schedule is installed"
            );
        } else if !next_complete {
            tracing::warn!(
                next_epoch,
                "[TPU_SCHEDULE][NEXT_UNAVAILABLE] next schedule is not published yet"
            );
        }
    }
}

///
/// Configuration for spawning a managed leader schedule.
///
/// See [`spawn_managed_leader_schedule`].
#[derive(Debug, Clone, Deserialize)]
pub struct ManagedLeaderScheduleConfig {
    /// How long to wait before checking for a new epoch schedule
    #[serde(
        with = "humantime_serde",
        default = "ManagedLeaderScheduleConfig::default_check_interval"
    )]
    pub check_interval: std::time::Duration,
}

impl ManagedLeaderScheduleConfig {
    ///
    /// Default check interval duration.
    ///
    pub fn default_check_interval() -> std::time::Duration {
        DEFAULT_AUTO_LEADER_SCHEDULE_CHECK_INTERVAL
    }
}

impl Default for ManagedLeaderScheduleConfig {
    fn default() -> Self {
        Self {
            check_interval: Self::default_check_interval(),
        }
    }
}

///
/// Spawn a managed leader schedule that automatically updates as epochs progress.
///
pub async fn spawn_managed_leader_schedule(
    rpc_client: Arc<RpcClient>,
    config: ManagedLeaderScheduleConfig,
) -> Result<(ManagedLeaderSchedule, JoinHandle<()>), ManagedLeaderScheduleError> {
    let initial_schedule = rpc_client
        .get_unnested_leader_schedule(None)
        .await?
        .ok_or(ManagedLeaderScheduleError::CurrentScheduleUnavailable)?;

    let current_epoch = initial_schedule.epoch();
    let next_epoch = current_epoch.saturating_add(1);
    let next_epoch_first_slot = next_epoch.saturating_mul(DEFAULT_SLOTS_PER_EPOCH);

    let next_schedule = match rpc_client
        .get_unnested_leader_schedule(Some(next_epoch_first_slot))
        .await
    {
        Ok(Some(schedule)) => schedule,
        Ok(None) => {
            tracing::warn!(
                next_epoch,
                "[TPU_SCHEDULE][NEXT_UNAVAILABLE] starting without unpublished next schedule"
            );
            CompactSortedSchedule::empty_for_epoch(next_epoch)
        }
        Err(error) => {
            tracing::warn!(
                next_epoch,
                ?error,
                "[TPU_SCHEDULE][NEXT_REFRESH_FAILED] starting with current schedule only"
            );
            CompactSortedSchedule::empty_for_epoch(next_epoch)
        }
    };

    let shared = Arc::new(RwLock::new(InnerManagedLeaderSchedule {
        double_buffer: [initial_schedule, next_schedule],
        fail: AtomicBool::new(false),
    }));

    let shared_clone = shared.clone();
    let cancellation_token = tokio_util::sync::CancellationToken::new();
    let loop_ct = cancellation_token.clone();
    let jh = tokio::spawn(async move {
        auto_leader_schedule_loop(config, shared_clone, rpc_client, loop_ct).await;
    });

    Ok((ManagedLeaderSchedule { inner: shared }, jh))
}

#[cfg(test)]
mod tests {
    use {
        rand::distr::{Distribution, weighted::WeightedIndex},
        solana_clock::DEFAULT_SLOTS_PER_EPOCH,
        solana_leader_schedule::NUM_CONSECUTIVE_LEADER_SLOTS,
        solana_pubkey::Pubkey,
        std::collections::BTreeMap,
    };

    fn complete_schedule(epoch: u64, leader: Pubkey) -> super::CompactSortedSchedule {
        super::CompactSortedSchedule {
            first_slot: epoch * DEFAULT_SLOTS_PER_EPOCH,
            schedule: vec![leader; super::schedule_round_count()],
        }
    }

    fn exponential_distribution(n: usize, base: f64, r: f64) -> Vec<u64> {
        // returns a vector of stake weights
        (0..n).map(|i| (base * r.powi(i as i32)) as u64).collect()
    }

    #[test]
    fn test_unnest_rpc_get_leader_schedule_resp() {
        let consecutive = NUM_CONSECUTIVE_LEADER_SLOTS.get();
        let validator_rounds = super::schedule_round_count();
        const N: usize = 100;
        let stakes = exponential_distribution(N, 1_000_000.0, 0.97);
        let dist = WeightedIndex::new(&stakes).unwrap();

        let validator_keys = (0..N)
            .map(|_| Pubkey::new_unique())
            .collect::<Vec<Pubkey>>();

        let mut rng = rand::rng();
        let mut nested_schedule: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for round in 0..validator_rounds {
            let chosen_idx = dist.sample(&mut rng);
            let chosen_key = &validator_keys[chosen_idx];
            let entries = nested_schedule.entry(chosen_key.to_string()).or_default();

            for i in 0..consecutive {
                let slot_idx = round * consecutive + i;
                entries.push(slot_idx);
            }
        }

        let compact_schedule =
            super::unnest_rpc_get_leader_schedule_resp(0, nested_schedule.iter()).unwrap();

        assert!(compact_schedule.first_slot == 0);

        let mut actual: BTreeMap<String, Vec<usize>> = Default::default();

        for (slot, pubkey) in compact_schedule.iter_unnested_schedule() {
            let pubkey_str = pubkey.to_string();
            assert!(
                nested_schedule.contains_key(&pubkey_str),
                "Unknown pubkey {pubkey_str}",
            );
            // println!("Slot {} assigned to leader {}", slot, pubkey_str);
            actual.entry(pubkey_str).or_default().push(slot as usize);
        }

        // // assert keys perflectly match
        assert_eq!(nested_schedule.len(), actual.len());
        let diff = nested_schedule
            .keys()
            .filter(|k| !actual.contains_key(*k))
            .collect::<Vec<_>>();

        assert!(diff.is_empty(), "Mismatched keys: {diff:?}");
        assert_eq!(nested_schedule, actual);
    }

    #[test]
    fn rejects_incomplete_or_conflicting_schedule() {
        let leader_a = Pubkey::new_unique();
        let leader_b = Pubkey::new_unique();
        let mut incomplete = BTreeMap::new();
        incomplete.insert(
            leader_a.to_string(),
            (0..NUM_CONSECUTIVE_LEADER_SLOTS.get()).collect(),
        );
        assert!(
            super::unnest_rpc_get_leader_schedule_resp(0, incomplete.iter()).is_none(),
            "an incomplete epoch must not be installed"
        );

        let mut conflicting = BTreeMap::new();
        conflicting.insert(
            leader_a.to_string(),
            (0..DEFAULT_SLOTS_PER_EPOCH as usize).collect(),
        );
        conflicting.insert(leader_b.to_string(), vec![0]);
        assert!(
            super::unnest_rpc_get_leader_schedule_resp(0, conflicting.iter()).is_none(),
            "two leaders for one boundary must not be installed"
        );
    }

    #[test]
    fn promotes_prefetched_next_schedule_when_rpc_is_unavailable() {
        let epoch = 100;
        let old_leader = Pubkey::new_unique();
        let promoted_leader = Pubkey::new_unique();
        let mut schedules = super::InnerManagedLeaderSchedule {
            double_buffer: [
                complete_schedule(epoch, old_leader),
                complete_schedule(epoch + 1, promoted_leader),
            ],
            fail: std::sync::atomic::AtomicBool::new(false),
        };

        let (current_complete, next_complete) =
            super::install_schedule_refresh(&mut schedules, epoch + 1, None, None);

        assert!(current_complete);
        assert!(!next_complete);
        assert_eq!(schedules.double_buffer[0].epoch(), epoch + 1);
        assert_eq!(
            schedules.double_buffer[0].get(&((epoch + 1) * DEFAULT_SLOTS_PER_EPOCH)),
            Some(&promoted_leader)
        );
        assert_eq!(schedules.double_buffer[1].epoch(), epoch + 2);
        assert!(schedules.double_buffer[1].schedule.is_empty());
    }

    #[test]
    fn refresh_replaces_both_epochs_atomically() {
        let epoch = 200;
        let old_current = Pubkey::new_unique();
        let old_next = Pubkey::new_unique();
        let new_current = Pubkey::new_unique();
        let new_next = Pubkey::new_unique();
        let mut schedules = super::InnerManagedLeaderSchedule {
            double_buffer: [
                complete_schedule(epoch, old_current),
                complete_schedule(epoch + 1, old_next),
            ],
            fail: std::sync::atomic::AtomicBool::new(false),
        };

        let completeness = super::install_schedule_refresh(
            &mut schedules,
            epoch,
            Some(complete_schedule(epoch, new_current)),
            Some(complete_schedule(epoch + 1, new_next)),
        );

        assert_eq!(completeness, (true, true));
        assert_eq!(
            schedules.double_buffer[0].get(&(epoch * DEFAULT_SLOTS_PER_EPOCH)),
            Some(&new_current)
        );
        assert_eq!(
            schedules.double_buffer[1].get(&((epoch + 1) * DEFAULT_SLOTS_PER_EPOCH)),
            Some(&new_next)
        );
    }
}
