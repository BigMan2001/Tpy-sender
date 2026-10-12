//! A Yellowstone-specific UpcomingLeaderPredictor implementation
//!
//! This module provides an implementation of the UpcomingLeaderPredictor trait
//! tailored for Yellowstone, utilizing gRPC and RPC services to track the current slot
//! and predict upcoming leaders.
//!
//! # Safety
//!
//! This module is designed to be thread-safe and can be shared across multiple tasks.
//!
//! # Poisoning
//!
//! The slot tracker/managed schedule used in this implementation can be poisoned if the background task
//! updating it panics or is dropped.
//!
use {
    crate::{
        core::UpcomingLeaderPredictor, rpc::schedule::ManagedLeaderSchedule,
        slot::AtomicSlotTracker,
    },
    solana_leader_schedule::NUM_CONSECUTIVE_LEADER_SLOTS,
    solana_pubkey::Pubkey,
    std::sync::Arc,
};

///
/// A Yellowstone-specific implementation of UpcomingLeaderPredictor
///
/// # Safety
///
/// This struct is cheaply-cloneable and can be shared between threads.
///
#[derive(Clone)]
pub struct YellowstoneUpcomingLeader {
    pub slot_tracker: Arc<AtomicSlotTracker>,
    pub managed_schedule: ManagedLeaderSchedule,
}

impl UpcomingLeaderPredictor for YellowstoneUpcomingLeader {
    fn try_predict_next_n_leaders(&self, n: usize) -> Vec<Pubkey> {
        let Ok(slot) = self.slot_tracker.load() else {
            tracing::warn!("TPU leader prediction skipped: slot tracker is unavailable");
            return Vec::new();
        };
        let consecutive = NUM_CONSECUTIVE_LEADER_SLOTS.get() as u64;
        let remainder = slot % consecutive;

        // Predict the next leader group, not the group containing the current
        // slot. On an exact boundary this intentionally advances one group.
        let next_leader_boundary = slot.saturating_add(consecutive - remainder);
        (0..n)
            .filter_map(|i| {
                let slot =
                    next_leader_boundary.saturating_add((i as u64).saturating_mul(consecutive));
                match self.managed_schedule.get_leader(slot) {
                    Ok(leader) => leader,
                    Err(_) => {
                        tracing::warn!(
                            slot,
                            "TPU leader prediction skipped: managed schedule is unavailable"
                        );
                        None
                    }
                }
            })
            .collect()
    }
}
