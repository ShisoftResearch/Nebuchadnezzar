//! Neb's half of the Phase 4 node manager: the [`FillFabric`] the coordinator
//! in bifrost drives, and the join watcher that triggers it.
//!
//! Split per the decision record
//! (`docs/superpowers/specs/2026-08-19-phase-4-node-manager-decision.md`):
//! bifrost decides WHICH slots move; this module answers what they weigh,
//! whether the recipient will take them, and actually moves them.

use crate::client::AsyncClient;
use crate::migration::{reshard_slots, MigrationPlan};
use bifrost::conshash::fill::{FillFabric, FillReport, JoinFiller, PlacementView};
use futures::future::BoxFuture;
use futures::FutureExt;
use std::sync::Arc;

pub struct NebFillFabric {
    client: Arc<AsyncClient>,
    plan: MigrationPlan,
}

impl NebFillFabric {
    pub fn new(client: Arc<AsyncClient>) -> Self {
        Self {
            client,
            plan: MigrationPlan::default(),
        }
    }
}

impl FillFabric for NebFillFabric {
    fn member_totals(&self, members: &[u64]) -> BoxFuture<'_, Result<Vec<u64>, String>> {
        let members = members.to_vec();
        async move {
            let mut totals = Vec::with_capacity(members.len());
            for member in members {
                let cells = self
                    .client
                    .client_by_server_id(member)
                    .await
                    .map_err(|e| format!("no client for member {member}: {e:?}"))?;
                totals.push(
                    cells
                        .total_live_bytes()
                        .await
                        .map_err(|e| format!("member {member} total_live_bytes: {e:?}"))?,
                );
            }
            Ok(totals)
        }
        .boxed()
    }

    fn slot_bytes(&self, member: u64, slots: &[u32]) -> BoxFuture<'_, Result<Vec<u64>, String>> {
        let slots = slots.to_vec();
        async move {
            let cells = self
                .client
                .client_by_server_id(member)
                .await
                .map_err(|e| format!("no client for member {member}: {e:?}"))?;
            cells
                .slot_live_bytes(&slots)
                .await
                .map_err(|e| format!("member {member} slot_live_bytes: {e:?}"))
        }
        .boxed()
    }

    fn admit(&self, recipient: u64, bytes: u64) -> BoxFuture<'_, Result<bool, String>> {
        async move {
            let cells = self
                .client
                .client_by_server_id(recipient)
                .await
                .map_err(|e| format!("no client for recipient {recipient}: {e:?}"))?;
            cells
                .can_admit_bytes(bytes)
                .await
                .map_err(|e| format!("recipient {recipient} can_admit_bytes: {e:?}"))
        }
        .boxed()
    }

    fn move_slots(&self, slots: &[u32], from: u64, to: u64) -> BoxFuture<'_, Result<(), String>> {
        let slots = slots.to_vec();
        async move {
            let reshard = reshard_slots(&self.client, &slots, from, to, &self.plan).await;
            if !reshard.failed.is_empty() {
                return Err(format!(
                    "{} of {} slots failed to move: {:?}",
                    reshard.failed.len(),
                    slots.len(),
                    reshard.failed
                ));
            }
            // The Phase 4 contract: a retained cell means the reclaim could
            // not verify the recipient's copy, and an automatic mover must
            // STOP rather than keep going on a cluster that cannot verify its
            // transfers. Nothing is lost -- the donor kept the copies -- but
            // continuing is an operator decision, not this driver's.
            let retained: usize = reshard.reclaims.iter().map(|r| r.retained).sum();
            if retained > 0 {
                return Err(format!(
                    "reclaim retained {retained} unverified cell(s); the fill must stop \
                     and an operator must look before anything else moves"
                ));
            }
            Ok(())
        }
        .boxed()
    }
}

/// How long a join must stand before the fill starts. A member bouncing at
/// startup should not trigger a transfer per bounce; a real joiner does not
/// care about a few seconds. Overridable for tests.
fn stability_window() -> std::time::Duration {
    let ms = std::env::var("NEB_JOIN_FILL_DELAY_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(10_000);
    std::time::Duration::from_millis(ms)
}

/// How long to wait for a freshly joined member to start SERVING before giving
/// up on filling it.
fn readiness_window() -> std::time::Duration {
    let ms = std::env::var("NEB_JOIN_FILL_READY_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(120_000);
    std::time::Duration::from_millis(ms)
}

/// How long to wait for a joining member to start SERVING before giving up on
/// filling it.
fn readiness_timeout() -> std::time::Duration {
    let ms = std::env::var("NEB_JOIN_FILL_READY_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(120_000);
    std::time::Duration::from_millis(ms)
}

impl crate::server::NebServer {
    /// Fill `joiner` toward the cluster's mean live bytes, leader-gated.
    ///
    /// Everything else about rebalancing stays operator-invoked; this is the
    /// one trigger with an unambiguous right answer, because a joiner owns
    /// nothing by design. Safe to call repeatedly: a filled member sits at
    /// the mean and a re-run moves nothing.
    pub async fn fill_joining_member(&self, joiner: u64) -> Result<FillReport, String> {
        // One balancer by construction: only the slot SM's raft leader runs
        // the fill. Everyone else answers quickly and does nothing, so the
        // join watcher can fire on every member without coordination.
        if !self.raft_service.is_leader_for_real().await {
            return Err("not the placement leader; the leader runs the fill".to_string());
        }

        let members: Vec<u64> = self
            .membership
            .group_members(&self.group_name, true)
            .await
            .map_err(|e| format!("cannot list group members: {e:?}"))?
            .ok_or_else(|| format!("group {} has no member list", self.group_name))?
            .0
            .into_iter()
            .map(|member| member.id)
            .collect();
        if !members.contains(&joiner) {
            return Err(format!("joiner {joiner} is not an online group member"));
        }

        // A member announces its membership BEFORE it finishes standing up its
        // services, so the join event arrives while the joiner still answers
        // `ServiceIdNotFound` to everything. Waiting on a fixed delay cannot
        // fix that -- it only picks a number and hopes -- so wait for the
        // member to actually SERVE. Asking it for its own live bytes is the
        // right probe: it is the first thing the fill needs anyway, it is
        // read-only, and a member that can answer it can answer the rest.
        let deadline = std::time::Instant::now() + readiness_timeout();
        let mut last_error = String::new();
        loop {
            match self.neb_client.client_by_server_id(joiner).await {
                Ok(cells) => match cells.total_live_bytes().await {
                    Ok(_) => break,
                    Err(e) => last_error = format!("{e:?}"),
                },
                Err(e) => last_error = format!("{e:?}"),
            }
            if std::time::Instant::now() >= deadline {
                return Err(format!(
                    "joining member {joiner} never started serving within {:?} \
                     (last error: {last_error}); nothing was moved",
                    readiness_timeout()
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }

        let placement = crate::migration::placement_client(&self.neb_client);
        let group = crate::slots::slot_group_id(self.neb_client.group_name());
        let fabric = Arc::new(NebFillFabric::new(self.neb_client.clone()));
        let filler = JoinFiller::new(
            Arc::new(GroupScopedPlacement {
                inner: Arc::new(placement),
                group,
            }),
            fabric,
            group,
        );
        let report = filler.fill_joiner(joiner, &members).await?;
        info!(
            "join fill for {joiner}: moved {} slots ({} bytes) in {} rounds, stopped: {:?}",
            report.moved_slots, report.moved_bytes, report.rounds, report.stopped
        );
        Ok(report)
    }

    /// Subscribe the automatic join fill for this server's group. Fires on
    /// every member; the leader gate inside `fill_joining_member` picks the
    /// one that acts. Holds only a weak reference -- a subscription that owns
    /// its server is the exact cycle that kept every Morpheus-hosted server
    /// alive forever.
    pub async fn start_join_fill_watcher(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        let subscription = self
            .membership
            .on_group_member_joined(
                move |(member, _version)| {
                    let weak = weak.clone();
                    async move {
                        let Some(server) = weak.upgrade() else { return };
                        tokio::spawn(async move {
                            tokio::time::sleep(stability_window()).await;
                            match server.fill_joining_member(member.id).await {
                                Ok(report) => {
                                    if report.moved_slots > 0 {
                                        info!(
                                            "auto-filled joining member {}: {} slots, {} bytes",
                                            member.id, report.moved_slots, report.moved_bytes
                                        );
                                    }
                                }
                                // "not the leader" is the normal answer on
                                // every member but one.
                                Err(reason) => debug!(
                                    "join fill for {} did not run here: {}",
                                    member.id, reason
                                ),
                            }
                        });
                    }
                    .boxed()
                },
                &self.group_name,
            )
            .await;
        if let Err(e) = subscription {
            warn!("could not subscribe the join-fill watcher: {e:?}");
        }
    }
}

/// [`PlacementView`] over the slots SM client, with the group baked in so the
/// bifrost driver never learns how Neb derives its group ids.
struct GroupScopedPlacement {
    inner: Arc<bifrost::conshash::slots::client::SMClient>,
    group: u64,
}

impl PlacementView for GroupScopedPlacement {
    fn slots_owned_by(&self, _group: u64, member: u64) -> BoxFuture<'_, Result<Vec<u32>, String>> {
        let group = self.group;
        async move {
            self.inner
                .slots_owned_by(&group, &member)
                .await
                .map_err(|e| format!("slots_owned_by: {e:?}"))
        }
        .boxed()
    }
    fn migration_control(
        &self,
        _group: u64,
    ) -> BoxFuture<'_, Result<bifrost::conshash::slots::MigrationControlView, String>> {
        let group = self.group;
        async move {
            self.inner
                .migration_control(&group)
                .await
                .map_err(|e| format!("migration_control: {e:?}"))
        }
        .boxed()
    }
}
