// SPDX-License-Identifier: LGPL-2.1-or-later

//! The registry of connected nodes: the only state the two faces of
//! the relay share. The node face is the write side (reserve, attach,
//! release via the guard), the caller face only reads ([`Nodes::get`]).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use h2::client::SendRequest;
use tokio::sync::Notify;

use varlink_http_bridge::tunnel::NodeId;

/// The connected nodes. First-wins among live connections: a claim on
/// an occupied id is rejected and triggers an immediate liveness probe
/// of the holder, so a dead holder is reaped within seconds instead of
/// a full heartbeat cycle.
#[derive(Default)]
pub(crate) struct Nodes {
    map: Mutex<HashMap<NodeId, Node>>,
    generation: AtomicU64,
}

struct Node {
    // None while reserved but not yet attached (h2 handshake pending)
    h2: Option<SendRequest<Bytes>>,
    // guards release: only the connection that reserved may deregister
    generation: u64,
    // pokes the holder's heartbeat out of cycle on a colliding claim
    probe: Arc<Notify>,
    // when a colliding claim on this id was last reported: a
    // misconfigured node retries forever, and one relay serves a whole
    // fleet of them, so the same complaint must not repeat per attempt
    claim_reported: Instant,
}

/// How often one node id repeats its "already connected" complaint.
const CLAIM_REPORT_INTERVAL: Duration = Duration::from_secs(600);

/// A claim on an id another live connection holds. `loud` is false
/// while the same collision is still being retried, so the caller can
/// keep the log proportional to the problem rather than to the retries.
#[derive(Debug)]
pub(crate) struct Collision {
    pub(crate) loud: bool,
}

/// A successful reservation; everything the node face needs to attach,
/// heartbeat, and release its entry.
pub(crate) struct Reservation {
    pub(crate) id: NodeId,
    generation: u64,
    pub(crate) probe: Arc<Notify>,
}

/// Releases the reserved id when dropped, so no exit path of the node
/// connection handler (error, timeout, cancellation) can leak an entry.
pub(crate) struct ReservationGuard<'a> {
    nodes: &'a Nodes,
    pub(crate) reservation: Reservation,
}

impl Drop for ReservationGuard<'_> {
    fn drop(&mut self) {
        self.nodes.release(&self.reservation);
    }
}

impl Nodes {
    /// reserve `id` or, when it is taken, poke the holder's heartbeat
    /// and refuse.
    pub(crate) fn reserve(&self, id: NodeId) -> Result<ReservationGuard<'_>, Collision> {
        let mut map = self.map.lock().expect("nodes lock");
        if let Some(holder) = map.get_mut(&id) {
            holder.probe.notify_one();
            let loud = holder.claim_reported.elapsed() >= CLAIM_REPORT_INTERVAL;
            if loud {
                holder.claim_reported = Instant::now();
            }
            return Err(Collision { loud });
        }
        let generation = self.generation.fetch_add(1, Ordering::Relaxed);
        let probe = Arc::new(Notify::new());
        map.insert(
            id,
            Node {
                h2: None,
                generation,
                probe: Arc::clone(&probe),
                // a fresh holder reports the first collision at once
                claim_reported: Instant::now() - CLAIM_REPORT_INTERVAL,
            },
        );
        Ok(ReservationGuard {
            nodes: self,
            reservation: Reservation {
                id,
                generation,
                probe,
            },
        })
    }

    pub(crate) fn attach(&self, reservation: &Reservation, h2: SendRequest<Bytes>) {
        let mut map = self.map.lock().expect("nodes lock");
        if let Some(node) = map.get_mut(&reservation.id)
            && node.generation == reservation.generation
        {
            node.h2 = Some(h2);
        }
    }

    pub(crate) fn get(&self, id: NodeId) -> Option<SendRequest<Bytes>> {
        self.map
            .lock()
            .expect("nodes lock")
            .get(&id)
            .and_then(|node| node.h2.clone())
    }

    fn release(&self, reservation: &Reservation) {
        let mut map = self.map.lock().expect("nodes lock");
        if map
            .get(&reservation.id)
            .is_some_and(|node| node.generation == reservation.generation)
        {
            map.remove(&reservation.id);
        }
    }

    #[cfg(test)]
    pub(crate) fn occupied(&self, id: NodeId) -> bool {
        self.map.lock().expect("nodes lock").contains_key(&id)
    }
}
