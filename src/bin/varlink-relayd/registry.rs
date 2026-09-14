// SPDX-License-Identifier: LGPL-2.1-or-later

//! The registry of connected nodes: the only state the two faces of
//! the relay share. The node face is the write side (reserve, attach,
//! release via the guard), the caller face only reads ([`Nodes::get`]).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use h2::client::SendRequest;
use tokio::sync::Notify;

use varlink_http_bridge::tunnel::{NodeId, StreamLoad};

/// The connected nodes. First-wins among live connections: a claim on
/// an occupied id is rejected and triggers an immediate liveness probe
/// of the holder, so a dead holder is reaped within seconds instead of
/// a full heartbeat cycle. Only a holder that answers the probe makes
/// the claim a collision worth reporting ([`Nodes::claim_is_news`]).
#[derive(Default)]
pub(crate) struct Nodes {
    map: Mutex<HashMap<NodeId, Node>>,
}

struct Node {
    // None while reserved but not yet attached (h2 handshake pending)
    h2: Option<SendRequest<Bytes>>,
    // pokes the holder's heartbeat out of cycle on a colliding claim
    probe: Arc<Notify>,
    load: Arc<StreamLoad>,
    // when a colliding claim on this live holder was last reported: a
    // misconfigured node retries forever, and one relay serves a whole
    // fleet of them, so the same complaint must not repeat per attempt
    claim_reported: Option<Instant>,
}

/// How often one node id repeats its "already connected" complaint.
const CLAIM_REPORT_INTERVAL: Duration = Duration::from_secs(600);

/// A claim on an id another connection holds. Whether that is worth a
/// warning is not known yet: the holder may be dead, and the probe this
/// claim triggered is what finds out.
#[derive(Debug)]
pub(crate) struct Collision;

/// A successful reservation; everything the node face needs to attach,
/// heartbeat, and release its entry.
pub(crate) struct Reservation {
    pub(crate) id: NodeId,
    pub(crate) probe: Arc<Notify>,
    pub(crate) load: Arc<StreamLoad>,
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
        if let Some(holder) = map.get(&id) {
            holder.probe.notify_one();
            return Err(Collision);
        }
        let probe = Arc::new(Notify::new());
        let load = Arc::new(StreamLoad::default());
        map.insert(
            id,
            Node {
                h2: None,
                probe: Arc::clone(&probe),
                load: Arc::clone(&load),
                claim_reported: None,
            },
        );
        Ok(ReservationGuard {
            nodes: self,
            reservation: Reservation { id, probe, load },
        })
    }

    /// Make a reserved node reachable for callers. The guard holding
    /// `reservation` keeps the entry in place, so it is always there.
    pub(crate) fn attach(&self, reservation: &Reservation, h2: SendRequest<Bytes>) {
        let mut map = self.map.lock().expect("nodes lock");
        let node = map.get_mut(&reservation.id).expect("reserved id is held");
        node.h2 = Some(h2);
    }

    /// Whether the holder of `reservation`, having just answered the
    /// probe a colliding claim triggered, should report that claim:
    /// yes for the first one and then once per [`CLAIM_REPORT_INTERVAL`],
    /// so the log stays proportional to the problem, not to the retries.
    pub(crate) fn claim_is_news(&self, reservation: &Reservation) -> bool {
        let mut map = self.map.lock().expect("nodes lock");
        let node = map.get_mut(&reservation.id).expect("reserved id is held");
        let news = node
            .claim_reported
            .is_none_or(|at| at.elapsed() >= CLAIM_REPORT_INTERVAL);
        if news {
            node.claim_reported = Some(Instant::now());
        }
        news
    }

    pub(crate) fn get(&self, id: NodeId) -> Option<(SendRequest<Bytes>, Arc<StreamLoad>)> {
        self.map
            .lock()
            .expect("nodes lock")
            .get(&id)
            .and_then(|node| Some((node.h2.clone()?, Arc::clone(&node.load))))
    }

    // no owner check needed: first-wins means a second reservation for
    // the id cannot exist until this one is gone
    fn release(&self, reservation: &Reservation) {
        self.map.lock().expect("nodes lock").remove(&reservation.id);
    }

    #[cfg(test)]
    pub(crate) fn occupied(&self, id: NodeId) -> bool {
        self.map.lock().expect("nodes lock").contains_key(&id)
    }
}
