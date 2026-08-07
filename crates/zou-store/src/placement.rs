//! The shard map: which page node serves which tenant shard (spec 06
//! section 3).
//!
//! The map is one versioned object at `ctl/MAP`, swapped with CAS like
//! a tenant manifest. It carries the healthy node roster and a list of
//! pins, and placement is rendezvous hashing over the roster: every
//! node scores every `(tenant, shard)` and the highest score wins, so
//! adding or removing one node only moves the shards that node gains
//! or loses and everything else keeps its cache warm. A pin overrides
//! the hash for one shard, which is how heat balancing evicts a hot
//! tenant from a crowded node without touching the roster.
//!
//! Staleness is handled at the edges, not by a coordinator. Every rpc
//! carries the sender's map version and every answer returns the
//! server's. A server asked for a shard it does not own under its map
//! answers [`PlacementError::WrongShard`] with its version, the client
//! refreshes past it and reroutes; a server that sees a newer version
//! than its own refreshes before deciding. Two stale peers can still
//! agree on an old owner for a moment, and that is fine: layers are
//! immutable and writes are fenced by the lease epoch, so the map
//! never guards safety, only reroute speed and cache warmth.
//!
//! The scoring hash is not format frozen. Changing it reshuffles
//! placement, which costs cache warmth on the next map publish, never
//! correctness; nothing durable names a node.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::cas::{CasError, CasStore};

/// The single mutable map object, the root of placement.
pub const MAP_KEY: &str = "ctl/MAP";

pub const MAP_FORMAT: u32 = 1;

/// Historical map snapshot, one per publish, so a reroute storm can be
/// reconstructed after the fact.
pub fn map_history(version: u64) -> String {
    format!("ctl/maps/{version:016}.json")
}

#[derive(Debug, thiserror::Error)]
pub enum PlacementError {
    #[error("this node does not serve the shard under map version {map_version}")]
    WrongShard { map_version: u64 },
    #[error("the map has no nodes, nothing can serve")]
    NoNodes,
    #[error("shard map: {0}")]
    Map(String),
    #[error(transparent)]
    Store(#[from] CasError),
}

/// One page node in the roster. The id is the stable identity the
/// scores hash over; the addr is whatever the rpc layer dials and the
/// map does not interpret it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub addr: String,
}

/// An explicit placement that beats the hash for one tenant shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pin {
    pub tenant: String,
    pub shard: u16,
    pub node: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardMap {
    pub format: u32,
    /// Bumped by every publish. What rpcs exchange to detect staleness.
    pub version: u64,
    /// The healthy roster placement hashes over.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<Node>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pins: Vec<Pin>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_unix: Option<u64>,
}

impl ShardMap {
    /// The map before anyone published one: version zero, no nodes.
    pub fn empty() -> Self {
        ShardMap {
            format: MAP_FORMAT,
            version: 0,
            nodes: Vec::new(),
            pins: Vec::new(),
            published_unix: None,
        }
    }

    pub fn to_json(&self) -> Vec<u8> {
        let mut out = serde_json::to_vec_pretty(self).expect("shard map serializes");
        out.push(b'\n');
        out
    }

    pub fn from_json(data: &[u8]) -> Result<Self, PlacementError> {
        serde_json::from_slice(data).map_err(|e| PlacementError::Map(e.to_string()))
    }

    /// Every node ordered by its rendezvous score for this tenant
    /// shard, best first. The head is the owner, the next entries are
    /// the natural secondaries a warm standby tier picks, and losing
    /// the head promotes exactly the next entry, nobody else moves.
    pub fn rank(&self, tenant_ref: &str, shard: u16) -> Vec<&Node> {
        let mut scored: Vec<(u64, &Node)> = self
            .nodes
            .iter()
            .map(|n| (score(&n.id, tenant_ref, shard), n))
            .collect();
        scored.sort_by_key(|(s, _)| std::cmp::Reverse(*s));
        scored.into_iter().map(|(_, n)| n).collect()
    }

    /// The node that serves this tenant shard: its pin when one
    /// exists, the rendezvous winner otherwise, nothing on an empty
    /// roster. A pin naming a node that left the roster is ignored
    /// rather than trusted, the hash always has a live answer.
    pub fn node_for(&self, tenant_ref: &str, shard: u16) -> Option<&Node> {
        if let Some(pin) = self
            .pins
            .iter()
            .find(|p| p.tenant == tenant_ref && p.shard == shard)
            && let Some(node) = self.nodes.iter().find(|n| n.id == pin.node)
        {
            return Some(node);
        }
        self.rank(tenant_ref, shard).into_iter().next()
    }
}

/// The rendezvous score of one node for one tenant shard: the first
/// eight bytes of sha256 over the three identities, big endian, so
/// every observer of the same roster computes the same placement.
fn score(node_id: &str, tenant_ref: &str, shard: u16) -> u64 {
    let mut h = Sha256::new();
    h.update(node_id.as_bytes());
    h.update([0]);
    h.update(tenant_ref.as_bytes());
    h.update([0]);
    h.update(shard.to_le_bytes());
    let digest = h.finalize();
    u64::from_be_bytes(digest[..8].try_into().expect("sha256 is long enough"))
}

/// Load the current map, [`ShardMap::empty`] when nobody published
/// one yet.
pub fn load(store: &dyn CasStore) -> Result<ShardMap, PlacementError> {
    match store.get(MAP_KEY)? {
        Some((data, _)) => ShardMap::from_json(&data),
        None => Ok(ShardMap::empty()),
    }
}

/// Publish a map change: load the current map, apply `edit`, bump the
/// version, CAS it in. Retries on conflict by re-reading and editing
/// again, so concurrent publishers each land exactly once. Returns the
/// map as published.
pub fn publish(
    store: &dyn CasStore,
    mut edit: impl FnMut(&mut ShardMap),
) -> Result<ShardMap, PlacementError> {
    loop {
        let (mut map, version) = match store.get(MAP_KEY)? {
            Some((data, version)) => (ShardMap::from_json(&data)?, Some(version)),
            None => (ShardMap::empty(), None),
        };
        edit(&mut map);
        map.format = MAP_FORMAT;
        map.version += 1;
        map.published_unix = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
        let outcome = match &version {
            Some(v) => store
                .put_if_match(MAP_KEY, &map.to_json(), Some(v))
                .map(|_| ()),
            None => store.put_if_absent(MAP_KEY, &map.to_json()).map(|_| ()),
        };
        match outcome {
            Ok(_) => {
                // Best effort history copy, same contract as the
                // manifest snapshots: a miss loses this entry from the
                // audit trail, never placement.
                let history = map_history(map.version);
                if let Err(e) = store.put_if_absent(&history, &map.to_json())
                    && !matches!(e, CasError::AlreadyExists { .. })
                {
                    log::warn!("map history {history} failed, audit loses this publish: {e}");
                }
                return Ok(map);
            }
            Err(CasError::Conflict { .. }) | Err(CasError::AlreadyExists { .. }) => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

/// The client half of the version handshake: a cached map, a route
/// call per rpc, and a refresh when an answer proves the cache stale.
pub struct MapClient<'a> {
    store: &'a dyn CasStore,
    map: ShardMap,
}

impl<'a> MapClient<'a> {
    pub fn new(store: &'a dyn CasStore) -> Result<Self, PlacementError> {
        Ok(MapClient {
            map: load(store)?,
            store,
        })
    }

    /// The version every outgoing rpc carries.
    pub fn version(&self) -> u64 {
        self.map.version
    }

    /// Where to send an rpc for this tenant shard under the cached map.
    pub fn route(&self, tenant_ref: &str, shard: u16) -> Result<&Node, PlacementError> {
        self.map
            .node_for(tenant_ref, shard)
            .ok_or(PlacementError::NoNodes)
    }

    /// Fold a server's version into the cache: refresh when the server
    /// has seen a newer map, whether it said so in an answer or in a
    /// [`PlacementError::WrongShard`] redirect. After a redirect the
    /// caller routes again and the retry lands on the new owner.
    pub fn absorb(&mut self, server_version: u64) -> Result<(), PlacementError> {
        if server_version > self.map.version {
            self.map = load(self.store)?;
        }
        Ok(())
    }
}

/// The server half: a node's cached map and the admission check every
/// incoming rpc passes before any work happens.
pub struct MapServer<'a> {
    store: &'a dyn CasStore,
    node: String,
    map: ShardMap,
}

impl<'a> MapServer<'a> {
    pub fn new(
        store: &'a dyn CasStore,
        node_id: impl Into<String>,
    ) -> Result<Self, PlacementError> {
        Ok(MapServer {
            map: load(store)?,
            node: node_id.into(),
            store,
        })
    }

    pub fn version(&self) -> u64 {
        self.map.version
    }

    /// Reload the cached map. Nodes call this when the control plane
    /// republishes and on a timer; between calls the version handshake
    /// still catches staleness one rpc later.
    pub fn refresh(&mut self) -> Result<(), PlacementError> {
        self.map = load(self.store)?;
        Ok(())
    }

    /// Admit one rpc for a tenant shard. A client version newer than
    /// the cache proves the cache stale, so refresh first; then the
    /// shard either maps here and the rpc proceeds, or the answer is
    /// [`PlacementError::WrongShard`] carrying this server's version so
    /// the client can refresh past it. Returns the version the answer
    /// carries either way.
    pub fn admit(
        &mut self,
        tenant_ref: &str,
        shard: u16,
        client_version: u64,
    ) -> Result<u64, PlacementError> {
        if client_version > self.map.version {
            self.map = load(self.store)?;
        }
        match self.map.node_for(tenant_ref, shard) {
            Some(node) if node.id == self.node => Ok(self.map.version),
            _ => Err(PlacementError::WrongShard {
                map_version: self.map.version,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem::MemStore;

    fn roster(ids: &[&str]) -> Vec<Node> {
        ids.iter()
            .map(|id| Node {
                id: id.to_string(),
                addr: format!("{id}.cell:6400"),
            })
            .collect()
    }

    #[test]
    fn rendezvous_moves_only_the_lost_nodes_shards() {
        let full = ShardMap {
            nodes: roster(&["a", "b", "c", "d"]),
            ..ShardMap::empty()
        };
        let mut without_c = full.clone();
        without_c.nodes.retain(|n| n.id != "c");

        let mut moved = 0;
        for shard in 0..256u16 {
            let before = full.node_for("t", shard).unwrap().id.clone();
            let after = without_c.node_for("t", shard).unwrap().id.clone();
            if before == "c" {
                // Orphaned shards land on their rendezvous runner up.
                assert_eq!(after, full.rank("t", shard)[1].id);
                moved += 1;
            } else {
                // Everyone else keeps its cache warm.
                assert_eq!(before, after);
            }
        }
        assert!(moved > 0, "four nodes over 256 shards must own some");
    }

    #[test]
    fn ranking_is_deterministic_and_spreads_load() {
        let map = ShardMap {
            nodes: roster(&["a", "b", "c", "d"]),
            ..ShardMap::empty()
        };
        let mut owned = std::collections::HashMap::new();
        for shard in 0..256u16 {
            let ranked = map.rank("t", shard);
            assert_eq!(ranked.len(), 4);
            assert_eq!(map.rank("t", shard), ranked, "same inputs, same order");
            *owned.entry(ranked[0].id.clone()).or_insert(0u32) += 1;
        }
        // Rendezvous over 256 shards should give every node real work.
        // The exact split is hash luck, the point is nobody starves.
        for id in ["a", "b", "c", "d"] {
            assert!(owned[id] > 16, "{id} owns {} of 256", owned[id]);
        }
    }

    #[test]
    fn a_pin_beats_the_hash_but_never_names_a_ghost() {
        let mut map = ShardMap {
            nodes: roster(&["a", "b"]),
            ..ShardMap::empty()
        };
        let hashed = map.node_for("t", 0).unwrap().id.clone();
        let other = if hashed == "a" { "b" } else { "a" };
        map.pins.push(Pin {
            tenant: "t".into(),
            shard: 0,
            node: other.into(),
        });
        assert_eq!(map.node_for("t", 0).unwrap().id, other);
        // A pin to a node that left the roster falls back to the hash.
        map.pins[0].node = "gone".into();
        assert_eq!(map.node_for("t", 0).unwrap().id, hashed);
    }

    #[test]
    fn publish_bumps_the_version_and_keeps_history() {
        let store = MemStore::default();
        let map = publish(&store, |m| m.nodes = roster(&["a"])).unwrap();
        assert_eq!(map.version, 1);
        let map = publish(&store, |m| m.nodes.push(roster(&["b"]).remove(0))).unwrap();
        assert_eq!(map.version, 2);
        assert_eq!(load(&store).unwrap(), map);
        assert!(store.get(&map_history(1)).unwrap().is_some());
        assert!(store.get(&map_history(2)).unwrap().is_some());
        // Nobody published yet reads as the empty map, version zero.
        assert_eq!(load(&MemStore::default()).unwrap(), ShardMap::empty());
    }

    #[test]
    fn a_stale_server_redirects_and_the_client_reroutes() {
        let store = MemStore::default();
        publish(&store, |m| m.nodes = roster(&["a", "b"])).unwrap();

        let mut client = MapClient::new(&store).unwrap();
        let owner = client.route("t", 0).unwrap().id.clone();
        let other = if owner == "a" { "b" } else { "a" };
        let mut on_owner = MapServer::new(&store, owner.clone()).unwrap();
        let mut on_other = MapServer::new(&store, other).unwrap();

        // Steady state: the rpc lands where the map says and both
        // sides agree on the version.
        assert_eq!(on_owner.admit("t", 0, client.version()).unwrap(), 1);

        // Heat balancing pins the shard away. The old owner's cache is
        // now stale and so is the client's.
        publish(&store, |m| {
            m.pins = vec![Pin {
                tenant: "t".into(),
                shard: 0,
                node: other.to_string(),
            }]
        })
        .unwrap();

        // Both sides stale: the rpc still lands on the old owner and
        // it still serves, which is safe, layers are immutable and the
        // map never guards correctness. Only reroute speed is lost.
        assert_eq!(client.route("t", 0).unwrap().id, owner);
        assert_eq!(on_owner.admit("t", 0, client.version()).unwrap(), 1);

        // The old owner hears about the publish and refreshes; now the
        // stale client gets redirected with the newer version.
        on_owner.refresh().unwrap();
        let err = on_owner.admit("t", 0, client.version()).unwrap_err();
        let PlacementError::WrongShard { map_version } = err else {
            panic!("expected a redirect, got {err}");
        };
        assert_eq!(map_version, 2);

        // The redirect carries everything the client needs: absorb,
        // reroute, land on the new owner, no coordinator in the loop.
        client.absorb(map_version).unwrap();
        assert_eq!(client.version(), 2);
        assert_eq!(client.route("t", 0).unwrap().id, other);
        assert_eq!(on_other.admit("t", 0, client.version()).unwrap(), 2);
    }

    #[test]
    fn a_stale_server_refreshes_when_the_client_is_ahead() {
        let store = MemStore::default();
        publish(&store, |m| m.nodes = roster(&["a", "b"])).unwrap();
        let owner = load(&store).unwrap().node_for("t", 0).unwrap().id.clone();
        let other = if owner == "a" { "b" } else { "a" };

        // The new owner booted before the pin landed, so its cache
        // says the shard lives elsewhere.
        let mut server = MapServer::new(&store, other).unwrap();
        publish(&store, |m| {
            m.pins = vec![Pin {
                tenant: "t".into(),
                shard: 0,
                node: other.to_string(),
            }]
        })
        .unwrap();

        // A fresh client carries version 2, which proves the server
        // stale; it refreshes before deciding and admits the rpc
        // instead of bouncing it back and forth.
        let client = MapClient::new(&store).unwrap();
        assert_eq!(client.route("t", 0).unwrap().id, other);
        assert_eq!(server.admit("t", 0, client.version()).unwrap(), 2);
    }

    #[test]
    fn the_map_round_trips_and_the_empty_map_is_bare() {
        let map = ShardMap {
            nodes: roster(&["a"]),
            pins: vec![Pin {
                tenant: "t".into(),
                shard: 3,
                node: "a".into(),
            }],
            published_unix: Some(1_766_000_000),
            ..ShardMap::empty()
        };
        assert_eq!(ShardMap::from_json(&map.to_json()).unwrap(), map);
        // The empty map keeps its optional sections off the wire.
        let bare = String::from_utf8(ShardMap::empty().to_json()).unwrap();
        assert!(!bare.contains("nodes") && !bare.contains("pins"));
    }
}
