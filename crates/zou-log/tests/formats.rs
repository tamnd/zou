//! Every durable format refuses a version it does not know.
//!
//! A fleet is not upgraded all at once. For as long as a rolling
//! restart takes, a binary at the new format and a binary at the old
//! one are both reading the same bucket, and the old one is the one at
//! risk: it meets objects written by something that knows more than it
//! does. What it must never do is take such an object as read. Every
//! one of these structs has fields that default, so a newer object
//! does not fail to parse on its own, it parses into something
//! plausible and wrong: an empty roster, a manifest with no layers, a
//! chain with no head. The format number is the only thing that stops
//! it, and only if somebody looks at the number.
//!
//! So this is one test over all of them rather than one test next to
//! each, because the thing being checked is not that a given reader
//! got it right, it is that the rule holds across the set and keeps
//! holding when the set grows. A format added later and not added here
//! is the failure this is shaped to catch.
//!
//! The local formats are not here. The counter file, the file cache
//! and the embedded sqlite are one process's own files in one
//! process's own directory, never shared and never written by another
//! binary, so a version they do not know is a corrupt file rather than
//! a fleet mid upgrade. They check anyway, they are just not what this
//! is about.

use zou_log::chain::{self, SHARD_MANIFEST_FORMAT, ShardManifest};
use zou_log::consolidate::{self, ROUND_INDEX_FORMAT, RoundIndex};
use zou_log::sealed::{SEALED_MAGIC, SEALED_VERSION, read_sealed_footer};
use zou_log::segment::{SEGMENT_MAGIC, SEGMENT_VERSION, read_footer};
use zou_store::cas::CasStore;
use zou_store::layer::{LAYER_MAGIC, LAYER_VERSION, read_layer_footer};
use zou_store::mem::MemStore;
use zou_store::placement::{MAP_FORMAT, ShardMap};
use zou_store::registry::{self, Alias, REGISTRY_FORMAT, Tenant, entry_key, host_key};
use zou_store::shardmanifest::{PAGE_SHARD_FORMAT, PageShardManifest};
use zou_store::{MANIFEST_FORMAT, Manifest};

/// A binary format's version sits in two little endian bytes right
/// after a four byte magic, in all three of them, and the version is
/// read before the length and the checksum are, so a buffer that is
/// only long enough and only right at the front is enough to reach
/// the refusal. That is the point: a reader that verified the file
/// before it read the version would be spending work on a file it is
/// about to refuse, and worse, would answer corrupt where the true
/// answer is that the binary is behind.
fn stamped(magic: [u8; 4], version: u16) -> Vec<u8> {
    let mut buf = vec![0u8; 4096];
    buf[0..4].copy_from_slice(&magic);
    buf[4..6].copy_from_slice(&version.to_le_bytes());
    buf
}

/// The json half, by name and by what reading one back says.
fn refusals() -> Vec<(&'static str, Result<(), String>)> {
    let say = |what: &dyn std::fmt::Display| Err(what.to_string());

    let mut manifest = Manifest::new("t1", 18);
    manifest.format = MANIFEST_FORMAT + 1;
    let manifest = match Manifest::from_json(&manifest.to_json()) {
        Ok(_) => Ok(()),
        Err(e) => say(&e),
    };

    let mut shard = PageShardManifest::new(0);
    shard.format = PAGE_SHARD_FORMAT + 1;
    let shard = match PageShardManifest::decode(&shard.encode()) {
        Ok(_) => Ok(()),
        Err(e) => say(&e),
    };

    let mut map = ShardMap::empty();
    map.format = MAP_FORMAT + 1;
    let map = match ShardMap::from_json(&map.to_json()) {
        Ok(_) => Ok(()),
        Err(e) => say(&e),
    };

    let store = MemStore::new();

    let mut tenant = Tenant::new("t1", "secret", 0);
    tenant.format = REGISTRY_FORMAT + 1;
    store.put(&entry_key("t1"), &tenant.to_json()).unwrap();
    let entry = match registry::get(&store, "t1") {
        Ok(_) => Ok(()),
        Err(e) => say(&e),
    };

    let alias = Alias {
        format: REGISTRY_FORMAT + 1,
        host: "db.example.com".to_string(),
        tenant_ref: "t1".to_string(),
    };
    store
        .put(
            &host_key("db.example.com"),
            &serde_json::to_vec(&alias).unwrap(),
        )
        .unwrap();
    let alias = match registry::host_ref(&store, "db.example.com") {
        Ok(_) => Ok(()),
        Err(e) => say(&e),
    };

    let chain_manifest = ShardManifest {
        format: SHARD_MANIFEST_FORMAT + 1,
        shard: 7,
        chain_epoch: 1,
        head: 0,
        consolidated_upto: 0,
        consolidated_digest: 0,
        rounds: None,
        gc_round: 0,
        sealed_by: "node-1".to_string(),
        sealed_unix: 0,
    };
    store
        .put(
            &chain::manifest_key(7),
            &serde_json::to_vec(&chain_manifest).unwrap(),
        )
        .unwrap();
    let chain = match ShardManifest::load(&store, 7) {
        Ok(_) => Ok(()),
        Err(e) => say(&e),
    };

    let round = RoundIndex {
        format: ROUND_INDEX_FORMAT + 1,
        shard: 7,
        round: 3,
        first_seq: 1,
        last_seq: 9,
        sealed: "sealed/7/3".to_string(),
        unix: 0,
        tenants: Vec::new(),
    };
    store
        .put(
            &consolidate::round_key(7, 3),
            &serde_json::to_vec(&round).unwrap(),
        )
        .unwrap();
    let round = match RoundIndex::load(&store, 7, 3) {
        Ok(_) => Ok(()),
        Err(e) => say(&e),
    };

    let segment = match read_footer(&stamped(SEGMENT_MAGIC, SEGMENT_VERSION + 1)) {
        Ok(_) => Ok(()),
        Err(e) => say(&e),
    };
    let sealed = match read_sealed_footer(&stamped(SEALED_MAGIC, SEALED_VERSION + 1)) {
        Ok(_) => Ok(()),
        Err(e) => say(&e),
    };
    let layer = match read_layer_footer(&stamped(LAYER_MAGIC, LAYER_VERSION + 1)) {
        Ok(_) => Ok(()),
        Err(e) => say(&e),
    };

    vec![
        ("tenant manifest", manifest),
        ("page shard manifest", shard),
        ("shard map", map),
        ("registry entry", entry),
        ("registry host alias", alias),
        ("chain shard manifest", chain),
        ("round index", round),
        ("landing segment", segment),
        ("sealed segment", sealed),
        ("layer", layer),
    ]
}

#[test]
fn a_format_newer_than_this_binary_is_refused_and_says_so() {
    let mut took = Vec::new();
    let mut vague = Vec::new();
    for (name, outcome) in refusals() {
        match outcome {
            Ok(()) => took.push(name),
            // Refusing is most of it, but a refusal a person cannot act
            // on is half a refusal: the operator reading the log has to
            // come away knowing the binary is behind, not that the
            // bucket is broken. Every one of these names the number it
            // found and says to upgrade.
            Err(said) => {
                let acted_on = said.contains("newer than")
                    && (said.contains("upgrade") || said.contains("unsupported"));
                if !acted_on {
                    vague.push((name, said));
                }
            }
        }
    }
    assert!(
        took.is_empty(),
        "these took an object from a format they do not know: {took:?}"
    );
    assert!(
        vague.is_empty(),
        "these refused without saying the binary is behind: {vague:?}"
    );
}
