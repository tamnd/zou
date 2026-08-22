//! The upgrade path: what a fleet halfway through a restart may meet.
//!
//! `formats.rs` next door checks one direction, that a binary refuses
//! an object written at a version it does not know. That is the half
//! that keeps a store from being misread. It is not the half that
//! keeps a deployment up, because a fleet where every old node refuses
//! every new object is a fleet that stopped serving, and a refusal is
//! only the right answer when the alternative was worse.
//!
//! What keeps it up is the other rule: a writer emits the lowest
//! format that carries what it wrote. A tenant that never split keeps
//! writing manifest format 2 for as long as it never splits, so a node
//! that predates sharding reads it, and the format it cannot read is
//! only ever written by a tenant that used the feature it was added
//! for. Nothing here is free: the split is the moment that tenant
//! stops being readable by the old binary, and that is the price of
//! the feature rather than the price of the release.
//!
//! Three things are checked, and only the first is about any one
//! format.
//!
//! The census is complete. Every format constant in the tree is found
//! by reading the source and matched against the table below, value
//! included, so a constant that moves fails here until somebody says
//! what the new floor is. That is the point: bumping a format is a
//! decision about every binary already running, and this is where it
//! is made rather than a line in a diff nobody connected to a rollout.
//!
//! The wire bytes are frozen. There has been no release yet, so there
//! is no previous binary to run against a live prefix and building one
//! out of history proves less every month. What is worth having
//! instead is what a plain object of each format serializes to today,
//! checked in, so the first change that would break a reader older
//! than it shows up as a diff in a fixture rather than as a fleet that
//! will not route.
//!
//! A plain object carries the floor. Not the ceiling, which is what a
//! writer reaches for by accident: the constant is right there and it
//! is the newest thing the writer knows.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use zou_log::chain::{SHARD_MANIFEST_FORMAT, ShardManifest};
use zou_log::consolidate::{ROUND_INDEX_FORMAT, RoundIndex};
use zou_log::sealed::{SEALED_MAGIC, SEALED_VERSION, SealedHeader, build_sealed};
use zou_log::segment::{
    SEGMENT_MAGIC, SEGMENT_VERSION, SegmentBuilder, SegmentHeader, SegmentKind,
};
use zou_store::layer::{DeltaEntry, LAYER_MAGIC, LAYER_VERSION, LayerKey, build_delta};
use zou_store::lsn::Lsn;
use zou_store::placement::{MAP_FORMAT, ShardMap};
use zou_store::registry::{Alias, REGISTRY_FORMAT, Tenant};
use zou_store::shardmanifest::{PAGE_SHARD_FORMAT, PageShardManifest};
use zou_store::{MANIFEST_FORMAT, Manifest};

/// Who else reads what this format is written into, which is what
/// decides whether a version mismatch is a fleet's problem or one
/// process's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reach {
    /// An object in the shared store. Every node in the fleet reads
    /// it, including the ones still on the release before this one,
    /// and it outlives all of them.
    Store,
    /// A file one machine holds and another machine's zou may be
    /// handed. Not the fleet's problem, still somebody's.
    File,
    /// Spoken between two processes that are both running. The two
    /// ends are on different releases for as long as a rollout takes.
    Wire,
    /// One process's own file, rewritten when it does not fit. Nothing
    /// else ever reads it, so a version it does not know is a stale
    /// file rather than a binary that is behind.
    Local,
}

struct Format {
    /// What an operator would call it.
    name: &'static str,
    /// Where the constant lives, relative to the workspace root.
    file: &'static str,
    konst: &'static str,
    reach: Reach,
    /// What a plain object of it carries today. Below the ceiling for
    /// a format whose newer versions only carry a feature, equal to it
    /// for one that has never moved.
    floor: u64,
    /// The highest this binary reads. Checked against the source, so
    /// this table cannot drift from the constant it describes.
    ceiling: u64,
    /// Where the bytes of a plain object are frozen, or why they are
    /// not frozen here.
    frozen: &'static str,
    /// Where meeting a version this binary does not know is checked.
    refusal: &'static str,
}

/// Every durable and spoken format in the tree.
///
/// A format missing from here is the failure this file is shaped to
/// catch, so the entries are not a summary of the code, they are the
/// list the code is checked against.
const CENSUS: &[Format] = &[
    Format {
        name: "tenant manifest",
        file: "crates/zou-store/src/manifest.rs",
        konst: "MANIFEST_FORMAT",
        reach: Reach::Store,
        // 3 carries sharding. A tenant that never split is still read
        // by every binary that predates it.
        floor: 2,
        ceiling: 3,
        frozen: "testdata/upgrade/tenant-manifest.json",
        refusal: "tests/formats.rs",
    },
    Format {
        name: "page shard manifest",
        file: "crates/zou-store/src/shardmanifest.rs",
        konst: "PAGE_SHARD_FORMAT",
        reach: Reach::Store,
        // 2 carries branch inheritance, written by a branched shard
        // and by nothing else.
        floor: 1,
        ceiling: 2,
        frozen: "testdata/upgrade/page-shard-manifest.json",
        refusal: "tests/formats.rs",
    },
    Format {
        name: "shard map",
        file: "crates/zou-store/src/placement.rs",
        konst: "MAP_FORMAT",
        reach: Reach::Store,
        floor: 1,
        ceiling: 1,
        frozen: "testdata/upgrade/shard-map.json",
        refusal: "tests/formats.rs",
    },
    Format {
        name: "registry entry",
        file: "crates/zou-store/src/registry.rs",
        konst: "REGISTRY_FORMAT",
        reach: Reach::Store,
        floor: 1,
        ceiling: 1,
        frozen: "testdata/upgrade/registry-tenant.json",
        refusal: "tests/formats.rs",
    },
    Format {
        name: "chain shard manifest",
        file: "crates/zou-log/src/chain.rs",
        konst: "SHARD_MANIFEST_FORMAT",
        reach: Reach::Store,
        floor: 1,
        ceiling: 1,
        frozen: "testdata/upgrade/chain-shard-manifest.json",
        refusal: "tests/formats.rs",
    },
    Format {
        name: "round index",
        file: "crates/zou-log/src/consolidate.rs",
        konst: "ROUND_INDEX_FORMAT",
        reach: Reach::Store,
        floor: 1,
        ceiling: 1,
        frozen: "testdata/upgrade/round-index.json",
        refusal: "tests/formats.rs",
    },
    Format {
        name: "landing segment",
        file: "crates/zou-log/src/segment.rs",
        konst: "SEGMENT_VERSION",
        reach: Reach::Store,
        floor: 1,
        ceiling: 1,
        frozen: "the header, checked below: magic then version, and nothing before them",
        refusal: "tests/formats.rs",
    },
    Format {
        name: "sealed segment",
        file: "crates/zou-log/src/sealed.rs",
        konst: "SEALED_VERSION",
        reach: Reach::Store,
        floor: 1,
        ceiling: 1,
        frozen: "the header, checked below: magic then version, and nothing before them",
        refusal: "tests/formats.rs",
    },
    Format {
        name: "layer",
        file: "crates/zou-store/src/layer.rs",
        konst: "LAYER_VERSION",
        reach: Reach::Store,
        floor: 1,
        ceiling: 1,
        frozen: "the header, checked below: magic then version, and nothing before them",
        refusal: "tests/formats.rs",
    },
    Format {
        name: "deployed functions",
        file: "crates/zou/src/bundle.rs",
        konst: "VERSION",
        reach: Reach::Store,
        floor: 1,
        ceiling: 1,
        // The zou crate is a binary, so no other crate's test can
        // build one of these. Both halves live next to the code.
        frozen: "crates/zou/src/bundle.rs, a binary crate this one cannot import",
        refusal: "crates/zou/src/bundle.rs",
    },
    Format {
        name: "sealed secrets",
        file: "crates/zou/src/secrets.rs",
        konst: "VERSION",
        reach: Reach::Store,
        floor: 1,
        ceiling: 1,
        // Sealed with a fresh nonce every write, so the bytes are
        // never the same twice and freezing them means freezing the
        // envelope, which is what the test there does.
        frozen: "crates/zou/src/secrets.rs, and the payload is a new nonce every write",
        refusal: "crates/zou/src/secrets.rs",
    },
    Format {
        name: "single file store",
        file: "crates/zou-store/src/zoufile.rs",
        konst: "FORMAT",
        reach: Reach::File,
        floor: 1,
        ceiling: 1,
        frozen: "crates/zou-store/src/zoufile.rs, the header it writes and reads back",
        refusal: "crates/zou-store/src/zoufile.rs",
    },
    Format {
        name: "realtime fanout link",
        file: "crates/zou-server/src/fanout.rs",
        konst: "VERSION",
        reach: Reach::Wire,
        // Equal, and always will be: a link is refused unless both
        // ends say the same number, so a node meets a holder of its
        // own version or it meets nothing. Two because the budget
        // frames replaced the one a node sent per relayed message, so
        // a version 1 holder would have counted a version 2 node's
        // deliveries as nothing at all.
        floor: 2,
        ceiling: 2,
        // Both ends are live, so there is nothing to freeze: the
        // hello names the version and a link whose ends disagree does
        // not open.
        frozen: "nothing durable, the version is in the hello and the link is the object",
        refusal: "crates/zou-server/src/fanout.rs",
    },
    Format {
        name: "store counter file",
        file: "crates/zou-store/src/stats.rs",
        konst: "FORMAT",
        reach: Reach::Local,
        floor: 4,
        ceiling: 4,
        // Counts, and only this machine's. A layout change throws the
        // file away rather than migrating it.
        frozen: "nothing, it is counters and it starts over",
        refusal: "crates/zou-store/src/stats.rs",
    },
];

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("the workspace root is two above this crate")
        .to_path_buf()
}

/// Every `const NAME: uN = value;` in the tree whose name says it is a
/// format or a version, by file and by value.
///
/// Read out of the source rather than imported, because a constant
/// that is private, or in a crate this one cannot depend on, is
/// exactly the one that would otherwise be missed.
fn constants_in_the_tree() -> BTreeMap<(String, String), u64> {
    let mut found = BTreeMap::new();
    let mut dirs = vec![root().join("crates")];
    while let Some(dir) = dirs.pop() {
        for entry in fs::read_dir(&dir).expect("read a source directory") {
            let path = entry.expect("a directory entry").path();
            if path.is_dir() {
                dirs.push(path);
                continue;
            }
            if path.extension().is_none_or(|e| e != "rs") {
                continue;
            }
            // Tests write these constants down to violate them, which
            // is not the same thing as declaring one.
            let rel = path
                .strip_prefix(root())
                .expect("under the root")
                .to_string_lossy()
                .replace('\\', "/");
            if rel.contains("/tests/") || rel.contains("/benches/") {
                continue;
            }
            let text = fs::read_to_string(&path).expect("read a source file");
            for line in text.lines() {
                if let Some((name, value)) = declaration(line) {
                    found.insert((rel.clone(), name), value);
                }
            }
        }
    }
    found
}

/// One `const` line, when it declares an unsigned number whose name
/// says it is a format or a version.
fn declaration(line: &str) -> Option<(String, u64)> {
    let line = line.trim();
    let rest = line
        .strip_prefix("pub const ")
        .or_else(|| line.strip_prefix("const "))?;
    let (name, rest) = rest.split_once(':')?;
    let name = name.trim();
    let says_so = name == "VERSION"
        || name == "FORMAT"
        || name.ends_with("_VERSION")
        || name.ends_with("_FORMAT");
    if !says_so {
        return None;
    }
    let (ty, value) = rest.split_once('=')?;
    if !matches!(ty.trim(), "u8" | "u16" | "u32" | "u64") {
        return None;
    }
    let value = value.trim().trim_end_matches(';').trim();
    value.parse().ok().map(|v| (name.to_string(), v))
}

/// A format added to the tree and not to the census is the thing this
/// file exists to catch, and a format whose number moved without
/// anybody visiting the census is the other thing. Both are the same
/// check: the table and the source agree, entry for entry and value
/// for value.
///
/// The remedy for a failure here is never to add the row and move on.
/// It is to answer what a binary already running meets when it reads
/// an object at the new number, and to write the answer into the
/// floor.
#[test]
fn every_format_in_the_tree_is_in_the_census_at_the_number_the_source_says() {
    let found = constants_in_the_tree();
    assert!(
        found.len() >= CENSUS.len(),
        "the scan found {} constants, fewer than the {} in the census, so it is not reading the tree",
        found.len(),
        CENSUS.len()
    );

    let mut missing = Vec::new();
    let mut wrong = Vec::new();
    for f in CENSUS {
        match found.get(&(f.file.to_string(), f.konst.to_string())) {
            None => missing.push((f.file, f.konst)),
            Some(&value) if value != f.ceiling => wrong.push((f.name, f.ceiling, value)),
            Some(_) => {}
        }
    }
    assert!(
        missing.is_empty(),
        "the census names constants that are not in the tree, so it has gone stale: {missing:?}"
    );
    assert!(
        wrong.is_empty(),
        "these formats moved without the census being visited, as (name, census, source): {wrong:?}"
    );

    let uncensused: Vec<_> = found
        .keys()
        .filter(|(file, konst)| {
            !CENSUS
                .iter()
                .any(|f| f.file == file && f.konst == konst.as_str())
        })
        .collect();
    assert!(
        uncensused.is_empty(),
        "these formats are in the tree and not in the census, so nothing says what an older binary does with one: {uncensused:?}"
    );

    for f in CENSUS {
        assert!(
            f.floor <= f.ceiling,
            "{}: a plain object cannot carry more than this binary reads",
            f.name
        );
        assert!(
            !f.frozen.is_empty() && !f.refusal.is_empty(),
            "{}: every entry says where its bytes are frozen and where a version it does not know is refused",
            f.name
        );
        // A format whose objects outlive the process that wrote them
        // has bytes somebody will read later, so there is always
        // something to freeze. A link between two live ends and a file
        // one process throws away have nothing durable, which is the
        // only reason they are allowed to say so.
        if matches!(f.reach, Reach::Store | Reach::File) {
            assert!(
                !f.frozen.starts_with("nothing"),
                "{}: its objects outlive the binary that wrote them, so something has to be frozen",
                f.name
            );
        }
    }
}

/// What a plain object of each store format serializes to, through the
/// same serializer the writer uses. Plain means nothing optional set:
/// this is what a store that uses no feature added after the floor
/// looks like on the wire.
fn plain() -> Vec<(&'static str, Vec<u8>)> {
    let manifest = Manifest::new("t1", 18).to_json();

    let shard = PageShardManifest::new(0).encode();

    let map = ShardMap::empty().to_json();

    let tenant = Tenant::new("t1", "a-jwt-secret", 1_767_100_000).to_json();

    let mut alias = serde_json::to_vec_pretty(&Alias {
        format: REGISTRY_FORMAT,
        host: "db.example.com".to_string(),
        tenant_ref: "t1".to_string(),
    })
    .expect("an alias serializes");
    alias.push(b'\n');

    let chain = serde_json::to_vec_pretty(&ShardManifest {
        format: SHARD_MANIFEST_FORMAT,
        shard: 7,
        chain_epoch: 1,
        head: 0,
        consolidated_upto: 0,
        consolidated_digest: 0,
        rounds: None,
        gc_round: 0,
        sealed_by: "node-1".to_string(),
        sealed_unix: 1_767_100_000,
    })
    .expect("a chain manifest serializes");

    let round = serde_json::to_vec(&RoundIndex {
        format: ROUND_INDEX_FORMAT,
        shard: 7,
        round: 3,
        first_seq: 1,
        last_seq: 9,
        sealed: "sealed/7/3".to_string(),
        unix: 1_767_100_000,
        tenants: Vec::new(),
    })
    .expect("a round index serializes");

    vec![
        ("tenant manifest", manifest),
        ("page shard manifest", shard),
        ("shard map", map),
        ("registry entry", tenant),
        ("registry host alias", alias),
        ("chain shard manifest", chain),
        ("round index", round),
    ]
}

/// The fixture a name is frozen in, from the census.
fn fixture_of(name: &str) -> Option<PathBuf> {
    // The host alias rides on the registry's format number and has no
    // constant of its own, so it is not a census row. It is a
    // different object with a different shape, so it is frozen anyway.
    if name == "registry host alias" {
        return Some(root().join("crates/zou-log/testdata/upgrade/registry-alias.json"));
    }
    let f = CENSUS.iter().find(|f| f.name == name)?;
    f.frozen
        .strip_prefix("testdata/")
        .map(|rel| root().join("crates/zou-log/testdata").join(rel))
}

/// The bytes are the contract with every binary that will ever read
/// them, and the ones that matter are the binaries that already exist.
/// A field renamed, a field that stopped being optional, a number that
/// became a string: each of those is a store an older node can no
/// longer read, and none of them changes a format constant on its own.
/// Freezing what a plain object writes is what turns that into a diff
/// somebody has to look at.
///
/// Set ZOU_FREEZE=1 to write the fixtures, then read the diff before
/// committing it. That the fixture can be regenerated is not a licence
/// to regenerate it: the question a diff here asks is what happens to
/// a node that is already running.
#[test]
fn what_a_plain_object_writes_is_frozen() {
    let freezing = std::env::var("ZOU_FREEZE").is_ok();
    let mut stale = Vec::new();
    for (name, bytes) in plain() {
        let path =
            fixture_of(name).unwrap_or_else(|| panic!("{name} has no fixture in the census"));
        if freezing {
            fs::create_dir_all(path.parent().expect("a parent")).expect("make the fixture dir");
            fs::write(&path, &bytes).expect("write the fixture");
            continue;
        }
        let want = fs::read(&path).unwrap_or_else(|e| {
            panic!(
                "{name}: read {}: {e}, run with ZOU_FREEZE=1",
                path.display()
            )
        });
        if want != bytes {
            stale.push(format!(
                "{name} at {}\n  frozen: {}\n  now:    {}",
                path.display(),
                String::from_utf8_lossy(&want),
                String::from_utf8_lossy(&bytes),
            ));
        }
    }
    assert!(
        !freezing,
        "ZOU_FREEZE rewrote the fixtures, this run proves nothing"
    );
    assert!(
        stale.is_empty(),
        "a plain object no longer writes what it used to, which is what an older binary is still reading:\n{}",
        stale.join("\n")
    );
}

/// Frozen bytes that parse are not enough. Every one of these structs
/// has fields that default, so bytes read through the wrong shape come
/// back as a plausible object rather than an error, which is the whole
/// reason the format numbers exist. So each fixture is read back with
/// the reader that will meet it and asked what it says.
#[test]
fn the_frozen_bytes_still_mean_what_they_meant() {
    let read = |name: &str| {
        let path = fixture_of(name).expect("a fixture");
        fs::read(&path).unwrap_or_else(|e| panic!("{name}: {e}"))
    };

    let manifest = Manifest::from_json(&read("tenant manifest")).expect("a manifest");
    assert_eq!(manifest.tenant_ref, "t1");
    assert_eq!(manifest.pg.version, 18);
    assert_eq!(manifest.shards, 1);
    assert!(manifest.checkpoints.is_empty());

    let shard = PageShardManifest::decode(&read("page shard manifest")).expect("a shard manifest");
    assert_eq!(shard.shard, 0);
    assert_eq!(shard.disk_consistent_lsn, Lsn(0));
    assert!(shard.covers.is_none() && shard.horizon.is_none());

    let map = ShardMap::from_json(&read("shard map")).expect("a map");
    assert_eq!(map.version, 0);
    assert!(map.nodes.is_empty() && map.pins.is_empty());

    let tenant: Tenant = serde_json::from_slice(&read("registry entry")).expect("an entry");
    assert_eq!(tenant.tenant_ref, "t1");
    assert_eq!(tenant.jwt_secret, "a-jwt-secret");
    assert_eq!(tenant.created_unix, 1_767_100_000);
    assert!(tenant.s3().is_none());

    let alias: Alias = serde_json::from_slice(&read("registry host alias")).expect("an alias");
    assert_eq!(alias.host, "db.example.com");
    assert_eq!(alias.tenant_ref, "t1");

    let chain: ShardManifest =
        serde_json::from_slice(&read("chain shard manifest")).expect("a chain manifest");
    assert_eq!(chain.shard, 7);
    assert_eq!(chain.chain_epoch, 1);
    assert_eq!(chain.sealed_by, "node-1");

    let round: RoundIndex = serde_json::from_slice(&read("round index")).expect("a round index");
    assert_eq!((round.shard, round.round), (7, 3));
    assert_eq!((round.first_seq, round.last_seq), (1, 9));
    assert_eq!(round.sealed, "sealed/7/3");
}

/// A writer reaching for the constant is the easy mistake, because the
/// constant is the newest thing it knows and it is right there. It
/// costs nothing until the constant moves, and then it costs every
/// node still on the release before: the first write after the deploy
/// is at a format they refuse, and a node that refuses is a node that
/// stopped answering for what it refused.
#[test]
fn a_plain_object_carries_the_floor_and_not_the_ceiling() {
    let floor_of = |name: &str| CENSUS.iter().find(|f| f.name == name).expect(name).floor;

    let mut wrote = BTreeMap::new();
    for (name, bytes) in plain() {
        if name == "registry host alias" {
            continue;
        }
        #[derive(serde::Deserialize)]
        struct Peek {
            format: u64,
        }
        let peek: Peek = serde_json::from_slice(&bytes).expect("every one of these has a format");
        wrote.insert(name, peek.format);
    }
    let wrong: Vec<_> = wrote
        .iter()
        .filter(|(name, format)| **format != floor_of(name))
        .collect();
    assert!(
        wrong.is_empty(),
        "these wrote a format a plain object does not need, as (name, written): {wrong:?}"
    );

    // The check above passes for a format that has never moved without
    // proving anything, because floor and ceiling are the same number
    // and there is nothing to get wrong. So the two formats that have
    // moved are named: for those the two numbers differ, and a plain
    // object being at the lower one is the rule doing work.
    let moved: Vec<_> = CENSUS
        .iter()
        .filter(|f| f.floor < f.ceiling)
        .map(|f| f.name)
        .collect();
    assert_eq!(moved, ["tenant manifest", "page shard manifest"]);
    for name in moved {
        let f = CENSUS.iter().find(|f| f.name == name).expect(name);
        assert_eq!(
            wrote[name], f.floor,
            "{name}: a plain object is at the older of its two formats"
        );
        assert!(wrote[name] < f.ceiling);
    }

    // And the numbers the census carries for those two are the ones the
    // imported constants hold, so the table is describing this build.
    assert_eq!(u64::from(MANIFEST_FORMAT), 3);
    assert_eq!(u64::from(PAGE_SHARD_FORMAT), 2);
    assert_eq!(wrote["shard map"], u64::from(MAP_FORMAT));
}

/// A binary format has nothing to freeze in the sense the json ones do,
/// because everything in it is behind a length and a checksum that an
/// older reader never reaches. What it does have is the one thing that
/// reader does reach: four bytes of magic and two of version, at the
/// front, before anything that could be refused for another reason.
/// That is what has to stay where it is, so that a reader from any
/// release can say which of the two of them is behind.
#[test]
fn a_binary_format_says_what_it_is_and_how_new_in_its_first_six_bytes() {
    let (segment, _) = SegmentBuilder::new(SegmentHeader {
        kind: SegmentKind::Landing,
        shard: 0,
        seq: 1,
        prev_digest: 0,
    })
    .finish();

    let (sealed, _) = build_sealed(
        SealedHeader {
            shard: 0,
            first_seq: 1,
            last_seq: 1,
        },
        &[],
        1 << 20,
    )
    .expect("an empty sealed segment");

    let (layer, _) = build_delta(
        &[DeltaEntry {
            key: LayerKey::page(1663, 5, 16384, 0, 0),
            lsn: Lsn(0x100),
            record: vec![1, 2, 3],
        }],
        1 << 16,
    )
    .expect("a delta layer");

    for (name, bytes, magic, version) in [
        ("landing segment", segment, SEGMENT_MAGIC, SEGMENT_VERSION),
        ("sealed segment", sealed, SEALED_MAGIC, SEALED_VERSION),
        ("layer", layer, LAYER_MAGIC, LAYER_VERSION),
    ] {
        assert_eq!(&bytes[..4], &magic, "{name}: the magic moved");
        assert_eq!(
            u16::from_le_bytes([bytes[4], bytes[5]]),
            version,
            "{name}: the version is not in the two bytes after the magic"
        );
        let floor = CENSUS.iter().find(|f| f.name == name).expect(name).floor;
        assert_eq!(u64::from(version), floor, "{name}: not at its floor");
    }
}
