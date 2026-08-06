//! The branch and PITR equivalence corpus for the page shard path: a
//! child branched at a checkpoint reconstructs every key at every lsn
//! up to the branch point exactly as its parent does, keeps serving
//! that history frozen while the parent moves on, layers its own
//! future on top, and refuses to serve at all without the shard
//! context that resolves inherited layers to their owner's prefix.

use zou_store::branch::branch;
use zou_store::cas::CasStore;
use zou_store::layer::{
    DeltaEntry, ImageEntry, LayerKey, PAGE_IMAGE_LEN, build_delta, build_image,
};
use zou_store::layermap::{LayerDesc, LayerMap};
use zou_store::layout::TenantLayout;
use zou_store::lsn::Lsn;
use zou_store::manifest::{CheckpointKind, CheckpointRef, Manifest};
use zou_store::mem::MemStore;
use zou_store::memtable::Memtable;
use zou_store::pageread::{LayerReader, ReadError};
use zou_store::shardmanifest::{LayerEntry, PageShardManifest, publish_layer};

fn k(block: u32) -> LayerKey {
    LayerKey::page(1663, 5, 16384, 0, block)
}

fn page(fill: u8) -> Vec<u8> {
    vec![fill; PAGE_IMAGE_LEN]
}

fn rec(b: u32, at: u64) -> Vec<u8> {
    format!("r{b}@{at}").into_bytes()
}

/// Put one built layer under the tenant's shard 0 and list it in the
/// SHARD manifest, the same two steps flush takes.
fn publish(store: &MemStore, tenant: &str, bytes: &[u8], desc: &LayerDesc, dcl: u64) {
    let layout = TenantLayout::new(tenant);
    store
        .put(&format!("{}{}", layout.shard_prefix(0), desc.name()), bytes)
        .unwrap();
    let entry = LayerEntry {
        name: desc.name(),
        size: bytes.len() as u64,
        owner: None,
        upto: None,
    };
    publish_layer(store, &layout.shard_manifest(0), 0, &entry, Lsn(dcl)).unwrap();
}

fn image_layer(entries: &[ImageEntry], lsn: u64) -> (Vec<u8>, LayerDesc) {
    let (buf, footer) = build_image(entries, Lsn(lsn), 3 * PAGE_IMAGE_LEN).unwrap();
    let desc = LayerDesc::from_footer(&footer, buf.len() as u64);
    (buf, desc)
}

fn delta_layer(mut entries: Vec<DeltaEntry>) -> (Vec<u8>, LayerDesc) {
    entries.sort_by_key(|e| (e.key, e.lsn));
    let (buf, footer) = build_delta(&entries, 512).unwrap();
    let desc = LayerDesc::from_footer(&footer, buf.len() as u64);
    (buf, desc)
}

/// The parent "p": an image at 100 for blocks 0..10, a delta
/// generation before the branch point, one spanning it, and an image
/// the parent took after it. Checkpoints at 100 and 250 are the fold
/// grid branch points must sit on.
fn build_parent(store: &MemStore) {
    let images: Vec<ImageEntry> = (0..10u32)
        .map(|b| ImageEntry {
            key: k(b),
            page: page(b as u8),
        })
        .collect();
    let (buf, desc) = image_layer(&images, 100);
    publish(store, "p", &buf, &desc, 100);

    let mut d1 = Vec::new();
    for b in [1u32, 3] {
        for at in [101u64, 150, 200] {
            d1.push(DeltaEntry {
                key: k(b),
                lsn: Lsn(at),
                record: rec(b, at),
            });
        }
    }
    let (buf, desc) = delta_layer(d1);
    publish(store, "p", &buf, &desc, 200);

    let mut d2 = Vec::new();
    for b in [3u32, 7] {
        for at in [201u64, 250, 300, 400] {
            d2.push(DeltaEntry {
                key: k(b),
                lsn: Lsn(at),
                record: rec(b, at),
            });
        }
    }
    let (buf, desc) = delta_layer(d2);
    publish(store, "p", &buf, &desc, 400);

    // The parent's later image: reads past 300 on the parent start
    // here, and it must never leak into a child branched at 250.
    let images: Vec<ImageEntry> = (0..10u32)
        .map(|b| ImageEntry {
            key: k(b),
            page: page(b as u8 + 100),
        })
        .collect();
    let (buf, desc) = image_layer(&images, 300);
    publish(store, "p", &buf, &desc, 400);

    let mut m = Manifest::new("p", 18);
    m.checkpoints = vec![
        CheckpointRef {
            id: "f1".into(),
            lsn: Lsn(100),
            kind: CheckpointKind::Full,
            owner: None,
        },
        CheckpointRef {
            id: "d2".into(),
            lsn: Lsn(250),
            kind: CheckpointKind::Delta,
            owner: None,
        },
    ];
    store
        .put_if_absent(&TenantLayout::new("p").manifest(), &m.to_json())
        .unwrap();
}

fn shard_map(store: &MemStore, tenant: &str) -> LayerMap {
    let key = TenantLayout::new(tenant).shard_manifest(0);
    let (m, _) = PageShardManifest::load(store, &key).unwrap().unwrap();
    m.layer_map().unwrap()
}

#[test]
fn the_child_reconstructs_the_parents_history_exactly() {
    let store = MemStore::default();
    build_parent(&store);
    branch(&store, "p", "c", Some(Lsn(250)), 5000).unwrap();

    let pmap = shard_map(&store, "p");
    let cmap = shard_map(&store, "c");
    let preader = LayerReader::for_shard(&store, "p", 0);
    let creader = LayerReader::for_shard(&store, "c", 0);
    let mem = Memtable::new();

    // Every block at every lsn up to the branch point: the whole
    // reconstruction agrees, base, chain, and read amplification.
    for b in 0..10u32 {
        for at in [100u64, 101, 150, 200, 201, 249, 250] {
            let want = preader.reconstruct(&pmap, &mem, &k(b), Lsn(at)).unwrap();
            let got = creader.reconstruct(&cmap, &mem, &k(b), Lsn(at)).unwrap();
            assert_eq!(got, want, "block {b} at lsn {at}");
        }
    }
}

#[test]
fn the_childs_inherited_history_freezes_at_the_branch_point() {
    let store = MemStore::default();
    build_parent(&store);
    branch(&store, "p", "c", Some(Lsn(250)), 5000).unwrap();

    let pmap = shard_map(&store, "p");
    let cmap = shard_map(&store, "c");
    let preader = LayerReader::for_shard(&store, "p", 0);
    let creader = LayerReader::for_shard(&store, "c", 0);
    let mem = Memtable::new();

    // Far past the branch point the child still serves the parent's
    // state as of 250: the spanning delta's records above the cut and
    // the parent's later image never leak in.
    let frozen = preader.reconstruct(&pmap, &mem, &k(3), Lsn(250)).unwrap();
    let got = creader.reconstruct(&cmap, &mem, &k(3), Lsn(1000)).unwrap();
    assert_eq!(got.base, frozen.base);
    assert_eq!(got.base_lsn, frozen.base_lsn);
    assert_eq!(got.records, frozen.records);

    // While the parent itself moved on to its later image.
    let moved = preader.reconstruct(&pmap, &mem, &k(3), Lsn(1000)).unwrap();
    assert_eq!(moved.base, Some(page(103)));
    assert_ne!(moved.records, got.records);

    // The child's own writes layer on top of the frozen history.
    let mut cmem = Memtable::new();
    cmem.insert(k(3), Lsn(260), b"c3@260".to_vec());
    let got = creader.reconstruct(&cmap, &cmem, &k(3), Lsn(1000)).unwrap();
    let lsns: Vec<u64> = got.records.iter().map(|(l, _)| l.0).collect();
    assert_eq!(
        lsns,
        vec![101, 150, 200, 201, 250, 260],
        "inherited chain to the cut, then the child's own future"
    );
}

#[test]
fn a_grandchild_still_reads_from_the_original_prefix() {
    let store = MemStore::default();
    build_parent(&store);
    branch(&store, "p", "c", Some(Lsn(250)), 5000).unwrap();
    branch(&store, "c", "g", Some(Lsn(100)), 6000).unwrap();

    let pmap = shard_map(&store, "p");
    let gmap = shard_map(&store, "g");
    let preader = LayerReader::for_shard(&store, "p", 0);
    let greader = LayerReader::for_shard(&store, "g", 0);
    let mem = Memtable::new();

    for b in 0..10u32 {
        let want = preader.reconstruct(&pmap, &mem, &k(b), Lsn(100)).unwrap();
        let got = greader.reconstruct(&gmap, &mem, &k(b), Lsn(100)).unwrap();
        assert_eq!(got, want, "block {b} two hops down");
    }
}

#[test]
fn a_reader_without_shard_context_refuses_inherited_layers() {
    let store = MemStore::default();
    build_parent(&store);
    branch(&store, "p", "c", Some(Lsn(250)), 5000).unwrap();

    let cmap = shard_map(&store, "c");
    let plain = LayerReader::new(&store, TenantLayout::new("c").shard_prefix(0));
    assert!(matches!(
        plain.reconstruct(&cmap, &Memtable::new(), &k(3), Lsn(200)),
        Err(ReadError::ForeignLayer { .. })
    ));
}
