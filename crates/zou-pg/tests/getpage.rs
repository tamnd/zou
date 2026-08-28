//! End to end check of the GetPage path: run a real cluster, take its
//! WAL, push it through the v2 pipeline the way production does, frames
//! into a shard ingest, flush to a layer in the store, then serve every
//! page of a table and its index through the page service and compare
//! with a direct redo pool replay of the same WAL. Both sides come out
//! of the same redo workers from the same records, so the comparison is
//! byte for byte, no bufmask style masking needed.
//!
//! Gated on ZOU_PG_PREFIX exactly like the redo pool test: without the
//! patched build the test prints a skip note and passes.
//!
//! Six scenarios share one cluster:
//! - everything flushed: pages come purely from the layer store
//! - the same reads through the local file cache, twice: once to fill
//!   it, once from a fresh cache instance over a store stripped of
//!   every layer, the restart story
//! - a partial flush: the tail of the WAL still sits in the memtable,
//!   so reconstruction stitches layer records and memtable records
//! - per block read lsns, the positions the shim's last written lsn
//!   table hands out, serve the same pages as a read at the very end
//!   of the WAL
//! - a compaction pass folds the runs into one delta plus a fresh image
//!   and no page a reader gets moves an inch
//! - a fold in the middle of the stream, on the boundary where the next
//!   record starts at the flush point, the shape of zou #358

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use zou_pg::getpage::{MAX_GETPAGE_BATCH, PageService};
use zou_pg::ingest::{IngestConfig, ShardIngest};
use zou_pg::redo::{RedoPool, RedoPoolConfig, RedoRequest, page_checksum};
use zou_pg::walscan::{BlockRef, WalRecord, WalWindow, read_records};
use zou_store::cas::CasStore;
use zou_store::filecache::FileCache;
use zou_store::frame::Frame2;
use zou_store::layout::TenantLayout;
use zou_store::lsn::Lsn;
use zou_store::mem::MemStore;
use zou_store::shardmanifest::PageShardManifest;

const BLCKSZ: usize = 8192;
const WAL_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;
const TENANT: u128 = 7;

fn run(cmd: &mut Command) -> String {
    let out = cmd
        .env_remove("ZOU_TARGET")
        .output()
        .unwrap_or_else(|err| panic!("spawn {cmd:?}: {err}"));
    assert!(
        out.status.success(),
        "{cmd:?} failed: {}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// The start LSN a segment file name encodes, if it is one.
fn segment_start(name: &str) -> Option<u64> {
    if name.len() != 24 || !name.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let log = u64::from_str_radix(&name[8..16], 16).ok()?;
    let seg = u64::from_str_radix(&name[16..24], 16).ok()?;
    Some((log << 32) + seg * WAL_SEGMENT_SIZE)
}

/// Concatenate the cluster's WAL segments into one flat buffer.
fn read_wal(datadir: &Path) -> (u64, Vec<u8>) {
    let mut segments = Vec::new();
    for entry in std::fs::read_dir(datadir.join("pg_wal")).expect("pg_wal exists") {
        let entry = entry.expect("readable dir");
        let name = entry.file_name();
        let Some(start) = segment_start(&name.to_string_lossy()) else {
            continue;
        };
        segments.push((
            start,
            std::fs::read(entry.path()).expect("readable segment"),
        ));
    }
    segments.sort_by_key(|(start, _)| *start);
    let base = segments.first().expect("wal exists").0;
    let end = segments
        .iter()
        .map(|(start, data)| start + data.len() as u64)
        .max()
        .expect("wal exists");
    let mut buf = vec![0u8; (end - base) as usize];
    for (start, data) in &segments {
        let at = (start - base) as usize;
        buf[at..at + data.len()].copy_from_slice(data);
    }
    (base, buf)
}

/// The records that touch any block of the relation, in WAL order.
fn records_for_rel(records: &[WalRecord], db: u32, rel: u32) -> Vec<(u64, u64, &[u8])> {
    records
        .iter()
        .filter(|record| {
            record
                .block_refs()
                .expect("well formed record")
                .iter()
                .any(|blk| blk.db == db && blk.rel == rel)
        })
        .map(|record| (record.lsn, record.end_lsn, record.bytes.as_slice()))
        .collect()
}

/// Fresh checksums over redo pool output, matching what the page
/// service stamps, so the two sides compare byte for byte.
fn restamp(pages: &mut [Vec<u8>]) {
    for (blk, page) in pages.iter_mut().enumerate() {
        if page.iter().any(|b| *b != 0) {
            let sum = page_checksum(page, blk as u32);
            page[8..10].copy_from_slice(&sum.to_le_bytes());
        }
    }
}

/// The stream sliced into frames the way the safekeeper tee delivers
/// it: contiguous, no hints, an awkward chunk size so records split
/// across frame boundaries.
fn frames_over(wal_start: u64, raw: &[u8]) -> Vec<Frame2> {
    let mut frames = Vec::new();
    let mut at = wal_start;
    for chunk in raw.chunks(96 * 1024) {
        frames.push(Frame2 {
            tenant: TENANT,
            writer_epoch: 1,
            start_lsn: Lsn(at),
            end_lsn: Lsn(at + chunk.len() as u64),
            contains_commit: false,
            first_of_epoch: false,
            hints: Vec::new(),
            payload: chunk.to_vec(),
        });
        at += chunk.len() as u64;
    }
    frames
}

fn blocks_of(datadir: &Path, spc: u32, db: u32, rel: u32) -> Vec<BlockRef> {
    let len = std::fs::metadata(
        datadir
            .join("base")
            .join(db.to_string())
            .join(rel.to_string()),
    )
    .expect("relation file exists")
    .len() as usize;
    assert!(len > 0 && len.is_multiple_of(BLCKSZ), "whole blocks");
    (0..len / BLCKSZ)
        .map(|blk| BlockRef {
            spc,
            db,
            rel,
            fork: 0,
            blk: blk as u32,
        })
        .collect()
}

#[test]
fn getpage_serves_what_redo_builds() {
    let Ok(prefix) = std::env::var("ZOU_PG_PREFIX") else {
        eprintln!("ZOU_PG_PREFIX not set, skipping the getpage integration test");
        return;
    };
    let prefix = PathBuf::from(prefix);
    let bin = |name: &str| prefix.join("bin").join(name);
    let tmp = tempfile::tempdir().expect("tempdir");
    let datadir = tmp.path().join("data");
    let sock = tmp.path().join("sock");
    std::fs::create_dir(&sock).expect("socket dir");

    run(Command::new(bin("initdb"))
        .args(["--no-sync", "-k", "-A", "trust", "-U", "zou"])
        .args(["--set", "full_page_writes=off", "-D"])
        .arg(&datadir));
    run(Command::new(bin("pg_ctl"))
        .arg("-D")
        .arg(&datadir)
        .arg("-l")
        .arg(tmp.path().join("pg.log"))
        .arg("-o")
        .arg(format!(
            "-c listen_addresses='' -c unix_socket_directories='{}' -c autovacuum=off",
            sock.display()
        ))
        .args(["-w", "start"]));
    let psql = |sql: &str| {
        run(Command::new(bin("psql"))
            .arg("-h")
            .arg(&sock)
            .args(["-U", "zou", "-d", "postgres", "-X", "-qAt", "-c", sql]))
    };

    // The same workload shape as the redo pool test: inserts across
    // several pages, then updates, deletes and a vacuum so early pages
    // collect long record chains on top of their birth records, with
    // no first touch images anywhere, full_page_writes is off.
    let wal_start = {
        let lsn = psql("SELECT pg_current_wal_lsn()");
        let (hi, lo) = lsn.split_once('/').expect("lsn format");
        (u64::from_str_radix(hi, 16).expect("lsn format") << 32)
            + u64::from_str_radix(lo, 16).expect("lsn format")
    };
    psql("CREATE TABLE t (id int primary key, v text)");
    psql("INSERT INTO t SELECT g, repeat('x', 500) FROM generate_series(1, 200) g");
    psql("UPDATE t SET v = repeat('y', 500) WHERE id <= 20");
    psql("UPDATE t SET v = repeat('z', 500) WHERE id % 3 = 0");
    psql("DELETE FROM t WHERE id % 7 = 0");
    psql("VACUUM t");
    let heap = psql("SELECT pg_relation_filepath('t')");
    let index = psql("SELECT pg_relation_filepath('t_pkey')");
    run(Command::new(bin("pg_ctl"))
        .arg("-D")
        .arg(&datadir)
        .args(["stop", "-m", "fast"]));

    let parse = |path: &str| {
        let mut parts = path.split('/');
        assert_eq!(parts.next(), Some("base"), "table sits in base");
        let db: u32 = parts.next().expect("db oid").parse().expect("db oid");
        let rel: u32 = parts
            .next()
            .expect("relfilenode")
            .parse()
            .expect("relfilenode");
        (db, rel)
    };
    let (db, heap_rel) = parse(&heap);
    let (_, index_rel) = parse(&index);
    let spc = 1663; // pg_default, the base directory

    let (base, buf) = read_wal(&datadir);
    let window = WalWindow::from_raw(base, buf.clone());
    let out = read_records(&window, wal_start, None).expect("wal parses");
    assert!(out.records.len() > 100, "cluster produced wal");
    // Cut at the last record's end, not at resume: the scan treats the
    // zero padding after the final record as xlog switch padding and
    // reports resume behind the next segment boundary, megabytes past
    // the last byte that ever existed on the wire.
    let wal_end = out.records.last().expect("records exist").end_lsn;
    let raw = &buf[(wal_start - base) as usize..(wal_end - base) as usize];

    // full_page_writes is off, so nothing in this corpus carries an
    // image as torn page protection. The images that remain are the
    // records whose redo is the image itself, the index build's
    // log_newpage under the xlog rmgr, and none of them reference the
    // heap: every heap page served below is reconstructed from its
    // birth record and deltas alone.
    let mut xlog_images = 0;
    for r in &out.records {
        if r.carries_image().expect("record parses") {
            assert_eq!(r.rmid, 0, "image outside the xlog rmgr at {:#X}", r.lsn);
            let refs = r.block_refs().expect("record parses");
            assert!(
                refs.iter().all(|b| b.rel != heap_rel),
                "a heap block got an image at {:#X}",
                r.lsn
            );
            xlog_images += 1;
        }
    }
    assert!(xlog_images > 0, "the index build still writes real images");

    let pool = RedoPool::new(RedoPoolConfig {
        postgres: bin("postgres"),
        scratch_root: tmp.path().join("scratch"),
        workers: 2,
        batch_timeout: Duration::from_secs(30),
        batches_per_worker: 8,
        data_checksums: true,
    });

    // The reference: a direct redo replay of each relation's chain,
    // checksummed the way the page service checksums.
    let heap_blocks = blocks_of(&datadir, spc, db, heap_rel);
    let index_blocks = blocks_of(&datadir, spc, db, index_rel);
    let direct = |blocks: &[BlockRef], rel: u32| {
        let chain = records_for_rel(&out.records, db, rel);
        assert!(!chain.is_empty(), "relation has wal");
        let mut pages = pool
            .apply(&RedoRequest {
                pages: &[],
                records: &chain,
                gets: blocks,
            })
            .expect("redo batch succeeds");
        restamp(&mut pages);
        pages
    };
    let heap_want = direct(&heap_blocks, heap_rel);
    let index_want = direct(&index_blocks, index_rel);

    let frames = frames_over(wal_start, raw);
    assert!(frames.len() > 2, "the stream split into several frames");
    let layout = TenantLayout::new("t");

    let serve = |ingest: &ShardIngest, store: &dyn CasStore, blocks: &[BlockRef], at: u64| {
        let map = match PageShardManifest::load(store, &layout.shard_manifest(0))
            .expect("manifest loads")
        {
            Some((manifest, _)) => manifest.layer_map().expect("map builds"),
            None => zou_store::layermap::LayerMap::new(Vec::new()).expect("empty map"),
        };
        let svc = PageService::new(store, layout.shard_prefix(0), Some(&pool), true);
        let mut pages = Vec::new();
        for chunk in blocks.chunks(MAX_GETPAGE_BATCH) {
            pages.extend(
                svc.get_pages(&map, ingest.memtable(), chunk, at)
                    .expect("getpage succeeds"),
            );
        }
        pages
    };

    // Scenario one: the whole stream ingested and flushed, every page
    // served from the layer store.
    let store = MemStore::default();
    let mut ingest = ShardIngest::new(IngestConfig::new(TENANT, 0, 1), wal_start);
    ingest.apply_frames(&frames).expect("frames apply");
    assert_eq!(ingest.applied(), wal_end, "the whole stream parsed");
    ingest
        .flush(&store, &layout)
        .expect("flush")
        .expect("layer");
    assert!(ingest.memtable().is_empty(), "flush drained");
    assert_eq!(
        serve(&ingest, &store, &heap_blocks, wal_end),
        heap_want,
        "heap from layers differs from direct redo"
    );
    assert_eq!(
        serve(&ingest, &store, &index_blocks, wal_end),
        index_want,
        "index from layers differs from direct redo"
    );

    // The same reads through the local file cache: the first pass
    // fills it, then a fresh cache instance over a store holding
    // nothing but the shard manifest serves identical pages, which is
    // the restart story, layer bytes from NVMe, only the mutable
    // manifest from the store.
    let cache_root = tmp.path().join("layer-cache");
    let manifest_key = layout.shard_manifest(0);
    let manifest_bytes = store
        .get(&manifest_key)
        .expect("store get")
        .expect("manifest exists")
        .0;
    let cached = FileCache::new(Box::new(store), &cache_root, 1 << 30).expect("cache opens");
    assert_eq!(
        serve(&ingest, &cached, &heap_blocks, wal_end),
        heap_want,
        "heap through a cold cache differs"
    );
    assert!(cached.stats().fills > 0, "the pass filled the cache");
    let survivors = MemStore::default();
    survivors
        .put_if_absent(&manifest_key, &manifest_bytes)
        .expect("manifest survives");
    let warm = FileCache::new(Box::new(survivors), &cache_root, 1 << 30).expect("cache reopens");
    assert_eq!(
        serve(&ingest, &warm, &heap_blocks, wal_end),
        heap_want,
        "heap after a restart with no layers in the store differs"
    );
    assert!(
        warm.stats().hits > 0 && warm.stats().misses == 0,
        "every layer read came from disk"
    );

    // Scenario two: flush halfway, leave the tail in the memtable, so
    // reconstruction stitches layer records and memtable records.
    let store = MemStore::default();
    let mut ingest = ShardIngest::new(IngestConfig::new(TENANT, 0, 1), wal_start);
    let (head, tail) = frames.split_at(frames.len() / 2);
    ingest.apply_frames(head).expect("head applies");
    ingest
        .flush(&store, &layout)
        .expect("flush")
        .expect("layer");
    ingest.apply_frames(tail).expect("tail applies");
    assert!(
        !ingest.memtable().is_empty(),
        "the tail landed in the memtable"
    );
    assert_eq!(
        serve(&ingest, &store, &heap_blocks, wal_end),
        heap_want,
        "heap from layers plus memtable differs from direct redo"
    );
    assert_eq!(
        serve(&ingest, &store, &index_blocks, wal_end),
        index_want,
        "index from layers plus memtable differs from direct redo"
    );

    // Scenario three: reads at per block lsns serve the same pages as
    // reads at the stream end. The shim keeps these positions in shared
    // memory off its own writebacks; here they come off the records,
    // which is the same number by a different route, the end lsn of the
    // last record that touched the block.
    let mut lw: std::collections::HashMap<BlockRef, u64> = std::collections::HashMap::new();
    for record in &out.records {
        for blk in record.block_refs().expect("well formed") {
            let slot = lw.entry(blk).or_insert(wal_start);
            *slot = (*slot).max(record.end_lsn);
        }
    }
    let map = PageShardManifest::load(&store, &layout.shard_manifest(0))
        .expect("manifest loads")
        .expect("manifest exists")
        .0
        .layer_map()
        .expect("map builds");
    let svc = PageService::new(&store, layout.shard_prefix(0), Some(&pool), true);
    for (index, blk) in heap_blocks.iter().enumerate().step_by(7) {
        let at = lw.get(blk).copied().unwrap_or(wal_start);
        assert!(at >= wal_start && at <= wal_end, "cache lsn in range");
        let page = svc
            .get_page(&map, ingest.memtable(), *blk, at)
            .expect("getpage succeeds");
        assert_eq!(
            page, heap_want[index],
            "block {index} at its last written lsn {at:#x} differs"
        );
    }

    // Scenario four: compaction with the pool folds the two runs into
    // one merged delta plus a fresh image at the flush point, and no
    // page a reader gets moves an inch.
    ingest
        .flush(&store, &layout)
        .expect("flush")
        .expect("layer");
    store
        .put_if_absent(
            &layout.manifest(),
            &zou_store::manifest::Manifest::new("t", 18).to_json(),
        )
        .expect("tenant manifest");
    let outcome = zou_pg::compact::compact_shard(&store, "t", 0, Some(&pool), true)
        .expect("compaction succeeds")
        .expect("two runs are work");
    assert_eq!(outcome.retired, 2, "both flushed runs retired");
    assert!(
        outcome.debt_after < outcome.debt_before,
        "the image erased delta debt: {} to {}",
        outcome.debt_before,
        outcome.debt_after
    );
    let manifest = PageShardManifest::load(&store, &layout.shard_manifest(0))
        .expect("manifest loads")
        .expect("manifest exists")
        .0;
    let map = manifest.layer_map().expect("map builds");
    use zou_store::layer::LayerKind;
    assert!(
        map.layers().iter().any(
            |d| d.kind == LayerKind::Image && d.min_lsn.0 + 1 == manifest.disk_consistent_lsn.0
        ),
        "an image sits just under the flush point"
    );
    assert_eq!(
        map.layers()
            .iter()
            .filter(|d| d.kind == LayerKind::Delta)
            .count(),
        1,
        "one merged run"
    );
    assert_eq!(
        serve(&ingest, &store, &heap_blocks, wal_end),
        heap_want,
        "heap after compaction differs"
    );
    assert_eq!(
        serve(&ingest, &store, &index_blocks, wal_end),
        index_want,
        "index after compaction differs"
    );
    // And a current read pays the image alone, the read amp bound the
    // whole pass exists to buy.
    for blk in heap_blocks.iter().step_by(11) {
        let key =
            zou_store::layer::LayerKey::page(blk.spc, blk.db, blk.rel, blk.fork as u8, blk.blk);
        let plan = map.plan(&key, Lsn(wal_end));
        let image = plan.images.first().expect("an image serves {key:?}");
        assert_eq!(
            plan.above(image.min_lsn).count(),
            0,
            "no deltas above the image for {key:?}"
        );
    }

    // Scenario five: fold in the middle of the stream, on the exact
    // boundary where the next record starts at the flush point.
    //
    // This is zou #358. The flush publishes its resume point as the
    // disk consistent lsn, so the layers hold every record below it
    // and the record starting at it is still only in the log. An image
    // cut at that lsn claims the record anyway, every later read of
    // the page skips it, and the next insert into the page redoes at
    // an offset the page has no room for and kills the redo worker.
    // The cut has to sit one byte under the flush point.
    let cut = {
        // A heap record that is not the first for its block, so the
        // page it touches has a history the fold will image.
        let mut seen = std::collections::HashSet::new();
        let mut chosen = None;
        for record in &out.records {
            for blk in record.block_refs().expect("well formed") {
                if blk.rel != heap_rel {
                    continue;
                }
                if !seen.insert((blk.rel, blk.blk)) && chosen.is_none() && record.lsn > wal_start {
                    chosen = Some(record.lsn);
                }
            }
        }
        chosen.expect("a heap page gets a second record")
    };
    let store = MemStore::default();
    store
        .put_if_absent(
            &layout.manifest(),
            &zou_store::manifest::Manifest::new("t", 18).to_json(),
        )
        .expect("tenant manifest");
    let mut ingest = ShardIngest::new(IngestConfig::new(TENANT, 0, 1), wal_start);
    ingest
        .apply_frames(&frames_over(wal_start, &raw[..(cut - wal_start) as usize]))
        .expect("head applies");
    assert_eq!(ingest.applied(), cut, "the flush point is the record start");
    ingest
        .flush(&store, &layout)
        .expect("flush")
        .expect("layer");
    zou_pg::compact::compact_shard(&store, "t", 0, Some(&pool), true)
        .expect("compaction succeeds")
        .expect("an image is work");
    ingest
        .apply_frames(&frames_over(cut, &raw[(cut - wal_start) as usize..]))
        .expect("tail applies");
    ingest
        .flush(&store, &layout)
        .expect("flush")
        .expect("layer");
    assert_eq!(
        serve(&ingest, &store, &heap_blocks, wal_end),
        heap_want,
        "heap folded at a record boundary differs from direct redo"
    );
    assert_eq!(
        serve(&ingest, &store, &index_blocks, wal_end),
        index_want,
        "index folded at a record boundary differs from direct redo"
    );
}
