//! End to end check of the redo pool against postgres itself: run a
//! real cluster, generate WAL, shut it down, then rebuild every page of
//! a table and its index purely from that WAL through the pool, and
//! compare the results with what postgres left on disk.
//!
//! Needs the patched build, so the test is gated on ZOU_PG_PREFIX
//! pointing at an install prefix with bin/initdb and friends, the thing
//! `make pg-build` leaves in build/pg. Without it the test passes after
//! printing a skip note, which keeps plain `cargo test` green on
//! machines without the build.
//!
//! The byte comparison masks exactly what postgres's own
//! wal_consistency_checking masks (bufmask.c, heap_mask, btree_mask):
//! hint bits, prune xids, unused space, tuple padding. Those legitimately
//! drift because the primary sets them without WAL. Unlike postgres's
//! checker the page LSN stays unmasked: the cluster runs with checksums
//! on and the pool is told so, and under checksums redo advances heap
//! page LSNs exactly like the primary, which is precisely the behavior
//! the checksum knob exists to preserve.
//!
//! The second half of the test is the production shape: recompute the
//! checksums on the materialized pages, write them over the relation
//! files, start postgres on them and check the data reads back.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use zou_pg::redo::{RedoPool, RedoPoolConfig, RedoRequest, page_checksum};
use zou_pg::walscan::{BlockRef, WalRecord, WalWindow, read_records};

const BLCKSZ: usize = 8192;
const WAL_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;

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

/// Concatenate the cluster's WAL segments into one window.
fn read_wal(datadir: &Path) -> WalWindow {
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
    WalWindow::from_raw(base, buf)
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

fn get_u16(page: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([page[at], page[at + 1]])
}

fn set_u16(page: &mut [u8], at: usize, v: u16) {
    page[at..at + 2].copy_from_slice(&v.to_le_bytes());
}

/// What bufmask.c masks on every page kind: the checksum, the header
/// hint bits (pd_prune_xid and the free lines, page full and all
/// visible flags) and the unused space between pd_lower and pd_upper.
fn mask_common(page: &mut [u8]) {
    page[8..10].fill(0);
    set_u16(page, 10, get_u16(page, 10) & !0x0007);
    page[20..24].fill(0);
    let lower = get_u16(page, 12) as usize;
    let upper = get_u16(page, 14) as usize;
    let special = get_u16(page, 16) as usize;
    assert!(
        (24..=upper).contains(&lower) && upper <= special && special <= BLCKSZ,
        "sane page bounds"
    );
    page[lower..upper].fill(0);
}

/// One line pointer: byte offset, flags, length.
fn line_pointer(page: &[u8], index: usize) -> (usize, u32, usize) {
    let at = 24 + 4 * index;
    let word = u32::from_le_bytes(page[at..at + 4].try_into().expect("checked length"));
    (
        (word & 0x7FFF) as usize,
        (word >> 15) & 3,
        (word >> 17) as usize,
    )
}

/// The heap_mask rules: xmin-unfrozen tuples lose all transaction hint
/// bits, frozen ones just the xmax hints, command ids are masked since
/// redo always uses the first one, and so is alignment padding after
/// each stored tuple.
fn mask_heap(page: &mut [u8]) {
    mask_common(page);
    let items = (get_u16(page, 12) as usize - 24) / 4;
    for index in 0..items {
        let (off, flags, len) = line_pointer(page, index);
        if flags == 1 {
            let infomask = get_u16(page, off + 20);
            let masked = if infomask & 0x0300 != 0x0300 {
                infomask & !0xFFF0
            } else {
                infomask & !(0x0800 | 0x0400)
            };
            set_u16(page, off + 20, masked);
            page[off + 8..off + 12].fill(0);
        }
        if len != 0 {
            let pad = (8 - len % 8) % 8;
            page[off + len..off + len + pad].fill(0);
        }
    }
}

/// The btree_mask rules: on leaf pages line pointer flags are unlogged
/// hints, and in the special area the garbage and split end flags plus
/// the cycle id go too.
fn mask_btree(page: &mut [u8]) {
    mask_common(page);
    let special = get_u16(page, 16) as usize;
    if get_u16(page, special + 12) & 0x0001 != 0 {
        let items = (get_u16(page, 12) as usize - 24) / 4;
        for index in 0..items {
            let at = 24 + 4 * index;
            let word = u32::from_le_bytes(page[at..at + 4].try_into().expect("checked length"));
            if (word >> 15) & 3 != 0 {
                page[at..at + 4].copy_from_slice(&(word & !(3 << 15)).to_le_bytes());
            }
        }
    }
    set_u16(page, special + 12, get_u16(page, special + 12) & !0x0060);
    page[special + 14..special + 16].fill(0);
}

/// Replay the relation's WAL through the pool, compare every main fork
/// block with the on disk file under the rmgr's masking rules, and
/// return the materialized pages.
fn check_rel(
    pool: &RedoPool,
    records: &[WalRecord],
    datadir: &Path,
    spc: u32,
    db: u32,
    rel: u32,
    mask: fn(&mut [u8]),
) -> Vec<Vec<u8>> {
    let disk = std::fs::read(
        datadir
            .join("base")
            .join(db.to_string())
            .join(rel.to_string()),
    )
    .expect("relation file exists");
    assert!(
        !disk.is_empty() && disk.len().is_multiple_of(BLCKSZ),
        "whole blocks"
    );
    let chain = records_for_rel(records, db, rel);
    assert!(!chain.is_empty(), "relation has wal");
    let gets: Vec<BlockRef> = (0..disk.len() / BLCKSZ)
        .map(|blk| BlockRef {
            spc,
            db,
            rel,
            fork: 0,
            blk: blk as u32,
        })
        .collect();
    let pages = pool
        .apply(&RedoRequest {
            pages: &[],
            records: &chain,
            gets: &gets,
        })
        .expect("redo batch succeeds");
    for (blk, page) in pages.iter().enumerate() {
        let mut want = disk[blk * BLCKSZ..(blk + 1) * BLCKSZ].to_vec();
        let mut got = page.clone();
        mask(&mut want);
        mask(&mut got);
        assert_eq!(got, want, "block {blk} of rel {rel} differs");
    }
    pages
}

/// Write materialized pages over a relation file with fresh checksums,
/// what the page service does before postgres gets to read them.
fn install_pages(datadir: &Path, db: u32, rel: u32, pages: &[Vec<u8>]) {
    let mut file = Vec::with_capacity(pages.len() * BLCKSZ);
    for (blk, page) in pages.iter().enumerate() {
        let mut page = page.clone();
        if page.iter().any(|b| *b != 0) {
            let sum = page_checksum(&page, blk as u32);
            set_u16(&mut page, 8, sum);
        }
        file.extend_from_slice(&page);
    }
    std::fs::write(
        datadir
            .join("base")
            .join(db.to_string())
            .join(rel.to_string()),
        file,
    )
    .expect("writable relation file");
}

#[test]
fn pool_pages_match_postgres() {
    let Ok(prefix) = std::env::var("ZOU_PG_PREFIX") else {
        eprintln!("ZOU_PG_PREFIX not set, skipping the redo pool integration test");
        return;
    };
    let prefix = PathBuf::from(prefix);
    let bin = |name: &str| prefix.join("bin").join(name);
    let tmp = tempfile::tempdir().expect("tempdir");
    let datadir = tmp.path().join("data");
    let sock = tmp.path().join("sock");
    std::fs::create_dir(&sock).expect("socket dir");

    run(Command::new(bin("initdb"))
        .args(["--no-sync", "-k", "-A", "trust", "-U", "zou", "-D"])
        .arg(&datadir));
    let start = |datadir: &Path| {
        run(Command::new(bin("pg_ctl"))
            .arg("-D")
            .arg(datadir)
            .arg("-l")
            .arg(tmp.path().join("pg.log"))
            .arg("-o")
            .arg(format!(
                "-c listen_addresses='' -c unix_socket_directories='{}' -c autovacuum=off",
                sock.display()
            ))
            .args(["-w", "start"]));
    };
    let stop = |datadir: &Path| {
        run(Command::new(bin("pg_ctl"))
            .arg("-D")
            .arg(datadir)
            .args(["stop", "-m", "fast"]));
    };
    let psql = |sql: &str| {
        run(Command::new(bin("psql"))
            .arg("-h")
            .arg(&sock)
            .args(["-U", "zou", "-d", "postgres", "-X", "-qAt", "-c", sql]))
    };

    // Several pages of inserts, then updates and deletes so early pages
    // collect plain records on top of their first touch images, and a
    // vacuum for pruning, freezing and visibility records. The wal scan
    // starts here rather than at the segment start, initdb's template
    // database copies are megabytes of records that prove nothing.
    start(&datadir);
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
    let fingerprint =
        "SELECT count(*) || ':' || md5(string_agg(id::text || v, ',' ORDER BY id)) FROM t";
    let by_index = "SET enable_seqscan = off; SELECT sum(id) FROM t WHERE id > 0";
    let expected = psql(fingerprint);
    let expected_by_index = psql(by_index);
    stop(&datadir);

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

    let window = read_wal(&datadir);
    let out = read_records(&window, wal_start, None).expect("wal parses");
    assert!(out.records.len() > 100, "cluster produced wal");

    let pool = RedoPool::new(RedoPoolConfig {
        postgres: bin("postgres"),
        scratch_root: tmp.path().join("scratch"),
        workers: 2,
        batch_timeout: Duration::from_secs(30),
        batches_per_worker: 8,
        data_checksums: true,
    });
    let heap_pages = check_rel(&pool, &out.records, &datadir, spc, db, heap_rel, mask_heap);
    let index_pages = check_rel(
        &pool,
        &out.records,
        &datadir,
        spc,
        db,
        index_rel,
        mask_btree,
    );

    // A garbage record kills the worker and surfaces as an error, and
    // the next batch gets a fresh worker, so the pool recovers.
    let mut garbage = vec![0xAAu8; 64];
    garbage[..4].copy_from_slice(&64u32.to_le_bytes());
    let err = pool.apply(&RedoRequest {
        pages: &[],
        records: &[(
            window.covered_from + 0x28,
            window.covered_from + 0x28 + 64,
            &garbage,
        )],
        gets: &[BlockRef {
            spc,
            db,
            rel: heap_rel,
            fork: 0,
            blk: 0,
        }],
    });
    assert!(err.is_err(), "garbage record must fail");
    check_rel(&pool, &out.records, &datadir, spc, db, heap_rel, mask_heap);

    // The production shape: put the materialized pages where postgres
    // will read them, freshly checksummed, and check the data survives
    // a real scan through both the heap and the index.
    install_pages(&datadir, db, heap_rel, &heap_pages);
    install_pages(&datadir, db, index_rel, &index_pages);
    start(&datadir);
    assert_eq!(psql(fingerprint), expected, "heap scan differs");
    assert_eq!(psql(by_index), expected_by_index, "index scan differs");
    stop(&datadir);
}
