//! `zou inspect <file>` or `zou inspect <target> <key>`: decode one
//! layer object and print what the footer claims, then prove it by
//! fully decoding every block.
//!
//! This is the standalone decoder the format discipline demands: it
//! shares the codec with the server but nothing else, takes no lease,
//! and writes nothing, so it is safe to point at a live store or at a
//! file copied out of one.

use zou_pg::walscan;
use zou_store::layer::{
    LayerKey, LayerKind, PAGE_IMAGE_LEN, decode_delta, decode_image, read_layer_footer,
};
use zou_store::layout::TenantLayout;
use zou_store::lsn::Lsn;
use zou_store::memtable::Memtable;
use zou_store::pageread::LayerReader;
use zou_store::shardmanifest::PageShardManifest;
use zou_store::{CasStore, open_store};

pub const USAGE: &str = "usage: zou inspect <file | target key | \
     target chain <ref> <shard> <spc/db/rel/fork/block> [at]>";

pub fn run(argv: &[String]) -> Result<(), String> {
    let bytes = match argv {
        [file] => std::fs::read(file).map_err(|e| format!("{file}: {e}"))?,
        [target, verb, tenant_ref, shard, block, rest @ ..] if verb == "chain" => {
            return chain(target, tenant_ref, shard, block, rest);
        }
        [target, key] => {
            let store: Box<dyn CasStore> = open_store(target)?;
            store
                .get(key)
                .map_err(|e| format!("store: {e}"))?
                .ok_or_else(|| format!("{target} has no object {key}"))?
                .0
        }
        _ => return Err(USAGE.into()),
    };

    let footer = read_layer_footer(&bytes).map_err(|e| format!("layer: {e}"))?;
    let kind = match footer.kind {
        LayerKind::Delta => "delta",
        LayerKind::Image => "image",
    };
    println!("{kind} layer, {} bytes", bytes.len());
    println!("keys {} .. {}", footer.min_key.hex(), footer.max_key.hex());
    match footer.kind {
        LayerKind::Delta => println!("lsns {:#X} .. {:#X}", footer.min_lsn.0, footer.max_lsn.0),
        LayerKind::Image => println!("lsn {:#X}", footer.min_lsn.0),
    }
    println!(
        "{} entries in {} blocks, bloom {} bytes",
        footer.entry_count,
        footer.blocks.len(),
        footer.bloom.bits().len()
    );
    for (i, b) in footer.blocks.iter().enumerate() {
        println!(
            "  block {i}: {} entries, {} bytes at {}, {} raw, keys {} .. {}",
            b.entries,
            b.len,
            b.offset,
            b.raw_len,
            b.first_key.hex(),
            b.last_key.hex()
        );
    }

    // The footer is only a claim until the blocks decode against it.
    match footer.kind {
        LayerKind::Delta => {
            let (entries, _) = decode_delta(&bytes).map_err(|e| format!("layer: {e}"))?;
            let payload: usize = entries.iter().map(|e| e.record.len()).sum();
            println!(
                "verified: {} records, {payload} record bytes",
                entries.len()
            );
        }
        LayerKind::Image => {
            let (entries, _) = decode_image(&bytes).map_err(|e| format!("layer: {e}"))?;
            println!(
                "verified: {} pages, {} page bytes",
                entries.len(),
                entries.len() * PAGE_IMAGE_LEN
            );
        }
    }
    Ok(())
}

/// `zou inspect <target> chain <ref> <shard> <spc/db/rel/fork/block>
/// [at]`: what a read of one page would be built from, without
/// building it.
///
/// The redo worker dies on the page, not on the plan, so the question
/// after one of those failures is always the same: which image did
/// the base come from, how far back does that page's own lsn sit, and
/// which records is redo being asked to put on top. This answers it
/// off a store nobody is serving, and answers it at any lsn, so the
/// state a fold saw when it cut a bad image can be read back after
/// the fact. Reads nothing but layer ranges and takes no lease.
fn chain(
    target: &str,
    tenant_ref: &str,
    shard: &str,
    block: &str,
    rest: &[String],
) -> Result<(), String> {
    let shard: u16 = shard.parse().map_err(|_| USAGE.to_string())?;
    let parts: Vec<&str> = block.split('/').collect();
    let [spc, db, rel, fork, blk] = parts.as_slice() else {
        return Err(format!("{block} is not spc/db/rel/fork/block"));
    };
    let num = |s: &str| s.parse::<u32>().map_err(|_| format!("{s} is not a number"));
    let key = LayerKey::page(num(spc)?, num(db)?, num(rel)?, num(fork)? as u8, num(blk)?);

    let store = open_store(target)?;
    let layout = TenantLayout::new(tenant_ref);
    let (manifest, _) = PageShardManifest::load(&*store, &layout.shard_manifest(shard))
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("{tenant_ref} shard {shard} has no manifest"))?;
    let map = manifest.layer_map().map_err(|e| e.to_string())?;
    let at = match rest {
        [] => manifest.disk_consistent_lsn.0,
        [at] => parse_lsn(at)?,
        _ => return Err(USAGE.into()),
    };
    println!(
        "shard {shard} of {tenant_ref}, dcl {:#x}, {} layers in the map, reading at {at:#x}",
        manifest.disk_consistent_lsn.0,
        map.layers().len()
    );
    for desc in map.layers() {
        let kind = match desc.kind {
            LayerKind::Delta => "delta",
            LayerKind::Image => "image",
        };
        if desc.min_key <= key && key <= desc.max_key {
            println!(
                "  {kind} {:#x}..{:#x} covers the key, {} bytes",
                desc.min_lsn.0, desc.max_lsn.0, desc.size
            );
        }
    }

    let reader = LayerReader::for_shard(&*store, tenant_ref, shard);
    let recon = reader
        .reconstruct(&map, &Memtable::new(), &key, Lsn(at))
        .map_err(|e| e.to_string())?;
    match (&recon.base, recon.base_lsn) {
        (Some(page), Some(lsn)) => {
            let hi = u32::from_le_bytes(page[0..4].try_into().expect("a page header"));
            let lo = u32::from_le_bytes(page[4..8].try_into().expect("a page header"));
            let lower = u16::from_le_bytes([page[12], page[13]]);
            let upper = u16::from_le_bytes([page[14], page[15]]);
            println!(
                "base from the image at {:#x}, page lsn {:#x}, lower {lower} upper {upper}, max off {}",
                lsn.0,
                ((hi as u64) << 32) | lo as u64,
                lower.saturating_sub(24) / 4
            );
        }
        _ => println!("no base, the first record has to build the page"),
    }
    println!(
        "{} records, {} layers touched",
        recon.records.len(),
        recon.layers_touched
    );
    for (lsn, record) in &recon.records {
        // rmgr and info come out of the fixed record header, and the
        // refs say which blocks the record touches and which of them
        // it can build from nothing. A chain whose first record is
        // not an init for this block has nowhere to start.
        let (info, rmid) = (record[16], record[17]);
        let refs = match walscan::record_init_refs(record) {
            Ok(refs) => refs
                .iter()
                .map(|(r, init)| {
                    format!(
                        "{}/{}/{}.{} blk {}{}",
                        r.spc,
                        r.db,
                        r.rel,
                        r.fork,
                        r.blk,
                        if *init { " init" } else { "" }
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
            Err(e) => format!("refs unreadable: {e}"),
        };
        println!(
            "  {:#x} {} bytes, rmgr {rmid} info {info:#04x}, {refs}",
            lsn.0,
            record.len()
        );
    }
    Ok(())
}

/// An lsn as hex with or without the 0x, or as postgres writes it.
fn parse_lsn(s: &str) -> Result<u64, String> {
    if let Some((hi, lo)) = s.split_once('/') {
        let hi = u64::from_str_radix(hi, 16).map_err(|_| format!("{s} is not an lsn"))?;
        let lo = u64::from_str_radix(lo, 16).map_err(|_| format!("{s} is not an lsn"))?;
        return Ok((hi << 32) | lo);
    }
    let body = s.strip_prefix("0x").unwrap_or(s);
    u64::from_str_radix(body, 16).map_err(|_| format!("{s} is not an lsn"))
}
