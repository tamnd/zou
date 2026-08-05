//! `zou inspect <file>` or `zou inspect <target> <key>`: decode one
//! layer object and print what the footer claims, then prove it by
//! fully decoding every block.
//!
//! This is the standalone decoder the format discipline demands: it
//! shares the codec with the server but nothing else, takes no lease,
//! and writes nothing, so it is safe to point at a live store or at a
//! file copied out of one.

use zou_store::layer::{LayerKind, PAGE_IMAGE_LEN, decode_delta, decode_image, read_layer_footer};
use zou_store::{CasStore, open_store};

pub const USAGE: &str = "usage: zou inspect <file | target key>";

pub fn run(argv: &[String]) -> Result<(), String> {
    let bytes = match argv {
        [file] => std::fs::read(file).map_err(|e| format!("{file}: {e}"))?,
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
