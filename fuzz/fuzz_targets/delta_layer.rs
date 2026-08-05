//! Delta layers come back off the store on every page reconstruction,
//! so the decoder must never panic on any bytes, the footer alone must
//! agree with the full decode, and everything the footer claims about
//! blocks, keys, and the bloom must hold on its word.

#![no_main]

use libfuzzer_sys::fuzz_target;
use zou_store::layer::{build_delta, decode_delta, decode_delta_block, read_layer_footer};

fuzz_target!(|data: &[u8]| {
    let shell = read_layer_footer(data);
    if let Ok((entries, footer)) = decode_delta(data) {
        let shell = shell.expect("full decode passed, the footer alone must too");
        assert_eq!(shell, footer);
        assert_eq!(entries.len() as u64, footer.entry_count);
        assert!(entries
            .windows(2)
            .all(|w| (w[0].key, w[0].lsn) < (w[1].key, w[1].lsn)));
        for e in &entries {
            assert!(footer.may_contain(&e.key), "bloom false negative");
            let run = footer.locate(&e.key);
            assert!(!run.is_empty(), "present key must locate");
        }
        for meta in &footer.blocks {
            let range = &data[meta.offset as usize..(meta.offset + meta.len as u64) as usize];
            let block = decode_delta_block(range, meta)
                .expect("full decode passed, every block must decode standalone");
            assert_eq!(block.len() as u32, meta.entries);
        }
        // The format round trips: rebuilding from the decoded entries
        // must decode back to the same entries. Bytes can differ, the
        // original may have used another block target.
        let (rebuilt, _) = build_delta(&entries, 4096).expect("decoded entries must rebuild");
        let (back, _) = decode_delta(&rebuilt).expect("rebuilt layer must decode");
        assert_eq!(back, entries);
    }
});
