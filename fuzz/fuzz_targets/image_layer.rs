//! Image layers are the floor of every page read, so the decoder must
//! never panic on any bytes, the footer alone must agree with the full
//! decode, and each key must land in exactly one block that really
//! holds it.

#![no_main]

use libfuzzer_sys::fuzz_target;
use zou_store::layer::{
    PAGE_IMAGE_LEN, build_image, decode_image, decode_image_block, read_layer_footer,
};

fuzz_target!(|data: &[u8]| {
    let shell = read_layer_footer(data);
    if let Ok((entries, footer)) = decode_image(data) {
        let shell = shell.expect("full decode passed, the footer alone must too");
        assert_eq!(shell, footer);
        assert_eq!(entries.len() as u64, footer.entry_count);
        assert_eq!(footer.min_lsn, footer.max_lsn, "an image layer has one lsn");
        assert!(entries.windows(2).all(|w| w[0].key < w[1].key));
        for e in &entries {
            assert_eq!(e.page.len(), PAGE_IMAGE_LEN);
            assert!(footer.may_contain(&e.key), "bloom false negative");
            let run = footer.locate(&e.key);
            assert_eq!(run.len(), 1, "image keys live in exactly one block");
            let meta = &run[0];
            let range = &data[meta.offset as usize..(meta.offset + meta.len as u64) as usize];
            let block = decode_image_block(range, meta)
                .expect("full decode passed, every block must decode standalone");
            assert!(block.iter().any(|b| b.key == e.key));
        }
        let (rebuilt, _) =
            build_image(&entries, footer.min_lsn, 4 * PAGE_IMAGE_LEN).expect("must rebuild");
        let (back, _) = decode_image(&rebuilt).expect("rebuilt layer must decode");
        assert_eq!(back, entries);
    }
});
