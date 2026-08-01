//! Sealed segments come back off the store on every catch up and
//! archival read, so the decoder must never panic on any bytes, the
//! planning path must agree with the full decode, and the footer's
//! chunk ranges must stay inside the frame region on its word.

#![no_main]

use libfuzzer_sys::fuzz_target;
use zou_log::{decode_sealed, read_sealed_footer};

fuzz_target!(|data: &[u8]| {
    let shell = read_sealed_footer(data);
    if let Ok((header, frames, footer)) = decode_sealed(data) {
        let (shell_header, shell_footer) = shell.expect("full decode passed, shell must too");
        assert_eq!(shell_header, header);
        assert_eq!(shell_footer, footer);
        assert_eq!(frames.len(), footer.frame_count as usize);
        for t in &footer.tenants {
            assert!(footer.bloom.may_contain(t.tenant));
            for c in &t.chunks {
                assert!(c.offset + c.len <= data.len() as u64);
            }
        }
    }
});
