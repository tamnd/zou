//! Landing segments come back off the store during takeover, recovery
//! and consolidation, so the decoder must never panic on any bytes,
//! and the planning path must agree with the full decode: whenever
//! both succeed, the footer is the same one.

#![no_main]

use libfuzzer_sys::fuzz_target;
use zou_log::{decode_segment, read_footer};

fuzz_target!(|data: &[u8]| {
    let shell = read_footer(data);
    if let Ok((header, frames, footer)) = decode_segment(data) {
        let (shell_header, shell_footer) = shell.expect("full decode passed, shell must too");
        assert_eq!(shell_header, header);
        assert_eq!(shell_footer, footer);
        assert_eq!(frames.len(), footer.frame_count as usize);
    }
});
