//! The v2 frame decoder consumes bytes straight off the network, so it
//! must never panic on any input, and anything it does accept must
//! survive a round trip: re-encoding a decoded frame and decoding that
//! again has to give the same frame back.

#![no_main]

use libfuzzer_sys::fuzz_target;
use zou_store::{Frame2, Frame2Stream};

fuzz_target!(|data: &[u8]| {
    if let Ok((frame, consumed)) = Frame2::decode(data) {
        assert!(consumed <= data.len());
        let wire = frame.encode();
        let (again, _) = Frame2::decode(&wire).expect("re-encoded frame must decode");
        assert_eq!(again, frame);
    }
    for _ in Frame2Stream::new(data) {}
});
