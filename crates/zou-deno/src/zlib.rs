//! The compression `node:zlib` is a name in front of.
//!
//! A package reaching for zlib is almost always decompressing a body it
//! fetched itself, or gzipping one it is about to send, and both of
//! those are deflate with a different eight or ten bytes around them.
//! The deflate is already linked into this binary, because a package
//! from the registry arrives as a gzipped tarball and something has to
//! open it, so what is here is not a new dependency and not a second
//! implementation. It is that one, with an id javascript can hold.
//!
//! The state is here rather than in the shim because a stream has to
//! keep going: a transform handed four kilobytes at a time cannot
//! compress each of them on its own and call the concatenation a gzip.
//! So a job is opened, written to as many times as the caller likes,
//! and ended, and the one shot calls in the shim are the same three
//! ops with nothing in between them. What comes back from a write is
//! whatever the encoder has finished with by then, which is usually
//! nothing until the window fills, and the rest arrives at the end.
//!
//! Brotli is not here. It is a different algorithm and a different
//! library rather than another header on this one, and `node:zlib`
//! refuses it by name instead of pretending.

use std::collections::HashMap;
use std::io::Write;

use deno_core::{OpState, op2};
use deno_error::JsErrorBox;
use flate2::Compression;
use flate2::write::{
    DeflateDecoder, DeflateEncoder, GzDecoder, GzEncoder, ZlibDecoder, ZlibEncoder,
};

/// How many compression jobs one isolate may hold at once. A stream
/// that is never ended is a job that is never dropped, and this is the
/// same reasoning the socket table's limit has: fail on the box the
/// leak is on rather than take its memory a window at a time.
const OPEN: usize = 256;

/// One compression or decompression in progress, in whichever of the
/// six shapes it was opened as.
///
/// The writer types are the incremental ones: bytes go in with
/// `write_all` and whatever has been finished comes out of the `Vec`
/// underneath, which is drained by every op that looks at it.
enum Job {
    Gzip(GzEncoder<Vec<u8>>),
    Deflate(ZlibEncoder<Vec<u8>>),
    DeflateRaw(DeflateEncoder<Vec<u8>>),
    Gunzip(GzDecoder<Vec<u8>>),
    Inflate(ZlibDecoder<Vec<u8>>),
    InflateRaw(DeflateDecoder<Vec<u8>>),
    /// `unzip`, before enough bytes have arrived to say which of the
    /// two it is. Node's own `unzip` reads the first two bytes and
    /// picks gzip or zlib off them, and so does this.
    Waiting(Vec<u8>),
}

/// Every compression this isolate has open, by the id javascript holds.
#[derive(Default)]
pub struct Jobs {
    last: u32,
    open: HashMap<u32, Job>,
}

impl Job {
    /// The job a name opens, or nothing if this runtime has no such
    /// thing. The names are the shim's and not node's, because node's
    /// are a class name in one place and a function name in another.
    fn named(name: &str, level: Compression) -> Option<Job> {
        Some(match name {
            "gzip" => Job::Gzip(GzEncoder::new(Vec::new(), level)),
            "deflate" => Job::Deflate(ZlibEncoder::new(Vec::new(), level)),
            "deflateRaw" => Job::DeflateRaw(DeflateEncoder::new(Vec::new(), level)),
            "gunzip" => Job::Gunzip(GzDecoder::new(Vec::new())),
            "inflate" => Job::Inflate(ZlibDecoder::new(Vec::new())),
            "inflateRaw" => Job::InflateRaw(DeflateDecoder::new(Vec::new())),
            "unzip" => Job::Waiting(Vec::new()),
            _ => return None,
        })
    }

    fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        match self {
            Job::Gzip(it) => it.write_all(bytes),
            Job::Deflate(it) => it.write_all(bytes),
            Job::DeflateRaw(it) => it.write_all(bytes),
            Job::Gunzip(it) => it.write_all(bytes),
            Job::Inflate(it) => it.write_all(bytes),
            Job::InflateRaw(it) => it.write_all(bytes),
            Job::Waiting(held) => {
                held.extend_from_slice(bytes);
                Ok(())
            }
        }
    }

    /// Whatever is finished, taken out of the buffer underneath so the
    /// same bytes do not come back on the next call.
    fn drained(&mut self) -> Vec<u8> {
        let made = match self {
            Job::Gzip(it) => it.get_mut(),
            Job::Deflate(it) => it.get_mut(),
            Job::DeflateRaw(it) => it.get_mut(),
            Job::Gunzip(it) => it.get_mut(),
            Job::Inflate(it) => it.get_mut(),
            Job::InflateRaw(it) => it.get_mut(),
            Job::Waiting(_) => return Vec::new(),
        };
        std::mem::take(made)
    }

    fn finish(self) -> std::io::Result<Vec<u8>> {
        match self {
            Job::Gzip(it) => it.finish(),
            Job::Deflate(it) => it.finish(),
            Job::DeflateRaw(it) => it.finish(),
            Job::Gunzip(it) => it.finish(),
            Job::Inflate(it) => it.finish(),
            Job::InflateRaw(it) => it.finish(),
            // Nothing ever arrived, or one byte did, which is not
            // enough of a header to be either format. An empty input
            // decompresses to nothing rather than to a complaint,
            // which is what node answers for it too.
            Job::Waiting(held) => match held.is_empty() {
                true => Ok(Vec::new()),
                false => Err(std::io::Error::other("incorrect header check")),
            },
        }
    }
}

/// The two bytes at the front of a gzip member, which is what tells it
/// apart from a zlib stream: no zlib header starts `1f 8b`, because the
/// low nibble of its first byte is the compression method and 8 is the
/// only one there is.
fn sniff(held: &[u8]) -> Option<Job> {
    match held.len() >= 2 {
        false => None,
        true => {
            let mut job = match held[0] == 0x1f && held[1] == 0x8b {
                true => Job::Gunzip(GzDecoder::new(Vec::new())),
                false => Job::Inflate(ZlibDecoder::new(Vec::new())),
            };
            // The held bytes have not been decompressed yet, so they go
            // through the job that was just chosen for them. A failure
            // here is a failure of the next write, which is where the
            // caller is standing.
            let _ = job.write(held);
            Some(job)
        }
    }
}

/// A job, and the id the shim holds it by.
#[op2(fast)]
#[smi]
pub fn op_zou_zlib_open(
    state: &mut OpState,
    #[string] kind: &str,
    #[smi] level: i32,
) -> Result<u32, JsErrorBox> {
    // Node's -1 is "whatever the library thinks", and every other
    // number outside the range is the caller's mistake rather than
    // something to round into place.
    let level = match level {
        -1 => Compression::default(),
        0..=9 => Compression::new(level as u32),
        _ => {
            return Err(JsErrorBox::type_error(format!(
                "{level} is not a compression level"
            )));
        }
    };
    let job = Job::named(kind, level).ok_or_else(|| {
        JsErrorBox::type_error(format!("{kind} is not a compression this runtime has"))
    })?;
    let jobs = state.borrow_mut::<Jobs>();
    if jobs.open.len() >= OPEN {
        return Err(JsErrorBox::type_error(format!(
            "this isolate has {OPEN} compressions open at once, which is all it may have"
        )));
    }
    jobs.last += 1;
    let id = jobs.last;
    jobs.open.insert(id, job);
    Ok(id)
}

/// Bytes in, and whatever came out on the way.
#[op2]
#[buffer]
pub fn op_zou_zlib_write(
    state: &mut OpState,
    #[smi] id: u32,
    #[buffer] bytes: &[u8],
) -> Result<Vec<u8>, JsErrorBox> {
    let jobs = state.borrow_mut::<Jobs>();
    let job = jobs.open.get_mut(&id).ok_or_else(gone)?;
    if let Job::Waiting(held) = job {
        let mut held = std::mem::take(held);
        held.extend_from_slice(bytes);
        match sniff(&held) {
            Some(chosen) => *job = chosen,
            None => {
                *job = Job::Waiting(held);
                return Ok(Vec::new());
            }
        }
    } else {
        job.write(bytes).map_err(|e| failed(&e))?;
    }
    Ok(job.drained())
}

/// The end of the input, which is where a gzip's trailer is written and
/// where a truncated one is noticed. The job is gone either way.
#[op2]
#[buffer]
pub fn op_zou_zlib_end(state: &mut OpState, #[smi] id: u32) -> Result<Vec<u8>, JsErrorBox> {
    let jobs = state.borrow_mut::<Jobs>();
    let job = jobs.open.remove(&id).ok_or_else(gone)?;
    job.finish().map_err(|e| failed(&e))
}

/// A stream that was destroyed rather than ended, which is a job to
/// drop and nothing to say about it.
#[op2(fast)]
pub fn op_zou_zlib_drop(state: &mut OpState, #[smi] id: u32) {
    state.borrow_mut::<Jobs>().open.remove(&id);
}

fn gone() -> JsErrorBox {
    JsErrorBox::type_error("this compression has already been ended")
}

/// What the library says went wrong, which for a corrupt input is a
/// sentence like node's own because both of them come from zlib.
fn failed(why: &std::io::Error) -> JsErrorBox {
    JsErrorBox::type_error(why.to_string())
}
