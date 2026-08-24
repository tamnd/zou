//! Taking a package apart: gzip off the outside, tar off the inside,
//! and the files onto a disk.
//!
//! What npm serves for a version is a `.tgz`, and every one of them has
//! the same shape: no directory entries, no links, every file under a
//! single root called `package`, which npm drops when it unpacks. That
//! root is dropped here too, so what lands is the package as the
//! resolver wants to walk it, `package.json` at the top.
//!
//! The tar reading is written here rather than taken from a crate
//! because what a package needs is the reading and none of the rest of
//! tar: no permissions, no ownership, no times, no devices, and above
//! all no links. A tarball is a list of names a stranger chose, and the
//! interesting question about a name is not what it decompresses to but
//! where it lands. `package/../../.ssh/authorized_keys` is a legal tar
//! entry and an unpacker that writes it is the whole vulnerability, so
//! a name that leaves the directory it was given is refused here rather
//! than sanitised, and so is a link, which is the same escape wearing a
//! different hat.
//!
//! Nothing here is on the network. Fetching the tarball and checking it
//! against the digest the registry published is #596's next piece.

// Nothing calls this yet: the caller is the registry client that comes
// next. It is checked in ahead of that caller because it is the part
// worth being sure about on its own, and the fixtures are what makes
// sure of it.
#![allow(dead_code)]

use std::fs;
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};

use flate2::read::GzDecoder;

/// A tar block, which is the unit everything in the format is counted
/// in: a header is one, and a file is as many as its size rounds up to.
const BLOCK: usize = 512;

/// The largest a single file in a package may be. Nothing published is
/// anywhere near it, and the number exists because the size in a header
/// is a stranger's number and a reader that trusts it will happily be
/// told to allocate a terabyte.
const LARGEST: u64 = 512 * 1024 * 1024;

/// Unpacks a package tarball into a directory, dropping the root the
/// way npm drops it.
///
/// The reader is streamed rather than read to a `Vec` first, since the
/// caller's is a socket and a package is a size a stranger picked.
pub(crate) fn unpack(tgz: impl Read, into: &Path) -> io::Result<()> {
    walk(GzDecoder::new(tgz), into).map_err(|e| match e.kind() {
        // What the gzip reader says about a body that is not one is
        // "invalid gzip header", which is true and is not an answer to
        // the question the caller asked. A mirror serving a 404 page
        // for a tarball is the ordinary way to get here.
        io::ErrorKind::InvalidInput | io::ErrorKind::UnexpectedEof => corrupt(&e.to_string()),
        _ => e,
    })
}

/// The entries, one after another, onto the disk.
fn walk(mut tar: impl Read, into: &Path) -> io::Result<()> {
    let mut long: Option<String> = None;
    let mut extended: Option<String> = None;
    loop {
        let Some(header) = block(&mut tar)? else {
            return Ok(());
        };
        if header.iter().all(|&b| b == 0) {
            // The first of the two zero blocks that end an archive. What
            // is after them is padding, and nothing in it is ours.
            return Ok(());
        }
        let header = Header::read(&header)?;
        let size = header.size;
        match header.kind {
            // A long name for the entry after this one, in the body of
            // this one. GNU spells it one way and pax the other, and
            // both spellings turn up in the wild.
            b'L' => long = Some(name(&body(&mut tar, size)?)?),
            b'x' | b'g' => extended = path_from(&body(&mut tar, size)?)?.or(extended),
            _ => {
                let named = extended.take().or_else(|| long.take());
                let named = named.unwrap_or_else(|| header.name.clone());
                let Some(at) = inside(into, &named)? else {
                    skip(&mut tar, size)?;
                    continue;
                };
                match header.kind {
                    b'0' | b'\0' | b'7' => {
                        if let Some(parent) = at.parent() {
                            fs::create_dir_all(parent)?;
                        }
                        fs::write(&at, body(&mut tar, size)?)?;
                    }
                    b'5' => {
                        fs::create_dir_all(&at)?;
                        skip(&mut tar, size)?;
                    }
                    // A link is a name that means a second name, and a
                    // second name is the thing this reader refuses to
                    // let a stranger pick. Nothing npm publishes has
                    // one, so refusing costs nothing real.
                    b'1' | b'2' => return Err(refused(&named, "a link")),
                    other => return Err(refused(&named, &format!("a {} entry", other as char))),
                }
            }
        }
    }
}

/// What a header says, out of the fields any of this cares about.
#[derive(Debug)]
struct Header {
    name: String,
    size: u64,
    kind: u8,
}

impl Header {
    fn read(raw: &[u8; BLOCK]) -> io::Result<Self> {
        if !adds_up(raw) {
            return Err(corrupt("a header that does not add up"));
        }
        let own = name(&raw[0..100])?;
        let prefix = name(&raw[345..500])?;
        let named = match prefix.is_empty() {
            true => own,
            false => format!("{prefix}/{own}"),
        };
        let size = octal(&raw[124..136]).ok_or_else(|| corrupt("a size that is not a number"))?;
        if size > LARGEST {
            return Err(corrupt(&format!("a file of {size} bytes, which is absurd")));
        }
        Ok(Self {
            name: named,
            size,
            kind: raw[156],
        })
    }
}

/// Whether the checksum in a header is the one its bytes add up to,
/// which is the format's only way of saying this block is a header and
/// not the middle of somebody's file.
fn adds_up(raw: &[u8; BLOCK]) -> bool {
    let Some(said) = octal(&raw[148..156]) else {
        return false;
    };
    let sum: u64 = raw
        .iter()
        .enumerate()
        .map(|(at, &b)| match (148..156).contains(&at) {
            // The field reads as spaces while the sum over it is taken,
            // which is how the writer computed it.
            true => u64::from(b' '),
            false => u64::from(b),
        })
        .sum();
    sum == said
}

/// The next block, or nothing at the end of the stream.
fn block(from: &mut impl Read) -> io::Result<Option<[u8; BLOCK]>> {
    let mut buf = [0u8; BLOCK];
    let mut filled = 0;
    while filled < BLOCK {
        match from.read(&mut buf[filled..])? {
            0 if filled == 0 => return Ok(None),
            0 => return Err(corrupt("an archive that stops in the middle of a block")),
            read => filled += read,
        }
    }
    Ok(Some(buf))
}

/// The bytes of an entry, with the padding to the end of its last block
/// read and thrown away.
fn body(from: &mut impl Read, size: u64) -> io::Result<Vec<u8>> {
    let mut body = vec![0u8; size as usize];
    from.read_exact(&mut body)
        .map_err(|_| corrupt("an archive that stops in the middle of a file"))?;
    padding(from, size)?;
    Ok(body)
}

/// An entry nobody wants the contents of, read past.
fn skip(from: &mut impl Read, size: u64) -> io::Result<()> {
    io::copy(&mut from.take(size), &mut io::sink())?;
    padding(from, size)
}

/// The rest of the last block of an entry.
fn padding(from: &mut impl Read, size: u64) -> io::Result<()> {
    let over = size % BLOCK as u64;
    match over {
        0 => Ok(()),
        over => io::copy(&mut from.take(BLOCK as u64 - over), &mut io::sink()).map(|_| ()),
    }
}

/// A field as text, up to the first NUL.
fn name(raw: &[u8]) -> io::Result<String> {
    let end = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
    String::from_utf8(raw[..end].to_vec())
        .map(|it| it.trim_end_matches('/').to_string())
        .map_err(|_| corrupt("a name that is not text"))
}

/// A numeric field, which tar writes in octal and pads with whatever it
/// felt like: NULs, spaces, or nothing at all.
fn octal(raw: &[u8]) -> Option<u64> {
    let text = String::from_utf8_lossy(raw);
    let text = text.trim_matches(|c: char| c == '\0' || c == ' ');
    match text.is_empty() {
        true => Some(0),
        false => u64::from_str_radix(text, 8).ok(),
    }
}

/// The `path` a pax extended header names, if it names one.
///
/// The body is a list of `length key=value\n` records, and the length
/// counts itself, which is the sort of thing a format does when it was
/// designed by a committee that had already agreed on everything else.
fn path_from(body: &[u8]) -> io::Result<Option<String>> {
    let mut rest = body;
    while !rest.is_empty() {
        let space = rest.iter().position(|&b| b == b' ');
        let Some(space) = space else { return Ok(None) };
        let Some(len) = octal_free(&rest[..space]) else {
            return Ok(None);
        };
        if len > rest.len() || len <= space {
            return Ok(None);
        }
        let record = &rest[space + 1..len - 1];
        if let Some(value) = record.strip_prefix(b"path=") {
            return String::from_utf8(value.to_vec())
                .map(|it| Some(it.trim_end_matches('/').to_string()))
                .map_err(|_| corrupt("a pax path that is not text"));
        }
        rest = &rest[len..];
    }
    Ok(None)
}

/// The decimal length in front of a pax record.
fn octal_free(raw: &[u8]) -> Option<usize> {
    String::from_utf8_lossy(raw).parse().ok()
}

/// Where an entry lands, or nothing for the root itself.
///
/// The first component is dropped, which is npm's `package/`, and what
/// is left has to be a plain relative walk downwards. A name that is
/// absolute, that has a `..` in it, or that is a windows drive is not
/// sanitised into something safe: it is refused, because a tarball
/// containing one is not a package with a mistake in it.
fn inside(into: &Path, named: &str) -> io::Result<Option<PathBuf>> {
    let named = named.replace('\\', "/");
    if named.starts_with('/') || named.contains(':') {
        return Err(refused(&named, "a name that is not relative"));
    }
    let mut walk = PathBuf::new();
    for part in Path::new(&named).components() {
        match part {
            Component::Normal(part) => walk.push(part),
            Component::CurDir => {}
            _ => return Err(refused(&named, "a name that climbs out of the directory")),
        }
    }
    let mut parts = walk.components();
    // The root, `package`, which is what npm drops.
    parts.next();
    let under: PathBuf = parts.collect();
    Ok(match under.as_os_str().is_empty() {
        true => None,
        false => Some(into.join(under)),
    })
}

fn refused(named: &str, why: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("{named}: {why}, which a package may not have"),
    )
}

fn corrupt(what: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("this is not a package tarball: {what}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The fixtures are real archives written by real tar, since the
    /// point of the exercise is reading what other people write rather
    /// than reading back what this file wrote. `ustar` is the shape npm
    /// publishes; `pax` and `gnu` are the two ways a path too long for
    /// a header is carried, and both turn up in packages with deep
    /// directories. The refused ones were written with python's tarfile,
    /// because no ordinary tar will write a name like that for you.
    ///
    /// Four real packages were unpacked too, is-number, ms,
    /// @supabase/supabase-js and the mcp sdk, and their file lists
    /// compared against what `tar tf` says is in them. That check is
    /// not here because it wants four tarballs off the network, and it
    /// is written down because a reader wondering whether this was
    /// only ever tried on archives written for it deserves an answer.
    fn fixture(named: &str) -> Vec<u8> {
        let at = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(named);
        fs::read(&at).unwrap_or_else(|e| panic!("{}: {e}", at.display()))
    }

    fn unpacked(named: &str) -> (tempfile::TempDir, io::Result<()>) {
        let into = tempfile::tempdir().expect("a temporary directory");
        let done = unpack(fixture(named).as_slice(), into.path());
        (into, done)
    }

    fn read(at: &Path, named: &str) -> String {
        fs::read_to_string(at.join(named)).unwrap_or_else(|e| panic!("{named}: {e}"))
    }

    #[test]
    fn a_package_arrives_with_its_root_taken_off() {
        let (into, done) = unpacked("tarball-ustar.tgz");
        done.expect("an ordinary package unpacks");
        assert!(
            into.path().join("package.json").exists(),
            "the manifest is at the top"
        );
        assert_eq!(read(into.path(), "index.js"), "export const one = 1;\n");
        assert_eq!(
            read(into.path(), "lib/deep/deeper.js"),
            "export const two = 2;\n"
        );
        assert!(
            !into.path().join("package").exists(),
            "and the root itself is not a directory"
        );
    }

    #[test]
    fn a_path_too_long_for_a_header_is_still_the_path() {
        for named in ["tarball-pax.tgz", "tarball-gnu.tgz"] {
            let (into, done) = unpacked(named);
            done.unwrap_or_else(|e| panic!("{named}: {e}"));
            let long = "lib/a-directory-with-a-name-long-enough-to-not-fit-in-a-header/and-another-one-underneath-it-just-to-be-sure/deeper.js";
            assert_eq!(
                read(into.path(), long),
                "export const three = 3;\n",
                "{named}"
            );
            assert_eq!(
                read(into.path(), "index.js"),
                "export const one = 1;\n",
                "{named}"
            );
        }
    }

    #[test]
    fn a_name_that_climbs_out_of_the_directory_is_refused() {
        let (into, done) = unpacked("tarball-climbing.tgz");
        let said = done.expect_err("a package may not name a parent directory");
        assert!(said.to_string().contains("climbs out"), "{said}");
        assert!(
            !into
                .path()
                .parent()
                .is_none_or(|it| it.join("escaped.js").exists())
        );
    }

    #[test]
    fn a_name_that_is_not_relative_is_refused() {
        let (_into, done) = unpacked("tarball-absolute.tgz");
        let said = done.expect_err("a package may not name an absolute path");
        assert!(said.to_string().contains("not relative"), "{said}");
    }

    #[test]
    fn a_link_is_refused_whichever_kind_it_is() {
        let (_into, done) = unpacked("tarball-symlink.tgz");
        let said = done.expect_err("a package may not carry a link");
        assert!(said.to_string().contains("a link"), "{said}");
    }

    #[test]
    fn an_archive_that_stops_early_is_not_a_package_that_arrived() {
        let whole = fixture("tarball-ustar.tgz");
        let half = &whole[..whole.len() / 2];
        let into = tempfile::tempdir().expect("a temporary directory");
        let said = unpack(half, into.path()).expect_err("half an archive is not an archive");
        assert_eq!(said.kind(), io::ErrorKind::InvalidData, "{said}");
    }

    #[test]
    fn something_that_is_not_a_tarball_at_all_says_so() {
        let into = tempfile::tempdir().expect("a temporary directory");
        let said = unpack(&b"<html>404 not found</html>"[..], into.path())
            .expect_err("a page from a mirror is not a package");
        assert!(
            said.to_string().contains("not a package tarball")
                || said.kind() == io::ErrorKind::InvalidData,
            "{said}"
        );
    }

    #[test]
    fn a_header_the_bytes_do_not_agree_with_is_not_read() {
        let mut raw = [0u8; BLOCK];
        raw[..8].copy_from_slice(b"index.js");
        raw[124..135].copy_from_slice(b"00000000000");
        raw[148..156].copy_from_slice(b"999999 \0");
        let said = Header::read(&raw).expect_err("a checksum that is wrong is a refusal");
        assert!(said.to_string().contains("does not add up"), "{said}");
    }

    #[test]
    fn a_numeric_field_is_read_however_tar_felt_like_padding_it() {
        assert_eq!(octal(b"00000001750\0"), Some(1000));
        assert_eq!(octal(b"0000750 "), Some(488));
        assert_eq!(octal(b"\0\0\0\0"), Some(0));
        assert_eq!(octal(b"not a number"), None);
    }

    #[test]
    fn the_path_a_pax_header_carries_is_the_one_it_names() {
        let body = b"27 mtime=1700000000.000000\n25 path=package/index.js\n";
        assert_eq!(
            path_from(body).expect("a body that parses"),
            Some("package/index.js".to_string())
        );
        assert_eq!(
            path_from(b"27 mtime=1700000000.000000\n").expect("no path in it"),
            None
        );
    }
}
