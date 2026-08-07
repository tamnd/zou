//! An answer that is not text, described rather than quoted.
//!
//! Everything else here compares an answer byte for byte, and that
//! works because an answer is text: json, xml, a plain refusal. The
//! render routes are the first ones that answer with an image, and an
//! image cannot go into a recording as itself. It is not utf-8, it is
//! tens of kilobytes, and a recording is a file somebody reads.
//!
//! So a body that is not text is written down as what it was. Its
//! length and the sha256 of its bytes, which is a byte for byte
//! comparison in sixty four characters, and, when the bytes decode as
//! an image, the format and the size of that image.
//!
//! The digest is the honest part and the description is the useful
//! part. Two encoders never agree on the bytes of a lossy image, so a
//! render case names the digest volatile the way an auth case names a
//! token, and what is left being compared is that both servers
//! answered a webp of the same width and height. A case that does not
//! name it, a download of a file that was uploaded by the case above
//! it, still compares every byte.
//!
//! The sniffing is written out here rather than taken from an image
//! crate on purpose. The server will decode these with a library, and
//! a harness that read the header with the same library would agree
//! with the server about anything both of them got wrong. Four formats
//! and their headers are a hundred lines; a shared bug is worse.

use serde_json::json;
use sha2::{Digest, Sha256};

/// What a body that is not text is recorded as.
pub fn describe(bytes: &[u8]) -> serde_json::Value {
    let mut binary = json!({
        "bytes": bytes.len(),
        "sha256": hex(&Sha256::digest(bytes)),
    });
    if let Some(image) = image(bytes) {
        binary["image"] = image;
    }
    json!({ "binary": binary })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The format and the size, for the formats this surface deals in.
///
/// An image whose format is known and whose size is not is still
/// written down, with the format alone. Saying "avif" and nothing else
/// is a smaller lie than saying nothing, and the alternative is a
/// recording where a client asking for avif and a client asking for
/// json look the same.
fn image(bytes: &[u8]) -> Option<serde_json::Value> {
    let (format, size) = match bytes {
        [0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, ..] => ("png", png(bytes)),
        [0xff, 0xd8, 0xff, ..] => ("jpeg", jpeg(bytes)),
        [b'G', b'I', b'F', b'8', ..] => ("gif", gif(bytes)),
        _ if riff(bytes) => ("webp", webp(bytes)),
        _ if avif(bytes) => ("avif", None),
        _ => return None,
    };
    Some(match size {
        Some((width, height)) => json!({ "format": format, "width": width, "height": height }),
        None => json!({ "format": format }),
    })
}

/// IHDR is the first chunk and its two dimensions are the first eight
/// bytes of it, at a fixed offset the format guarantees.
fn png(bytes: &[u8]) -> Option<(u32, u32)> {
    match bytes.get(12..16)? {
        b"IHDR" => Some((be32(bytes, 16)?, be32(bytes, 20)?)),
        _ => None,
    }
}

/// The logical screen descriptor, which is the six bytes after the
/// signature and is little endian where every other format here is not.
fn gif(bytes: &[u8]) -> Option<(u32, u32)> {
    Some((le16(bytes, 6)? as u32, le16(bytes, 8)? as u32))
}

/// The size is in the frame header, which is one of ten markers rather
/// than one, and the markers are reached by walking the segments.
///
/// The four that are not a frame header, `DHT`, `DAC`, `DNL` and the
/// restart markers, are skipped by length like everything else. A
/// scan is the end of the walk: everything after it is entropy coded
/// and the length bytes stop meaning a length.
fn jpeg(bytes: &[u8]) -> Option<(u32, u32)> {
    let mut at = 2;
    while at + 3 < bytes.len() {
        if bytes[at] != 0xff {
            return None;
        }
        let marker = bytes[at + 1];
        // Padding, and the standalone markers that carry no length.
        if marker == 0xff || (0xd0..=0xd9).contains(&marker) || marker == 0x01 {
            at += 1;
            continue;
        }
        let length = be16(bytes, at + 2)? as usize;
        let frame = matches!(marker, 0xc0..=0xc3 | 0xc5..=0xc7 | 0xc9..=0xcb | 0xcd..=0xcf);
        if frame {
            return Some((be16(bytes, at + 7)? as u32, be16(bytes, at + 5)? as u32));
        }
        if marker == 0xda {
            return None;
        }
        at += 2 + length;
    }
    None
}

/// The signature, which is two four byte tags with the file's length
/// between them.
fn riff(bytes: &[u8]) -> bool {
    matches!(bytes.get(..4), Some(b"RIFF")) && matches!(bytes.get(8..12), Some(b"WEBP"))
}

/// Three containers under one signature, and the one that is used
/// depends on what the encoder decided rather than on what was asked
/// for, so all three are read.
fn webp(bytes: &[u8]) -> Option<(u32, u32)> {
    match bytes.get(12..16)? {
        // Lossy: the key frame header, past the three byte start code
        // and the three byte sync code, holds fourteen bits of each.
        b"VP8 " => {
            let at = 20;
            match bytes.get(at + 3..at + 6)? {
                [0x9d, 0x01, 0x2a] => Some((
                    (le16(bytes, at + 6)? & 0x3fff) as u32,
                    (le16(bytes, at + 8)? & 0x3fff) as u32,
                )),
                _ => None,
            }
        }
        // Lossless: fourteen bits of each, minus one, packed behind the
        // signature byte and read out of a little endian run of bits.
        b"VP8L" => {
            let bits = le32(bytes, 21)?;
            match bytes.get(20)? {
                0x2f => Some(((bits & 0x3fff) + 1, ((bits >> 14) & 0x3fff) + 1)),
                _ => None,
            }
        }
        // Extended: the canvas size, three bytes of each, minus one.
        b"VP8X" => Some((le24(bytes, 24)? + 1, le24(bytes, 27)? + 1)),
        _ => None,
    }
}

/// The brand in the file type box, which is the only part of an ISO
/// base media file this reads. The size is in an `ispe` box several
/// levels down, and a walk to it is a parser rather than a sniff.
fn avif(bytes: &[u8]) -> bool {
    matches!(bytes.get(4..8), Some(b"ftyp"))
        && matches!(bytes.get(8..12), Some(b"avif") | Some(b"avis"))
}

fn be32(bytes: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_be_bytes(bytes.get(at..at + 4)?.try_into().ok()?))
}

fn be16(bytes: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_be_bytes(bytes.get(at..at + 2)?.try_into().ok()?))
}

fn le32(bytes: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(bytes.get(at..at + 4)?.try_into().ok()?))
}

fn le24(bytes: &[u8], at: usize) -> Option<u32> {
    let three = bytes.get(at..at + 3)?;
    Some(three[0] as u32 | (three[1] as u32) << 8 | (three[2] as u32) << 16)
}

fn le16(bytes: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes(bytes.get(at..at + 2)?.try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn format(bytes: &[u8]) -> serde_json::Value {
        describe(bytes)["binary"]["image"].clone()
    }

    #[test]
    fn a_png_says_what_its_header_says() {
        let mut bytes = vec![0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a];
        bytes.extend_from_slice(&[0, 0, 0, 13]);
        bytes.extend_from_slice(b"IHDR");
        bytes.extend_from_slice(&100u32.to_be_bytes());
        bytes.extend_from_slice(&63u32.to_be_bytes());
        assert_eq!(
            format(&bytes),
            json!({"format": "png", "width": 100, "height": 63})
        );
    }

    /// The one format here that walks rather than reads a fixed offset,
    /// so it is given a segment to walk past first.
    #[test]
    fn a_jpeg_is_read_out_of_the_frame_header_it_reaches() {
        let mut bytes = vec![0xff, 0xd8];
        bytes.extend_from_slice(&[0xff, 0xe0, 0x00, 0x04, 0x00, 0x00]);
        bytes.extend_from_slice(&[0xff, 0xc0, 0x00, 0x11, 0x08]);
        bytes.extend_from_slice(&300u16.to_be_bytes());
        bytes.extend_from_slice(&200u16.to_be_bytes());
        assert_eq!(
            format(&bytes),
            json!({"format": "jpeg", "width": 200, "height": 300})
        );
    }

    #[test]
    fn a_lossy_webp_is_fourteen_bits_of_each() {
        let mut bytes = b"RIFF\0\0\0\0WEBPVP8 ".to_vec();
        bytes.extend_from_slice(&[0, 0, 0, 0]);
        bytes.extend_from_slice(&[0, 0, 0]);
        bytes.extend_from_slice(&[0x9d, 0x01, 0x2a]);
        bytes.extend_from_slice(&100u16.to_le_bytes());
        bytes.extend_from_slice(&63u16.to_le_bytes());
        assert_eq!(
            format(&bytes),
            json!({"format": "webp", "width": 100, "height": 63})
        );
    }

    #[test]
    fn a_lossless_webp_is_the_same_two_numbers_minus_one() {
        let mut bytes = b"RIFF\0\0\0\0WEBPVP8L".to_vec();
        bytes.extend_from_slice(&[0, 0, 0, 0]);
        bytes.push(0x2f);
        let packed: u32 = (100 - 1) | ((63 - 1) << 14);
        bytes.extend_from_slice(&packed.to_le_bytes());
        assert_eq!(
            format(&bytes),
            json!({"format": "webp", "width": 100, "height": 63})
        );
    }

    #[test]
    fn an_extended_webp_is_the_canvas_it_declares() {
        let mut bytes = b"RIFF\0\0\0\0WEBPVP8X".to_vec();
        bytes.extend_from_slice(&[0, 0, 0, 0]);
        bytes.extend_from_slice(&[0, 0, 0, 0]);
        bytes.extend_from_slice(&[99, 0, 0]);
        bytes.extend_from_slice(&[62, 0, 0]);
        assert_eq!(
            format(&bytes),
            json!({"format": "webp", "width": 100, "height": 63})
        );
    }

    #[test]
    fn a_gif_is_little_endian_where_the_rest_are_not() {
        let mut bytes = b"GIF89a".to_vec();
        bytes.extend_from_slice(&100u16.to_le_bytes());
        bytes.extend_from_slice(&63u16.to_le_bytes());
        assert_eq!(
            format(&bytes),
            json!({"format": "gif", "width": 100, "height": 63})
        );
    }

    /// Named without a size rather than left out, since what a client
    /// asked for and what it got is the thing being compared.
    #[test]
    fn an_avif_is_named_even_though_its_size_is_not_read() {
        let bytes = b"\0\0\0\x20ftypavifmore bytes".to_vec();
        assert_eq!(format(&bytes), json!({"format": "avif"}));
    }

    /// The part that is a comparison rather than a description.
    #[test]
    fn bytes_that_are_no_image_are_still_compared_by_their_digest() {
        let described = describe(&[0u8, 1, 2, 3]);
        assert_eq!(described["binary"]["bytes"], 4);
        assert_eq!(
            described["binary"]["sha256"],
            "054edec1d0211f624fed0cbca9d4f9400b0e491c43742af2c5b0abebf0c990d8"
        );
        assert_eq!(described["binary"]["image"], serde_json::Value::Null);
    }

    /// A header that says png over bytes that are not one is a
    /// difference worth seeing rather than an image to describe.
    #[test]
    fn a_truncated_header_is_not_given_a_size_it_does_not_have() {
        let bytes = [0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, 0, 0];
        assert_eq!(format(&bytes), json!({"format": "png"}));
    }
}
