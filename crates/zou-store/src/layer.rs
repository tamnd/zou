//! Delta and image layer codecs (spec 04 section 2).
//!
//! The layer store is an LSM over the object store, two immutable
//! object kinds per tenant shard. A delta layer holds WAL records
//! sorted by (key, lsn), covering a key range by an lsn range. An
//! image layer holds full page images at one lsn, sorted by key. Both
//! share one file shape:
//!
//! ```text
//! header  magic, version, kind
//! blocks  sorted entries, compressed per block, 256 KB target
//! footer  key range, lsn range, entry count, one index row per block
//!         (key bounds, byte range, sizes, crc), bloom over keys
//! tail    footer length, crc, footer magic
//! ```
//!
//! The footer is the whole read plan: one range GET fetches it, the
//! sparse index names the single block that can hold a key, the bloom
//! says whether to bother, and the block's own crc verifies the second
//! range GET without touching the rest of the object. Blocks are
//! contiguous and the decoder enforces it, so a footer that lies about
//! byte ranges fails decode instead of misreading.
//!
//! Blocks compress with lz4. The spec says zstd at rest; lz4 is the
//! same deliberate deviation the sealed segment codec documents, one
//! pure Rust compressor end to end, and the ratio loss on 8 KB pages
//! is a price worth not carrying a C toolchain for. The block target
//! bounds what a point read decompresses, not what it fetches: fetch
//! length is the compressed length in the index row.
//!
//! Integrity is layered like every zou codec: the footer crc covers
//! the header and footer body, each index row carries the crc of its
//! block bytes, and the decoder never panics and never allocates on a
//! lying length.

use crate::bloom::Bloom;
use crate::lsn::Lsn;
use crate::stats::{Packed, note_packed};

pub const LAYER_MAGIC: [u8; 4] = *b"ZLYR";
pub const LAYER_FOOTER_MAGIC: [u8; 4] = *b"ZLYF";
pub const LAYER_VERSION: u16 = 1;

/// Uncompressed bytes a block aims for. A single oversized entry gets
/// its own block, so this is a target, not a limit.
pub const LAYER_BLOCK_TARGET: usize = 256 * 1024;

/// Full page images are exactly this long, the Postgres block size.
pub const PAGE_IMAGE_LEN: usize = 8192;

/// Cap on one delta record, matching the frame payload cap upstream.
pub const MAX_RECORD_LEN: u32 = 64 * 1024 * 1024;

/// Cap on one block's uncompressed length: the target plus one maximal
/// record and its entry framing. A footer claiming more is lying.
const MAX_BLOCK_RAW_LEN: u32 = LAYER_BLOCK_TARGET as u32 + MAX_RECORD_LEN + ENTRY_FIXED_LEN as u32;

/// magic, version, kind, reserved. A range reader never fetches these,
/// [`read_layer_footer_suffix`] names them from the layer's kind and
/// lets the footer crc prove the object agrees.
pub const LAYER_HEADER_LEN: usize = 4 + 2 + 1 + 1;
const HEADER_LEN: usize = LAYER_HEADER_LEN;
/// footer body length, crc, footer magic.
const TAIL_LEN: usize = 4 + 4 + 4;
/// first key, last key, offset, compressed len, raw len, entry count,
/// block crc, encoding byte.
const BLOCK_META_LEN: usize = KEY_ENCODED_LEN * 2 + 8 + 4 + 4 + 4 + 4 + 1;
/// key, lsn, record length prefix of one delta entry.
const ENTRY_FIXED_LEN: usize = KEY_ENCODED_LEN + 8 + 4;

const ENC_RAW: u8 = 0;
const ENC_LZ4: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayerKind {
    Delta,
    Image,
}

/// Key kind for relation pages, [`LayerKey::page`].
pub const KEY_PAGE: u8 = 0;
/// Key kind for relation fork sizes, [`LayerKey::relsize`].
pub const KEY_RELSIZE: u8 = 1;

pub const KEY_ENCODED_LEN: usize = 1 + 4 + 4 + 4 + 1 + 4;

/// One addressable thing in the page service keyspace: a relation page
/// or a non page key like a fork size. The derive order below is the
/// total order of the keyspace and the encoding is these fields little
/// endian in the same order, both format frozen: layers persist
/// encoded keys and name themselves by key ranges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LayerKey {
    pub kind: u8,
    pub spc: u32,
    pub db: u32,
    pub rel: u32,
    pub fork: u8,
    pub block: u32,
}

impl LayerKey {
    pub fn page(spc: u32, db: u32, rel: u32, fork: u8, block: u32) -> Self {
        Self {
            kind: KEY_PAGE,
            spc,
            db,
            rel,
            fork,
            block,
        }
    }

    /// The block count of one relation fork at some lsn. Sizes ride in
    /// delta layers as records so reads can answer nblocks from the
    /// same reconstruction that answers pages.
    pub fn relsize(spc: u32, db: u32, rel: u32, fork: u8) -> Self {
        Self {
            kind: KEY_RELSIZE,
            spc,
            db,
            rel,
            fork,
            block: 0,
        }
    }

    pub fn encode(&self) -> [u8; KEY_ENCODED_LEN] {
        let mut out = [0u8; KEY_ENCODED_LEN];
        out[0] = self.kind;
        out[1..5].copy_from_slice(&self.spc.to_le_bytes());
        out[5..9].copy_from_slice(&self.db.to_le_bytes());
        out[9..13].copy_from_slice(&self.rel.to_le_bytes());
        out[13] = self.fork;
        out[14..18].copy_from_slice(&self.block.to_le_bytes());
        out
    }

    pub fn decode(buf: &[u8; KEY_ENCODED_LEN]) -> Self {
        Self {
            kind: buf[0],
            spc: u32::from_le_bytes(buf[1..5].try_into().unwrap()),
            db: u32::from_le_bytes(buf[5..9].try_into().unwrap()),
            rel: u32::from_le_bytes(buf[9..13].try_into().unwrap()),
            fork: buf[13],
            block: u32::from_le_bytes(buf[14..18].try_into().unwrap()),
        }
    }

    /// Fixed width hex form for layer object names, sorts like the key.
    pub fn hex(&self) -> String {
        format!(
            "{:02x}{:08x}{:08x}{:08x}{:02x}{:08x}",
            self.kind, self.spc, self.db, self.rel, self.fork, self.block
        )
    }
}

/// One WAL record addressed to one key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaEntry {
    pub key: LayerKey,
    pub lsn: Lsn,
    pub record: Vec<u8>,
}

/// One full page image. The page is exactly [`PAGE_IMAGE_LEN`] bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageEntry {
    pub key: LayerKey,
    pub page: Vec<u8>,
}

/// One index row: everything needed to fetch and verify one block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LayerBlock {
    pub first_key: LayerKey,
    pub last_key: LayerKey,
    /// Byte range of the compressed block in the object.
    pub offset: u64,
    pub len: u32,
    /// Uncompressed length, what the block decodes to.
    pub raw_len: u32,
    pub entries: u32,
    /// crc32c of the block's bytes as stored, so a range GET verifies
    /// against the footer without any other part of the object.
    pub crc: u32,
    enc: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayerFooter {
    pub kind: LayerKind,
    pub min_key: LayerKey,
    pub max_key: LayerKey,
    /// For an image layer both bounds are the layer's one lsn.
    pub min_lsn: Lsn,
    pub max_lsn: Lsn,
    pub entry_count: u64,
    /// In file order, contiguous, key ranges strictly ascending.
    pub blocks: Vec<LayerBlock>,
    pub bloom: Bloom,
}

impl LayerFooter {
    /// False positives are possible, false negatives are not.
    pub fn may_contain(&self, key: &LayerKey) -> bool {
        self.bloom.may_contain(&key.encode())
    }

    /// The contiguous run of blocks whose key ranges cover `key`, empty
    /// when the key falls outside every block. Plans the second range
    /// GET from the footer alone. In an image layer a key lives in at
    /// most one block; in a delta layer one key's records can spill
    /// across neighbors, so this is a slice.
    pub fn locate(&self, key: &LayerKey) -> &[LayerBlock] {
        let i = self.blocks.partition_point(|b| b.last_key < *key);
        let n = self.blocks[i..]
            .iter()
            .take_while(|b| b.first_key <= *key)
            .count();
        &self.blocks[i..i + n]
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LayerBuildError {
    #[error("a layer needs at least one entry")]
    Empty,
    #[error("entries are not strictly sorted")]
    Unsorted,
    #[error("a page image must be exactly {PAGE_IMAGE_LEN} bytes, got {len}")]
    BadPageLen { len: usize },
    #[error("a delta record is over the {MAX_RECORD_LEN} byte cap")]
    RecordTooLarge,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LayerDecodeError {
    #[error("truncated layer: have {have} bytes, need {need}")]
    Truncated { have: usize, need: usize },
    #[error("not a layer object")]
    BadMagic,
    #[error("layer version {found} is newer than this zou, upgrade")]
    UnsupportedVersion { found: u16 },
    #[error("expected a {expected:?} layer, this is a {found:?} layer")]
    WrongKind {
        expected: LayerKind,
        found: LayerKind,
    },
    #[error("layer crc mismatch, the object is corrupt")]
    Corrupt,
    #[error("block {index} is bad")]
    Block { index: usize },
}

fn push_block(
    buf: &mut Vec<u8>,
    blocks: &mut Vec<LayerBlock>,
    raw: &[u8],
    first_key: LayerKey,
    last_key: LayerKey,
    entries: u32,
) {
    let compressed = lz4_flex::compress(raw);
    let (enc, stored): (u8, &[u8]) = if compressed.len() < raw.len() {
        (ENC_LZ4, &compressed)
    } else {
        (ENC_RAW, raw)
    };
    note_packed(Packed::Layer, raw.len(), stored.len());
    blocks.push(LayerBlock {
        first_key,
        last_key,
        offset: buf.len() as u64,
        len: stored.len() as u32,
        raw_len: raw.len() as u32,
        entries,
        crc: crc32c::crc32c(stored),
        enc,
    });
    buf.extend_from_slice(stored);
}

fn finish(
    mut buf: Vec<u8>,
    kind: LayerKind,
    blocks: Vec<LayerBlock>,
    bloom: Bloom,
    min_lsn: Lsn,
    max_lsn: Lsn,
    entry_count: u64,
) -> (Vec<u8>, LayerFooter) {
    let footer = LayerFooter {
        kind,
        min_key: blocks.first().expect("layers are never empty").first_key,
        max_key: blocks.last().expect("layers are never empty").last_key,
        min_lsn,
        max_lsn,
        entry_count,
        blocks,
        bloom,
    };
    let footer_start = buf.len();
    buf.extend_from_slice(&footer.min_key.encode());
    buf.extend_from_slice(&footer.max_key.encode());
    buf.extend_from_slice(&footer.min_lsn.0.to_le_bytes());
    buf.extend_from_slice(&footer.max_lsn.0.to_le_bytes());
    buf.extend_from_slice(&footer.entry_count.to_le_bytes());
    buf.extend_from_slice(&(footer.blocks.len() as u32).to_le_bytes());
    for b in &footer.blocks {
        buf.extend_from_slice(&b.first_key.encode());
        buf.extend_from_slice(&b.last_key.encode());
        buf.extend_from_slice(&b.offset.to_le_bytes());
        buf.extend_from_slice(&b.len.to_le_bytes());
        buf.extend_from_slice(&b.raw_len.to_le_bytes());
        buf.extend_from_slice(&b.entries.to_le_bytes());
        buf.extend_from_slice(&b.crc.to_le_bytes());
        buf.push(b.enc);
    }
    buf.extend_from_slice(&(footer.bloom.bits().len() as u32).to_le_bytes());
    buf.extend_from_slice(footer.bloom.bits());
    let footer_len = (buf.len() - footer_start) as u32;
    buf.extend_from_slice(&footer_len.to_le_bytes());
    let mut crc = crc32c::crc32c(&buf[..HEADER_LEN]);
    crc = crc32c::crc32c_append(crc, &buf[footer_start..footer_start + footer_len as usize]);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf.extend_from_slice(&LAYER_FOOTER_MAGIC);
    (buf, footer)
}

fn header(kind: LayerKind) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&LAYER_MAGIC);
    buf.extend_from_slice(&LAYER_VERSION.to_le_bytes());
    buf.push(match kind {
        LayerKind::Delta => 0,
        LayerKind::Image => 1,
    });
    buf.push(0);
    debug_assert_eq!(buf.len(), HEADER_LEN);
    buf
}

/// Incremental delta layer encoder: entries stream in strictly sorted
/// by (key, lsn) and compressed blocks land as they fill, so building
/// a layer never holds more than one raw block of entries.
///
/// The filter is sized at the end from the distinct keys the layer
/// actually holds, not from an estimate of the entries. In a delta
/// layer those numbers are not close: one hot page takes a record per
/// update, and a layer of 1.6 million records off a pgbench run covers
/// tens of thousands of keys. Sized for the records the filter came
/// out at 2 MB, which every reader of that layer then fetched and held
/// (zou #343).
pub struct DeltaBuilder {
    buf: Vec<u8>,
    blocks: Vec<LayerBlock>,
    hashes: Vec<(u64, u64)>,
    raw: Vec<u8>,
    block_target: usize,
    first: LayerKey,
    last: Option<(LayerKey, Lsn)>,
    count: u32,
    total: u64,
    min_lsn: Lsn,
    max_lsn: Lsn,
}

impl DeltaBuilder {
    pub fn new(block_target: usize) -> Self {
        DeltaBuilder {
            buf: header(LayerKind::Delta),
            blocks: Vec::new(),
            hashes: Vec::new(),
            raw: Vec::new(),
            block_target,
            first: LayerKey::page(0, 0, 0, 0, 0),
            last: None,
            count: 0,
            total: 0,
            min_lsn: Lsn(u64::MAX),
            max_lsn: Lsn(0),
        }
    }

    pub fn push(&mut self, key: LayerKey, lsn: Lsn, record: &[u8]) -> Result<(), LayerBuildError> {
        if self.last.is_some_and(|p| (key, lsn) <= p) {
            return Err(LayerBuildError::Unsorted);
        }
        if record.len() > MAX_RECORD_LEN as usize {
            return Err(LayerBuildError::RecordTooLarge);
        }
        if self.last.is_none() {
            self.first = key;
        }
        // Sorted input means a key that repeats repeats right here, so
        // one comparison keeps the filter counting keys, not records.
        if self.last.is_none_or(|(prev, _)| prev != key) {
            self.hashes.push(Bloom::hash(&key.encode()));
        }
        if !self.raw.is_empty()
            && self.raw.len() + ENTRY_FIXED_LEN + record.len() > self.block_target
        {
            let last_key = self.last.expect("a filled block has entries").0;
            push_block(
                &mut self.buf,
                &mut self.blocks,
                &self.raw,
                self.first,
                last_key,
                self.count,
            );
            self.raw.clear();
            self.count = 0;
            self.first = key;
        }
        self.raw.extend_from_slice(&key.encode());
        self.raw.extend_from_slice(&lsn.0.to_le_bytes());
        self.raw
            .extend_from_slice(&(record.len() as u32).to_le_bytes());
        self.raw.extend_from_slice(record);
        self.last = Some((key, lsn));
        self.count += 1;
        self.total += 1;
        self.min_lsn = self.min_lsn.min(lsn);
        self.max_lsn = self.max_lsn.max(lsn);
        Ok(())
    }

    /// How many entries went in so far, zero meaning nothing to write.
    pub fn len(&self) -> u64 {
        self.total
    }

    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    pub fn finish(mut self) -> Result<(Vec<u8>, LayerFooter), LayerBuildError> {
        let Some((last_key, _)) = self.last else {
            return Err(LayerBuildError::Empty);
        };
        push_block(
            &mut self.buf,
            &mut self.blocks,
            &self.raw,
            self.first,
            last_key,
            self.count,
        );
        Ok(finish(
            self.buf,
            LayerKind::Delta,
            self.blocks,
            Bloom::from_hashes(&self.hashes),
            self.min_lsn,
            self.max_lsn,
            self.total,
        ))
    }
}

/// Incremental image layer encoder at one lsn, the mirror of
/// [`DeltaBuilder`] for pages streaming in strictly sorted by key.
pub struct ImageBuilder {
    buf: Vec<u8>,
    blocks: Vec<LayerBlock>,
    hashes: Vec<(u64, u64)>,
    raw: Vec<u8>,
    block_target: usize,
    lsn: Lsn,
    first: LayerKey,
    last: Option<LayerKey>,
    count: u32,
    total: u64,
}

impl ImageBuilder {
    pub fn new(lsn: Lsn, block_target: usize) -> Self {
        ImageBuilder {
            buf: header(LayerKind::Image),
            blocks: Vec::new(),
            hashes: Vec::new(),
            raw: Vec::new(),
            block_target,
            lsn,
            first: LayerKey::page(0, 0, 0, 0, 0),
            last: None,
            count: 0,
            total: 0,
        }
    }

    pub fn push(&mut self, key: LayerKey, page: &[u8]) -> Result<(), LayerBuildError> {
        if self.last.is_some_and(|p| key <= p) {
            return Err(LayerBuildError::Unsorted);
        }
        if page.len() != PAGE_IMAGE_LEN {
            return Err(LayerBuildError::BadPageLen { len: page.len() });
        }
        if self.last.is_none() {
            self.first = key;
        }
        self.hashes.push(Bloom::hash(&key.encode()));
        if !self.raw.is_empty()
            && self.raw.len() + KEY_ENCODED_LEN + PAGE_IMAGE_LEN > self.block_target
        {
            let last_key = self.last.expect("a filled block has entries");
            push_block(
                &mut self.buf,
                &mut self.blocks,
                &self.raw,
                self.first,
                last_key,
                self.count,
            );
            self.raw.clear();
            self.count = 0;
            self.first = key;
        }
        self.raw.extend_from_slice(&key.encode());
        self.raw.extend_from_slice(page);
        self.last = Some(key);
        self.count += 1;
        self.total += 1;
        Ok(())
    }

    pub fn len(&self) -> u64 {
        self.total
    }

    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// Bytes the builder is holding, blocks already encoded plus the
    /// one filling. A caller cutting layers by size reads this and
    /// starts a new builder; nobody can afford to find out how big an
    /// image was going to be by finishing it.
    pub fn bytes(&self) -> usize {
        self.buf.len() + self.raw.len()
    }

    pub fn finish(mut self) -> Result<(Vec<u8>, LayerFooter), LayerBuildError> {
        let Some(last_key) = self.last else {
            return Err(LayerBuildError::Empty);
        };
        push_block(
            &mut self.buf,
            &mut self.blocks,
            &self.raw,
            self.first,
            last_key,
            self.count,
        );
        Ok(finish(
            self.buf,
            LayerKind::Image,
            self.blocks,
            Bloom::from_hashes(&self.hashes),
            self.lsn,
            self.lsn,
            self.total,
        ))
    }
}

/// Encode one delta layer from entries already strictly sorted by
/// (key, lsn). Returns the object bytes and the footer the shard
/// manifest will summarize.
pub fn build_delta(
    entries: &[DeltaEntry],
    block_target: usize,
) -> Result<(Vec<u8>, LayerFooter), LayerBuildError> {
    let mut b = DeltaBuilder::new(block_target);
    for e in entries {
        b.push(e.key, e.lsn, &e.record)?;
    }
    b.finish()
}

/// Encode one image layer at `lsn` from entries already strictly
/// sorted by key, every page exactly [`PAGE_IMAGE_LEN`] bytes.
pub fn build_image(
    entries: &[ImageEntry],
    lsn: Lsn,
    block_target: usize,
) -> Result<(Vec<u8>, LayerFooter), LayerBuildError> {
    let mut b = ImageBuilder::new(lsn, block_target);
    for e in entries {
        b.push(e.key, &e.page)?;
    }
    b.finish()
}

fn parse_header(header: &[u8]) -> Result<LayerKind, LayerDecodeError> {
    if header.len() < HEADER_LEN {
        return Err(LayerDecodeError::Truncated {
            have: header.len(),
            need: HEADER_LEN,
        });
    }
    if header[0..4] != LAYER_MAGIC {
        return Err(LayerDecodeError::BadMagic);
    }
    let version = u16::from_le_bytes(header[4..6].try_into().unwrap());
    if version != LAYER_VERSION {
        return Err(LayerDecodeError::UnsupportedVersion { found: version });
    }
    match header[6] {
        0 => Ok(LayerKind::Delta),
        1 => Ok(LayerKind::Image),
        _ => Err(LayerDecodeError::Corrupt),
    }
}

fn parse_shell(buf: &[u8]) -> Result<(LayerFooter, usize), LayerDecodeError> {
    if buf.len() < HEADER_LEN + TAIL_LEN {
        return Err(LayerDecodeError::Truncated {
            have: buf.len(),
            need: HEADER_LEN + TAIL_LEN,
        });
    }
    let kind = parse_header(&buf[..HEADER_LEN])?;
    if buf[buf.len() - 4..] != LAYER_FOOTER_MAGIC {
        return Err(LayerDecodeError::BadMagic);
    }
    let tail_at = buf.len() - TAIL_LEN;
    let footer_len = u32::from_le_bytes(buf[tail_at..tail_at + 4].try_into().unwrap()) as usize;
    let stored_crc = u32::from_le_bytes(buf[tail_at + 4..tail_at + 8].try_into().unwrap());
    if footer_len > tail_at - HEADER_LEN {
        return Err(LayerDecodeError::Corrupt);
    }
    let footer_start = tail_at - footer_len;
    let mut crc = crc32c::crc32c(&buf[..HEADER_LEN]);
    crc = crc32c::crc32c_append(crc, &buf[footer_start..tail_at]);
    if crc != stored_crc {
        return Err(LayerDecodeError::Corrupt);
    }
    let footer = parse_footer_body(kind, &buf[footer_start..tail_at], footer_start as u64)?;
    Ok((footer, footer_start))
}

/// Parse the footer from one suffix read ending at `object_len`. When
/// the suffix is too short to hold the whole footer the error's `need`
/// says how many tail bytes to refetch, so a reader guesses once and
/// refetches exactly at most once. The block index is validated against
/// `object_len` the same way the full object decode validates it, so a
/// footer fetched by ranges proves the same claims.
///
/// The header never gets fetched. `kind` names it instead, the caller
/// taking it from the layer name, and the footer crc covers the header
/// bytes plus the footer body: a suffix whose crc matches has proved
/// the object's header is the one the caller named, kind, version and
/// all. A layer some later format version wrote fails that check as
/// corrupt rather than as an unsupported version, which is the same
/// refusal spelled differently, and it saves a round trip to the store
/// on every cold read of every layer.
pub fn read_layer_footer_suffix(
    kind: LayerKind,
    suffix: &[u8],
    object_len: u64,
) -> Result<LayerFooter, LayerDecodeError> {
    let header = header(kind);
    if suffix.len() < TAIL_LEN || (suffix.len() as u64) > object_len {
        return Err(LayerDecodeError::Truncated {
            have: suffix.len(),
            need: TAIL_LEN,
        });
    }
    if suffix[suffix.len() - 4..] != LAYER_FOOTER_MAGIC {
        return Err(LayerDecodeError::BadMagic);
    }
    let tail_at = suffix.len() - TAIL_LEN;
    let footer_len = u32::from_le_bytes(suffix[tail_at..tail_at + 4].try_into().unwrap()) as usize;
    let stored_crc = u32::from_le_bytes(suffix[tail_at + 4..tail_at + 8].try_into().unwrap());
    let need = footer_len
        .checked_add(TAIL_LEN)
        .ok_or(LayerDecodeError::Corrupt)?;
    if (need as u64) > object_len - HEADER_LEN as u64 {
        return Err(LayerDecodeError::Corrupt);
    }
    if suffix.len() < need {
        return Err(LayerDecodeError::Truncated {
            have: suffix.len(),
            need,
        });
    }
    let body = &suffix[suffix.len() - need..tail_at];
    let mut crc = crc32c::crc32c(&header[..HEADER_LEN]);
    crc = crc32c::crc32c_append(crc, body);
    if crc != stored_crc {
        return Err(LayerDecodeError::Corrupt);
    }
    parse_footer_body(kind, body, object_len - need as u64)
}

fn parse_footer_body(
    kind: LayerKind,
    body: &[u8],
    footer_start: u64,
) -> Result<LayerFooter, LayerDecodeError> {
    let mut at = 0usize;
    let take = |at: &mut usize, n: usize| -> Result<&[u8], LayerDecodeError> {
        let end = at.checked_add(n).ok_or(LayerDecodeError::Corrupt)?;
        let slice = body.get(*at..end).ok_or(LayerDecodeError::Corrupt)?;
        *at = end;
        Ok(slice)
    };
    let key = |at: &mut usize| -> Result<LayerKey, LayerDecodeError> {
        Ok(LayerKey::decode(
            take(at, KEY_ENCODED_LEN)?.try_into().unwrap(),
        ))
    };

    let min_key = key(&mut at)?;
    let max_key = key(&mut at)?;
    let min_lsn = Lsn(u64::from_le_bytes(take(&mut at, 8)?.try_into().unwrap()));
    let max_lsn = Lsn(u64::from_le_bytes(take(&mut at, 8)?.try_into().unwrap()));
    let entry_count = u64::from_le_bytes(take(&mut at, 8)?.try_into().unwrap());
    let block_count = u32::from_le_bytes(take(&mut at, 4)?.try_into().unwrap()) as usize;
    // Every block needs its index row in the body, so a lying count is
    // caught before this reserves anything.
    if block_count == 0 || block_count > (body.len() - at) / BLOCK_META_LEN {
        return Err(LayerDecodeError::Corrupt);
    }
    let mut blocks = Vec::with_capacity(block_count);
    let mut expect_at = HEADER_LEN as u64;
    let mut entry_sum = 0u64;
    for _ in 0..block_count {
        let first_key = key(&mut at)?;
        let last_key = key(&mut at)?;
        let fixed = take(&mut at, BLOCK_META_LEN - 2 * KEY_ENCODED_LEN)?;
        let block = LayerBlock {
            first_key,
            last_key,
            offset: u64::from_le_bytes(fixed[0..8].try_into().unwrap()),
            len: u32::from_le_bytes(fixed[8..12].try_into().unwrap()),
            raw_len: u32::from_le_bytes(fixed[12..16].try_into().unwrap()),
            entries: u32::from_le_bytes(fixed[16..20].try_into().unwrap()),
            crc: u32::from_le_bytes(fixed[20..24].try_into().unwrap()),
            enc: fixed[24],
        };
        // Blocks are contiguous from the header to the footer, their
        // key ranges ascend, and no block is empty or impossibly
        // large. A reader range reads on the footer's word, so the
        // footer proves its own claims here. Image blocks are fully
        // disjoint; delta blocks may share a boundary key, one key's
        // lsn chain spilling into the next block, but never overlap
        // further than that.
        let overlaps = |p: &LayerBlock| match kind {
            LayerKind::Image => block.first_key <= p.last_key,
            LayerKind::Delta => block.first_key < p.last_key,
        };
        if block.offset != expect_at
            || block.entries == 0
            || block.first_key > block.last_key
            || block.raw_len > MAX_BLOCK_RAW_LEN
            || (block.enc != ENC_RAW && block.enc != ENC_LZ4)
            || (block.enc == ENC_RAW && block.len != block.raw_len)
            || blocks.last().is_some_and(overlaps)
        {
            return Err(LayerDecodeError::Corrupt);
        }
        expect_at = expect_at
            .checked_add(block.len as u64)
            .ok_or(LayerDecodeError::Corrupt)?;
        entry_sum += block.entries as u64;
        blocks.push(block);
    }
    if expect_at != footer_start || entry_sum != entry_count {
        return Err(LayerDecodeError::Corrupt);
    }
    if min_key != blocks[0].first_key
        || max_key != blocks[blocks.len() - 1].last_key
        || min_lsn > max_lsn
    {
        return Err(LayerDecodeError::Corrupt);
    }
    let bloom_len = u32::from_le_bytes(take(&mut at, 4)?.try_into().unwrap()) as usize;
    if bloom_len > body.len() - at {
        return Err(LayerDecodeError::Corrupt);
    }
    let bloom =
        Bloom::from_bits(take(&mut at, bloom_len)?.to_vec()).ok_or(LayerDecodeError::Corrupt)?;
    if at != body.len() {
        return Err(LayerDecodeError::Corrupt);
    }
    Ok(LayerFooter {
        kind,
        min_key,
        max_key,
        min_lsn,
        max_lsn,
        entry_count,
        blocks,
        bloom,
    })
}

/// Footer only, for planning and inspection. This is what a reader
/// fetches with the first range GET.
pub fn read_layer_footer(buf: &[u8]) -> Result<LayerFooter, LayerDecodeError> {
    let (footer, _) = parse_shell(buf)?;
    Ok(footer)
}

fn block_raw(bytes: &[u8], meta: &LayerBlock) -> Result<Vec<u8>, LayerDecodeError> {
    // The raw length caps what decompression will allocate, so a lying
    // index row dies here before any allocation happens.
    if bytes.len() != meta.len as usize
        || meta.raw_len > MAX_BLOCK_RAW_LEN
        || crc32c::crc32c(bytes) != meta.crc
    {
        return Err(LayerDecodeError::Corrupt);
    }
    let raw = match meta.enc {
        ENC_RAW if meta.len == meta.raw_len => bytes.to_vec(),
        ENC_LZ4 => lz4_flex::decompress(bytes, meta.raw_len as usize)
            .map_err(|_| LayerDecodeError::Corrupt)?,
        _ => return Err(LayerDecodeError::Corrupt),
    };
    if raw.len() != meta.raw_len as usize {
        return Err(LayerDecodeError::Corrupt);
    }
    Ok(raw)
}

/// Decode one delta block fetched by the byte range in its index row,
/// standalone: the crc in the row verifies the bytes came back intact.
pub fn decode_delta_block(
    bytes: &[u8],
    meta: &LayerBlock,
) -> Result<Vec<DeltaEntry>, LayerDecodeError> {
    let raw = block_raw(bytes, meta)?;
    let mut entries = Vec::with_capacity(meta.entries as usize);
    let mut at = 0usize;
    for _ in 0..meta.entries {
        let fixed = raw
            .get(at..at + ENTRY_FIXED_LEN)
            .ok_or(LayerDecodeError::Corrupt)?;
        let key = LayerKey::decode(fixed[..KEY_ENCODED_LEN].try_into().unwrap());
        let lsn = Lsn(u64::from_le_bytes(fixed[18..26].try_into().unwrap()));
        let len = u32::from_le_bytes(fixed[26..30].try_into().unwrap());
        if len > MAX_RECORD_LEN {
            return Err(LayerDecodeError::Corrupt);
        }
        at += ENTRY_FIXED_LEN;
        let record = raw
            .get(at..at + len as usize)
            .ok_or(LayerDecodeError::Corrupt)?
            .to_vec();
        at += len as usize;
        if entries
            .last()
            .is_some_and(|p: &DeltaEntry| (key, lsn) <= (p.key, p.lsn))
        {
            return Err(LayerDecodeError::Corrupt);
        }
        entries.push(DeltaEntry { key, lsn, record });
    }
    if at != raw.len()
        || entries.first().map(|e| e.key) != Some(meta.first_key)
        || entries.last().map(|e| e.key) != Some(meta.last_key)
    {
        return Err(LayerDecodeError::Corrupt);
    }
    Ok(entries)
}

/// Decode one image block fetched by the byte range in its index row.
pub fn decode_image_block(
    bytes: &[u8],
    meta: &LayerBlock,
) -> Result<Vec<ImageEntry>, LayerDecodeError> {
    let raw = block_raw(bytes, meta)?;
    let stride = KEY_ENCODED_LEN + PAGE_IMAGE_LEN;
    if raw.len() != meta.entries as usize * stride {
        return Err(LayerDecodeError::Corrupt);
    }
    let mut entries = Vec::with_capacity(meta.entries as usize);
    for chunk in raw.chunks_exact(stride) {
        let key = LayerKey::decode(chunk[..KEY_ENCODED_LEN].try_into().unwrap());
        if entries.last().is_some_and(|p: &ImageEntry| key <= p.key) {
            return Err(LayerDecodeError::Corrupt);
        }
        entries.push(ImageEntry {
            key,
            page: chunk[KEY_ENCODED_LEN..].to_vec(),
        });
    }
    if entries.first().map(|e| e.key) != Some(meta.first_key)
        || entries.last().map(|e| e.key) != Some(meta.last_key)
    {
        return Err(LayerDecodeError::Corrupt);
    }
    Ok(entries)
}

fn check_kind(footer: &LayerFooter, expected: LayerKind) -> Result<(), LayerDecodeError> {
    if footer.kind != expected {
        return Err(LayerDecodeError::WrongKind {
            expected,
            found: footer.kind,
        });
    }
    Ok(())
}

/// Full decode of a delta layer: every entry in key then lsn order,
/// plus the footer.
pub fn decode_delta(buf: &[u8]) -> Result<(Vec<DeltaEntry>, LayerFooter), LayerDecodeError> {
    let (footer, _) = parse_shell(buf)?;
    check_kind(&footer, LayerKind::Delta)?;
    let mut entries: Vec<DeltaEntry> = Vec::new();
    for (index, meta) in footer.blocks.iter().enumerate() {
        let bytes = &buf[meta.offset as usize..(meta.offset + meta.len as u64) as usize];
        let block =
            decode_delta_block(bytes, meta).map_err(|_| LayerDecodeError::Block { index })?;
        // Per block ordering is checked inside the block decode; when
        // one key's records spill across the boundary the lsns must
        // keep ascending too.
        if let (Some(prev), Some(first)) = (entries.last(), block.first())
            && (first.key, first.lsn) <= (prev.key, prev.lsn)
        {
            return Err(LayerDecodeError::Block { index });
        }
        entries.extend(block);
    }
    Ok((entries, footer))
}

/// Stream a delta layer's entries in (key, lsn) order holding one
/// decoded block at a time, the merge reader for compaction. An error
/// item reports the corrupt block and ends the stream.
pub struct DeltaCursor<'a> {
    buf: &'a [u8],
    footer: LayerFooter,
    block: usize,
    entries: std::vec::IntoIter<DeltaEntry>,
    prev: Option<(LayerKey, Lsn)>,
    dead: bool,
}

pub fn delta_cursor(buf: &[u8]) -> Result<DeltaCursor<'_>, LayerDecodeError> {
    let (footer, _) = parse_shell(buf)?;
    check_kind(&footer, LayerKind::Delta)?;
    Ok(DeltaCursor {
        buf,
        footer,
        block: 0,
        entries: Vec::new().into_iter(),
        prev: None,
        dead: false,
    })
}

impl DeltaCursor<'_> {
    pub fn footer(&self) -> &LayerFooter {
        &self.footer
    }
}

impl Iterator for DeltaCursor<'_> {
    type Item = Result<DeltaEntry, LayerDecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.dead {
            return None;
        }
        loop {
            if let Some(e) = self.entries.next() {
                // Per block ordering is checked in the block decode;
                // across the boundary the order must hold too.
                if self.prev.is_some_and(|p| (e.key, e.lsn) <= p) {
                    self.dead = true;
                    return Some(Err(LayerDecodeError::Block {
                        index: self.block - 1,
                    }));
                }
                self.prev = Some((e.key, e.lsn));
                return Some(Ok(e));
            }
            let meta = self.footer.blocks.get(self.block)?;
            let index = self.block;
            self.block += 1;
            let bytes = &self.buf[meta.offset as usize..(meta.offset + meta.len as u64) as usize];
            match decode_delta_block(bytes, meta) {
                Ok(entries) => self.entries = entries.into_iter(),
                Err(_) => {
                    self.dead = true;
                    return Some(Err(LayerDecodeError::Block { index }));
                }
            }
        }
    }
}

/// Stream an image layer's pages in key order, the mirror of
/// [`DeltaCursor`].
pub struct ImageCursor<'a> {
    buf: &'a [u8],
    footer: LayerFooter,
    block: usize,
    entries: std::vec::IntoIter<ImageEntry>,
    dead: bool,
}

pub fn image_cursor(buf: &[u8]) -> Result<ImageCursor<'_>, LayerDecodeError> {
    let (footer, _) = parse_shell(buf)?;
    check_kind(&footer, LayerKind::Image)?;
    Ok(ImageCursor {
        buf,
        footer,
        block: 0,
        entries: Vec::new().into_iter(),
        dead: false,
    })
}

impl ImageCursor<'_> {
    pub fn footer(&self) -> &LayerFooter {
        &self.footer
    }
}

impl Iterator for ImageCursor<'_> {
    type Item = Result<ImageEntry, LayerDecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.dead {
            return None;
        }
        loop {
            if let Some(e) = self.entries.next() {
                return Some(Ok(e));
            }
            let meta = self.footer.blocks.get(self.block)?;
            let index = self.block;
            self.block += 1;
            let bytes = &self.buf[meta.offset as usize..(meta.offset + meta.len as u64) as usize];
            match decode_image_block(bytes, meta) {
                Ok(entries) => self.entries = entries.into_iter(),
                Err(_) => {
                    self.dead = true;
                    return Some(Err(LayerDecodeError::Block { index }));
                }
            }
        }
    }
}

/// Full decode of an image layer: every entry in key order, plus the
/// footer.
pub fn decode_image(buf: &[u8]) -> Result<(Vec<ImageEntry>, LayerFooter), LayerDecodeError> {
    let (footer, _) = parse_shell(buf)?;
    check_kind(&footer, LayerKind::Image)?;
    let mut entries = Vec::new();
    for (index, meta) in footer.blocks.iter().enumerate() {
        let bytes = &buf[meta.offset as usize..(meta.offset + meta.len as u64) as usize];
        let block =
            decode_image_block(bytes, meta).map_err(|_| LayerDecodeError::Block { index })?;
        entries.extend(block);
    }
    Ok((entries, footer))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Deterministic bytes without a rand dep, same trick as sealed.rs.
    fn lcg_bytes(seed: u64, len: usize) -> Vec<u8> {
        let mut state = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        (0..len)
            .map(|_| {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (state >> 33) as u8
            })
            .collect()
    }

    fn delta_entries(n: usize) -> Vec<DeltaEntry> {
        (0..n)
            .map(|i| DeltaEntry {
                key: LayerKey::page(1663, 5, 16384, 0, (i / 3) as u32),
                lsn: Lsn(0x1000 + i as u64 * 8),
                record: lcg_bytes(i as u64, 40 + (i * 7) % 300),
            })
            .collect()
    }

    fn image_entries(n: usize) -> Vec<ImageEntry> {
        (0..n)
            .map(|i| ImageEntry {
                key: LayerKey::page(1663, 5, 16384, 0, i as u32),
                page: lcg_bytes(i as u64, PAGE_IMAGE_LEN),
            })
            .collect()
    }

    /// The shape of a real delta layer: a page takes a record on every
    /// update, so records outnumber keys many times over. The filter
    /// has to follow the keys, and the layer still has to answer for
    /// every key it holds and mostly say no to the ones it does not.
    #[test]
    fn a_layer_of_repeated_keys_carries_a_filter_sized_for_the_keys() {
        let keys = 500usize;
        let per_key = 40usize;
        let entries: Vec<DeltaEntry> = (0..keys * per_key)
            .map(|i| DeltaEntry {
                key: LayerKey::page(1663, 5, 16384, 0, (i / per_key) as u32),
                lsn: Lsn(0x1000 + i as u64 * 8),
                record: lcg_bytes(i as u64, 64),
            })
            .collect();
        let (_, footer) = build_delta(&entries, LAYER_BLOCK_TARGET).unwrap();

        assert_eq!(footer.entry_count, (keys * per_key) as u64);
        assert_eq!(
            footer.bloom.bits().len(),
            Bloom::sized_for(keys).bits().len(),
            "the filter is sized for {keys} keys, not {} records",
            keys * per_key
        );
        for i in 0..keys as u32 {
            assert!(
                footer.may_contain(&LayerKey::page(1663, 5, 16384, 0, i)),
                "block {i} is in the layer and the filter has to say so"
            );
        }
        let absent = (keys as u32..keys as u32 * 2)
            .filter(|&i| !footer.may_contain(&LayerKey::page(1663, 5, 16384, 0, i)))
            .count();
        assert!(
            absent > keys * 9 / 10,
            "only {absent} of {keys} absent keys missed, the filter is too small"
        );
    }

    #[test]
    fn delta_round_trips() {
        let entries = delta_entries(50);
        let (buf, footer) = build_delta(&entries, LAYER_BLOCK_TARGET).unwrap();
        let (back, footer2) = decode_delta(&buf).unwrap();
        assert_eq!(back, entries);
        assert_eq!(footer2, footer);
        assert_eq!(footer.kind, LayerKind::Delta);
        assert_eq!(footer.entry_count, 50);
        assert_eq!(footer.min_key, entries[0].key);
        assert_eq!(footer.max_key, entries[49].key);
        assert_eq!(footer.min_lsn, entries[0].lsn);
        assert_eq!(footer.max_lsn, entries[49].lsn);
    }

    #[test]
    fn image_round_trips() {
        let entries = image_entries(20);
        let (buf, footer) = build_image(&entries, Lsn(0x9000), LAYER_BLOCK_TARGET).unwrap();
        let (back, footer2) = decode_image(&buf).unwrap();
        assert_eq!(back, entries);
        assert_eq!(footer2, footer);
        assert_eq!(footer.kind, LayerKind::Image);
        assert_eq!(footer.min_lsn, Lsn(0x9000));
        assert_eq!(footer.max_lsn, Lsn(0x9000));
    }

    #[test]
    fn small_block_target_splits_into_many_blocks() {
        let entries = delta_entries(200);
        let (buf, footer) = build_delta(&entries, 2048).unwrap();
        assert!(
            footer.blocks.len() > 10,
            "got {} blocks",
            footer.blocks.len()
        );
        let (back, _) = decode_delta(&buf).unwrap();
        assert_eq!(back, entries);
        let images = image_entries(30);
        let (ibuf, ifooter) = build_image(&images, Lsn(7), 3 * PAGE_IMAGE_LEN).unwrap();
        assert!(ifooter.blocks.len() >= 10);
        let (iback, _) = decode_image(&ibuf).unwrap();
        assert_eq!(iback, images);
    }

    #[test]
    fn blocks_decode_standalone_from_their_byte_ranges() {
        let entries = delta_entries(200);
        let (buf, footer) = build_delta(&entries, 2048).unwrap();
        let mut collected = Vec::new();
        for meta in &footer.blocks {
            let range = &buf[meta.offset as usize..meta.offset as usize + meta.len as usize];
            collected.extend(decode_delta_block(range, meta).unwrap());
        }
        assert_eq!(collected, entries);
    }

    #[test]
    fn cursors_stream_what_the_full_decode_returns() {
        let entries = delta_entries(200);
        let (buf, footer) = build_delta(&entries, 2048).unwrap();
        let cursor = delta_cursor(&buf).unwrap();
        assert_eq!(cursor.footer(), &footer);
        let streamed: Vec<_> = cursor.map(|r| r.unwrap()).collect();
        assert_eq!(streamed, entries);
        let images = image_entries(30);
        let (ibuf, ifooter) = build_image(&images, Lsn(7), 3 * PAGE_IMAGE_LEN).unwrap();
        let cursor = image_cursor(&ibuf).unwrap();
        assert_eq!(cursor.footer(), &ifooter);
        let streamed: Vec<_> = cursor.map(|r| r.unwrap()).collect();
        assert_eq!(streamed, images);
    }

    #[test]
    fn a_corrupt_block_ends_the_cursor_with_one_error() {
        let entries = delta_entries(200);
        let (mut buf, footer) = build_delta(&entries, 2048).unwrap();
        let meta = &footer.blocks[2];
        buf[meta.offset as usize + 4] ^= 0xff;
        let mut clean = 0;
        let mut cursor = delta_cursor(&buf).unwrap();
        for item in &mut cursor {
            match item {
                Ok(_) => clean += 1,
                Err(LayerDecodeError::Block { index }) => {
                    assert_eq!(index, 2);
                    break;
                }
                Err(other) => panic!("unexpected {other:?}"),
            }
        }
        assert!(cursor.next().is_none(), "the stream ends at the error");
        let before: u32 = footer.blocks[..2].iter().map(|b| b.entries).sum();
        assert_eq!(clean, before, "every entry before the bad block came out");
    }

    #[test]
    fn locate_finds_the_blocks_for_every_key() {
        let entries = image_entries(40);
        let (buf, footer) = build_image(&entries, Lsn(7), 3 * PAGE_IMAGE_LEN).unwrap();
        for e in &entries {
            let run = footer.locate(&e.key);
            assert_eq!(run.len(), 1, "image keys live in exactly one block");
            let meta = &run[0];
            let range = &buf[meta.offset as usize..meta.offset as usize + meta.len as usize];
            let block = decode_image_block(range, meta).unwrap();
            assert!(block.iter().any(|b| b.key == e.key));
        }
        let below = LayerKey::page(0, 0, 0, 0, 0);
        let above = LayerKey::relsize(u32::MAX, u32::MAX, u32::MAX, u8::MAX);
        assert!(footer.locate(&below).is_empty());
        assert!(footer.locate(&above).is_empty());
    }

    #[test]
    fn locate_returns_every_block_a_spilling_key_touches() {
        // One key with many records under a tiny block target must
        // spill across blocks, and locate has to name all of them.
        let entries: Vec<DeltaEntry> = (0..50u64)
            .map(|i| DeltaEntry {
                key: LayerKey::page(1, 1, 1, 0, 7),
                lsn: Lsn(100 + i),
                record: lcg_bytes(i, 200),
            })
            .collect();
        let (buf, footer) = build_delta(&entries, 1024).unwrap();
        let run = footer.locate(&entries[0].key);
        assert_eq!(run.len(), footer.blocks.len());
        assert!(run.len() > 5);
        let mut collected = Vec::new();
        for meta in run {
            let range = &buf[meta.offset as usize..meta.offset as usize + meta.len as usize];
            collected.extend(decode_delta_block(range, meta).unwrap());
        }
        assert_eq!(collected, entries);
    }

    #[test]
    fn the_bloom_has_no_false_negatives() {
        let entries = image_entries(500);
        let (_, footer) = build_image(&entries, Lsn(7), LAYER_BLOCK_TARGET).unwrap();
        for e in &entries {
            assert!(footer.may_contain(&e.key));
        }
        let misses = (10_000u32..10_200)
            .filter(|b| !footer.may_contain(&LayerKey::page(1663, 5, 16384, 0, *b)))
            .count();
        assert!(misses > 190, "only {misses} of 200 absent keys missed");
    }

    #[test]
    fn the_footer_alone_agrees_with_the_full_decode() {
        let entries = delta_entries(120);
        let (buf, _) = build_delta(&entries, 4096).unwrap();
        let footer = read_layer_footer(&buf).unwrap();
        let (back, full_footer) = decode_delta(&buf).unwrap();
        assert_eq!(footer, full_footer);
        assert_eq!(back.len() as u64, footer.entry_count);
    }

    #[test]
    fn the_footer_parses_from_one_suffix_range_alone() {
        let entries = delta_entries(120);
        let (buf, footer) = build_delta(&entries, 4096).unwrap();
        // A generous suffix guess parses in one shot, no header read.
        let from = buf.len().saturating_sub(4096);
        let got =
            read_layer_footer_suffix(LayerKind::Delta, &buf[from..], buf.len() as u64).unwrap();
        assert_eq!(got, footer);
        // A guess that only covers the tail names the exact refetch.
        let tail_only = &buf[buf.len() - TAIL_LEN..];
        let need = match read_layer_footer_suffix(LayerKind::Delta, tail_only, buf.len() as u64) {
            Err(LayerDecodeError::Truncated { need, .. }) => need,
            other => panic!("expected a truncated error, got {other:?}"),
        };
        let exact = &buf[buf.len() - need..];
        let got = read_layer_footer_suffix(LayerKind::Delta, exact, buf.len() as u64).unwrap();
        assert_eq!(got, footer);
        // A lying object length shifts the block index base and fails.
        assert!(read_layer_footer_suffix(LayerKind::Delta, exact, buf.len() as u64 + 8).is_err());
        // A corrupt suffix byte fails the crc.
        let mut bad = exact.to_vec();
        bad[3] ^= 0x41;
        assert!(read_layer_footer_suffix(LayerKind::Delta, &bad, buf.len() as u64).is_err());
        // And so does the wrong kind: the crc covers the header bytes
        // the caller named, so a delta cannot be read as an image.
        assert!(matches!(
            read_layer_footer_suffix(LayerKind::Image, exact, buf.len() as u64),
            Err(LayerDecodeError::Corrupt)
        ));
    }

    #[test]
    fn empty_layers_are_refused() {
        assert_eq!(build_delta(&[], 1024), Err(LayerBuildError::Empty));
        assert_eq!(build_image(&[], Lsn(1), 1024), Err(LayerBuildError::Empty));
    }

    #[test]
    fn unsorted_and_duplicate_entries_are_refused() {
        let mut entries = delta_entries(10);
        entries.swap(3, 7);
        assert_eq!(build_delta(&entries, 1024), Err(LayerBuildError::Unsorted));
        let mut dup = delta_entries(10);
        dup[4] = dup[3].clone();
        assert_eq!(build_delta(&dup, 1024), Err(LayerBuildError::Unsorted));
        let mut images = image_entries(10);
        images.swap(2, 8);
        assert_eq!(
            build_image(&images, Lsn(1), 1024),
            Err(LayerBuildError::Unsorted)
        );
    }

    #[test]
    fn wrong_page_length_is_refused() {
        let mut entries = image_entries(3);
        entries[1].page.pop();
        assert_eq!(
            build_image(&entries, Lsn(1), 1024),
            Err(LayerBuildError::BadPageLen {
                len: PAGE_IMAGE_LEN - 1
            })
        );
    }

    #[test]
    fn decoding_a_delta_as_an_image_is_refused() {
        let (buf, _) = build_delta(&delta_entries(5), 1024).unwrap();
        assert_eq!(
            decode_image(&buf).unwrap_err(),
            LayerDecodeError::WrongKind {
                expected: LayerKind::Image,
                found: LayerKind::Delta,
            }
        );
        let (ibuf, _) = build_image(&image_entries(2), Lsn(1), 1024).unwrap();
        assert_eq!(
            decode_delta(&ibuf).unwrap_err(),
            LayerDecodeError::WrongKind {
                expected: LayerKind::Delta,
                found: LayerKind::Image,
            }
        );
    }

    #[test]
    fn future_versions_are_refused_with_an_upgrade_hint() {
        let (mut buf, _) = build_delta(&delta_entries(5), 1024).unwrap();
        buf[4..6].copy_from_slice(&2u16.to_le_bytes());
        assert_eq!(
            read_layer_footer(&buf).unwrap_err(),
            LayerDecodeError::UnsupportedVersion { found: 2 }
        );
    }

    #[test]
    fn every_single_byte_corruption_is_caught() {
        let entries = delta_entries(12);
        let (buf, _) = build_delta(&entries, 1024).unwrap();
        for at in 0..buf.len() {
            let mut bad = buf.clone();
            bad[at] ^= 0x41;
            if let Ok((back, _)) = decode_delta(&bad) {
                panic!(
                    "flip at {at} of {} decoded to {} entries",
                    buf.len(),
                    back.len()
                );
            }
        }
    }

    #[test]
    fn truncation_at_every_length_is_caught() {
        let (buf, _) = build_image(&image_entries(3), Lsn(9), 1024).unwrap();
        for len in 0..buf.len() {
            assert!(
                decode_image(&buf[..len]).is_err(),
                "truncation to {len} decoded"
            );
        }
    }

    #[test]
    fn random_garbage_never_panics() {
        for seed in 0..200u64 {
            let junk = lcg_bytes(seed, 64 + (seed as usize * 37) % 4096);
            let _ = read_layer_footer(&junk);
            let _ = decode_delta(&junk);
            let _ = decode_image(&junk);
        }
        let mut disguised = lcg_bytes(1, 512);
        disguised[..4].copy_from_slice(&LAYER_MAGIC);
        let end = disguised.len() - 4;
        disguised[end..].copy_from_slice(&LAYER_FOOTER_MAGIC);
        let _ = decode_delta(&disguised);
    }

    #[test]
    fn lying_length_fields_never_allocate_or_read_wild() {
        // A syntactically valid tail whose footer length points past
        // the buffer, and a block count larger than the body can hold.
        let (buf, _) = build_delta(&delta_entries(8), 1024).unwrap();
        let tail_at = buf.len() - 12;
        let mut lying = buf.clone();
        lying[tail_at..tail_at + 4].copy_from_slice(&(u32::MAX).to_le_bytes());
        assert!(decode_delta(&lying).is_err());
        let footer = read_layer_footer(&buf).unwrap();
        let mut meta = footer.blocks[0];
        meta.raw_len = u32::MAX;
        let range = &buf[meta.offset as usize..meta.offset as usize + meta.len as usize];
        assert!(decode_delta_block(range, &meta).is_err());
        meta = footer.blocks[0];
        meta.len = u32::MAX;
        assert!(decode_delta_block(range, &meta).is_err());
    }

    #[test]
    fn keys_order_and_round_trip() {
        let a = LayerKey::page(1, 2, 3, 0, 9);
        let b = LayerKey::page(1, 2, 3, 1, 0);
        let c = LayerKey::relsize(0, 0, 0, 0);
        assert!(a < b, "fork orders before block");
        assert!(b < c, "every page key orders before every relsize key");
        for k in [a, b, c] {
            assert_eq!(LayerKey::decode(&k.encode()), k);
            assert_eq!(k.hex().len(), 36);
        }
        // Encoded byte order must agree with the derived Ord, layers
        // are named by encoded ranges.
        assert!(a.encode() < b.encode());
        assert!(b.encode() < c.encode());
    }

    #[test]
    fn incompressible_blocks_are_stored_raw() {
        // lcg pages are incompressible, so lz4 would grow them and the
        // builder must fall back to raw storage.
        let entries = image_entries(2);
        let (buf, footer) = build_image(&entries, Lsn(1), LAYER_BLOCK_TARGET).unwrap();
        assert_eq!(footer.blocks[0].len, footer.blocks[0].raw_len);
        let (back, _) = decode_image(&buf).unwrap();
        assert_eq!(back, entries);
        // Zero pages compress, so the other path is exercised too.
        let zeros: Vec<ImageEntry> = (0..4)
            .map(|i| ImageEntry {
                key: LayerKey::page(1, 1, 1, 0, i),
                page: vec![0; PAGE_IMAGE_LEN],
            })
            .collect();
        let (zbuf, zfooter) = build_image(&zeros, Lsn(1), LAYER_BLOCK_TARGET).unwrap();
        assert!(zfooter.blocks[0].len < zfooter.blocks[0].raw_len);
        assert!(zbuf.len() < 4 * PAGE_IMAGE_LEN / 8);
        let (zback, _) = decode_image(&zbuf).unwrap();
        assert_eq!(zback, zeros);
    }

    #[test]
    fn an_oversized_record_gets_its_own_block() {
        let entries: Vec<DeltaEntry> = (0..6u32)
            .map(|i| DeltaEntry {
                key: LayerKey::page(1, 1, 1, 0, i),
                lsn: Lsn(10 + i as u64),
                record: if i == 3 {
                    lcg_bytes(9, 3 * LAYER_BLOCK_TARGET)
                } else {
                    lcg_bytes(i as u64, 50)
                },
            })
            .collect();
        let (buf, footer) = build_delta(&entries, 1024).unwrap();
        assert!(
            footer
                .blocks
                .iter()
                .any(|b| b.raw_len as usize > LAYER_BLOCK_TARGET)
        );
        let (back, _) = decode_delta(&buf).unwrap();
        assert_eq!(back, entries);
    }

    #[test]
    fn records_over_the_cap_are_refused() {
        let entries = vec![DeltaEntry {
            key: LayerKey::page(1, 1, 1, 0, 0),
            lsn: Lsn(1),
            record: vec![0; MAX_RECORD_LEN as usize + 1],
        }];
        assert_eq!(
            build_delta(&entries, 1024),
            Err(LayerBuildError::RecordTooLarge)
        );
    }
}
