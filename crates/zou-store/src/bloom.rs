//! Byte keyed bloom filter for layer footers.
//!
//! Layer footers answer "is this key in here at all" before a reader
//! pays a range GET for a block. The bit layout is format frozen:
//! layers store these bits on disk, so the hash scheme and sizing rule
//! must never change for the format version.
//!
//! Double hashing: h1 and h2 start as crc32c of the key, plain and
//! seeded with 0x5EED, then each runs through the splitmix64 finalizer
//! and h2 is forced odd. Probe i lands on h1 + i * h2 modulo the bit
//! count. The finalizer matters: crc32c alone is linear over GF(2), so
//! dense sequential keys, which is exactly what block numbers in a
//! layer look like, land on correlated positions and the false
//! positive rate collapses. Sealed segment footers keep their own
//! older filter without the finalizer; their keys are sha derived
//! tenant ids, which never cluster, and their bits are already on
//! disk. Seven probes at roughly ten bits per key gives about a one
//! percent false positive rate.

const BLOOM_HASHES: u32 = 7;

/// The 64-bit finalizer from splitmix64. Nonlinear over GF(2), which
/// is the whole point, see the module doc.
fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

/// Where one key's probes land in a filter of `bits` bits.
fn probes(h1: u64, h2: u64, bits: u64) -> impl Iterator<Item = usize> {
    (0..BLOOM_HASHES as u64).map(move |i| (h1.wrapping_add(i.wrapping_mul(h2)) % bits) as usize)
}

/// The smallest legal filter, and the floor `sized_for` rounds up to.
pub const BLOOM_MIN_BYTES: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bloom {
    bits: Vec<u8>,
}

impl Bloom {
    /// An empty filter sized for roughly ten bits per expected key,
    /// rounded up to a power of two of at least [`BLOOM_MIN_BYTES`].
    pub fn sized_for(keys: usize) -> Self {
        let bytes = (keys * 10)
            .div_ceil(8)
            .next_power_of_two()
            .max(BLOOM_MIN_BYTES);
        Self {
            bits: vec![0; bytes],
        }
    }

    /// A filter over keys hashed before its size was known, sized for
    /// exactly the hashes given.
    ///
    /// A probe position depends on the bit count, so a filter cannot
    /// take a key until it knows how big it is, and a streaming layer
    /// builder does not know how many distinct keys it will see until
    /// the last one is in. Taking the hash once up front and setting
    /// the bits at the end costs 16 bytes a key while a layer builds
    /// and saves the difference between a guess and the truth in every
    /// footer the layer is ever read with.
    pub fn from_hashes(hashes: &[(u64, u64)]) -> Self {
        let mut bloom = Self::sized_for(hashes.len().max(1));
        let bits = (bloom.bits.len() * 8) as u64;
        for &(h1, h2) in hashes {
            for pos in probes(h1, h2, bits) {
                bloom.bits[pos / 8] |= 1 << (pos % 8);
            }
        }
        bloom
    }

    /// The pair a key hashes to, for [`Bloom::from_hashes`]. Part of
    /// the frozen layout, see the module doc.
    pub fn hash(key: &[u8]) -> (u64, u64) {
        (
            mix(crc32c::crc32c(key) as u64),
            mix(crc32c::crc32c_append(0x5EED, key) as u64) | 1,
        )
    }

    /// Adopt bits read back from a footer. Refuses sizes the builder
    /// cannot have produced, so a lying length field dies here.
    pub fn from_bits(bits: Vec<u8>) -> Option<Self> {
        if !bits.len().is_power_of_two() || bits.len() < BLOOM_MIN_BYTES {
            return None;
        }
        Some(Self { bits })
    }

    /// The raw bits, for embedding in a footer.
    pub fn bits(&self) -> &[u8] {
        &self.bits
    }

    fn positions(&self, key: &[u8]) -> impl Iterator<Item = usize> {
        let (h1, h2) = Self::hash(key);
        probes(h1, h2, (self.bits.len() * 8) as u64)
    }

    pub fn insert(&mut self, key: &[u8]) {
        for pos in self.positions(key).collect::<Vec<_>>() {
            self.bits[pos / 8] |= 1 << (pos % 8);
        }
    }

    /// False positives are possible, false negatives are not.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.positions(key)
            .collect::<Vec<_>>()
            .into_iter()
            .all(|pos| self.bits[pos / 8] & (1 << (pos % 8)) != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserted_keys_are_always_found() {
        let mut bloom = Bloom::sized_for(500);
        for i in 0u32..500 {
            bloom.insert(&i.to_le_bytes());
        }
        for i in 0u32..500 {
            assert!(bloom.may_contain(&i.to_le_bytes()));
        }
    }

    #[test]
    fn most_absent_keys_miss() {
        let mut bloom = Bloom::sized_for(300);
        for i in 0u32..300 {
            bloom.insert(&i.to_le_bytes());
        }
        // The absent keys right next to the inserted ones are the hard
        // case: without the nonlinear finalizer the crc correlation on
        // dense sequential keys pushed over a quarter of these through.
        let near = (300u32..500)
            .filter(|i| !bloom.may_contain(&i.to_le_bytes()))
            .count();
        assert!(near > 190, "only {near} of 200 adjacent absent keys missed");
        let far = (1_000_000u32..1_000_200)
            .filter(|i| !bloom.may_contain(&i.to_le_bytes()))
            .count();
        assert!(far > 190, "only {far} of 200 distant absent keys missed");
    }

    #[test]
    fn hashing_first_and_setting_bits_later_is_the_same_filter() {
        // The deferred path is what layer builders use, so it has to
        // land on the same bits as inserting into a filter that was
        // sized right from the start, not merely on a working filter.
        let keys: Vec<[u8; 4]> = (0u32..500).map(u32::to_le_bytes).collect();
        let mut eager = Bloom::sized_for(500);
        for key in &keys {
            eager.insert(key);
        }
        let hashes: Vec<(u64, u64)> = keys.iter().map(|k| Bloom::hash(k)).collect();
        let deferred = Bloom::from_hashes(&hashes);
        assert_eq!(deferred.bits(), eager.bits());
        assert!(keys.iter().all(|k| deferred.may_contain(k)));
        assert_eq!(
            Bloom::from_hashes(&[]).bits().len(),
            BLOOM_MIN_BYTES,
            "a layer of no keys still gets a legal filter"
        );
    }

    #[test]
    fn from_bits_refuses_impossible_sizes() {
        assert!(Bloom::from_bits(vec![0; 63]).is_none());
        assert!(Bloom::from_bits(vec![0; 96]).is_none());
        assert!(Bloom::from_bits(vec![0; 0]).is_none());
        assert!(Bloom::from_bits(vec![0; 64]).is_some());
        assert!(Bloom::from_bits(vec![0; 1024]).is_some());
    }

    #[test]
    fn the_bit_layout_is_pinned_forever() {
        // Layers persist these bits, so the hash scheme is part of the
        // format. If this test fails the change corrupts every layer
        // footer already written.
        let mut bloom = Bloom::sized_for(3);
        bloom.insert(&7u128.to_le_bytes());
        bloom.insert(&99u128.to_le_bytes());
        let set: Vec<usize> = (0..bloom.bits().len() * 8)
            .filter(|p| bloom.bits()[p / 8] & (1 << (p % 8)) != 0)
            .collect();
        assert_eq!(
            set,
            vec![
                97, 114, 149, 184, 196, 213, 308, 312, 343, 411, 467, 502, 510
            ]
        );
    }
}
