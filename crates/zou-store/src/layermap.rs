//! Layer map: which layers a lookup touches (spec 04 section 2).
//!
//! The shard manifest lists layers by name, key range, lsn range and
//! size. This module turns that list into an in-memory structure that
//! answers the read algorithm for `(key, lsn)`: the newest image layer
//! at or below the lsn that covers the key, plus every delta layer
//! that could hold records for the key in the window between that
//! image and the read lsn. Compaction is what keeps the answer small,
//! and the plan reports its own read amplification so that the bound
//! is observable instead of assumed. Images count one each, not one
//! in total: they are sparse, so a lookup may walk several before it
//! finds the base for its key.
//!
//! Both layer kinds are indexed by an interval tree over the key
//! space, the classic centered kind: a stab costs O(log n) plus one
//! entry per layer actually covering the key. Lsn filtering happens on
//! the stabbed candidates, which is exactly the covering set, so a
//! query never scans layers whose key range excludes the key.
//!
//! Layer names are format frozen and sort usefully: the kind letter,
//! the encoded key range, the lsn range in fixed width hex. A name
//! carries everything the map needs except the size, which rides next
//! to the name in the manifest.

use crate::layer::{KEY_ENCODED_LEN, LayerKey, LayerKind};
use crate::lsn::Lsn;

/// One layer as the shard manifest lists it: enough to plan reads and
/// fetch by range without touching the object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayerDesc {
    pub kind: LayerKind,
    /// Inclusive key bounds, the footer's min and max key.
    pub min_key: LayerKey,
    pub max_key: LayerKey,
    /// Inclusive lsn bounds. An image layer has one lsn, both bounds
    /// equal: every page in it is materialized as of that lsn.
    pub min_lsn: Lsn,
    pub max_lsn: Lsn,
    /// Object length in bytes, for suffix range reads of the footer.
    pub size: u64,
    /// The tenant that owns the object, set on layers a branch
    /// inherited. The reader fetches these from the owner's shard
    /// prefix; the name and bounds are the owner's unchanged, which is
    /// why the tag lives here and not in the name.
    pub owner: Option<String>,
    /// The branch cut for an inherited layer: records above this lsn
    /// belong to the parent's future, not the child's history, and
    /// must not serve. A delta spanning the cut stays listed and gets
    /// clamped here; layers entirely below their cut carry no clamp.
    pub upto: Option<Lsn>,
    /// The shard whose prefix holds the object, set on layers served
    /// across a split or merge: a child reads its ancestors' layers in
    /// place until compaction separates them. Never stored, stamped at
    /// load time from which shard manifest listed the layer, and
    /// composes with `owner` when the ancestor era was itself
    /// inherited from a branch parent.
    pub home: Option<u16>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LayerNameError {
    #[error("{0:?} is not a layer name")]
    Malformed(String),
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LayerMapError {
    #[error("layer {name} has inverted bounds")]
    Inverted { name: String },
    #[error("image layer {name} spans lsns, images live at one lsn")]
    ImageSpansLsns { name: String },
}

impl LayerDesc {
    pub fn delta(min_key: LayerKey, max_key: LayerKey, min_lsn: Lsn, max_lsn: Lsn) -> Self {
        Self {
            kind: LayerKind::Delta,
            min_key,
            max_key,
            min_lsn,
            max_lsn,
            size: 0,
            owner: None,
            upto: None,
            home: None,
        }
    }

    pub fn image(min_key: LayerKey, max_key: LayerKey, lsn: Lsn) -> Self {
        Self {
            kind: LayerKind::Image,
            min_key,
            max_key,
            min_lsn: lsn,
            max_lsn: lsn,
            size: 0,
            owner: None,
            upto: None,
            home: None,
        }
    }

    /// The descriptor a footer describes: how flush and compaction
    /// name what they just built, so names and footers always agree.
    pub fn from_footer(footer: &crate::layer::LayerFooter, size: u64) -> Self {
        Self {
            kind: footer.kind,
            min_key: footer.min_key,
            max_key: footer.max_key,
            min_lsn: footer.min_lsn,
            max_lsn: footer.max_lsn,
            size,
            owner: None,
            upto: None,
            home: None,
        }
    }

    pub fn covers(&self, key: &LayerKey) -> bool {
        self.min_key <= *key && *key <= self.max_key
    }

    /// The newest lsn this layer may serve for a read at `lsn`: the
    /// read lsn itself, tightened by the branch cut on an inherited
    /// layer.
    pub fn clamp(&self, lsn: Lsn) -> Lsn {
        self.upto.map_or(lsn, |u| u.min(lsn))
    }

    /// The object name, derived from the ranges and format frozen:
    /// `d-<keymin>-<keymax>-<lsnmin>-<lsnmax>.dl` for a delta,
    /// `i-<keymin>-<keymax>-<lsn>.il` for an image, keys in the fixed
    /// width hex of [`LayerKey::hex`], lsns as sixteen hex digits.
    pub fn name(&self) -> String {
        match self.kind {
            LayerKind::Delta => format!(
                "d-{}-{}-{:016x}-{:016x}.dl",
                self.min_key.hex(),
                self.max_key.hex(),
                self.min_lsn.0,
                self.max_lsn.0
            ),
            LayerKind::Image => format!(
                "i-{}-{}-{:016x}.il",
                self.min_key.hex(),
                self.max_key.hex(),
                self.min_lsn.0
            ),
        }
    }

    /// Parse a layer name back into a descriptor with the given size.
    pub fn parse(name: &str, size: u64) -> Result<Self, LayerNameError> {
        let malformed = || LayerNameError::Malformed(name.to_string());
        // The inverse of [`LayerKey::hex`]: fixed width hex fields in
        // key order.
        let key = |s: &str| -> Result<LayerKey, LayerNameError> {
            if s.len() != KEY_ENCODED_LEN * 2 || !s.is_ascii() {
                return Err(malformed());
            }
            let field =
                |lo: usize, hi: usize| u32::from_str_radix(&s[lo..hi], 16).map_err(|_| malformed());
            Ok(LayerKey {
                kind: field(0, 2)? as u8,
                spc: field(2, 10)?,
                db: field(10, 18)?,
                rel: field(18, 26)?,
                fork: field(26, 28)? as u8,
                block: field(28, 36)?,
            })
        };
        let lsn = |s: &str| -> Result<Lsn, LayerNameError> {
            if s.len() != 16 {
                return Err(malformed());
            }
            u64::from_str_radix(s, 16).map(Lsn).map_err(|_| malformed())
        };
        let (kind, rest) = if let Some(rest) = name.strip_prefix("d-") {
            (
                LayerKind::Delta,
                rest.strip_suffix(".dl").ok_or_else(malformed)?,
            )
        } else if let Some(rest) = name.strip_prefix("i-") {
            (
                LayerKind::Image,
                rest.strip_suffix(".il").ok_or_else(malformed)?,
            )
        } else {
            return Err(malformed());
        };
        let parts: Vec<&str> = rest.split('-').collect();
        let desc = match (kind, parts.as_slice()) {
            (LayerKind::Delta, [kmin, kmax, lmin, lmax]) => LayerDesc {
                kind,
                min_key: key(kmin)?,
                max_key: key(kmax)?,
                min_lsn: lsn(lmin)?,
                max_lsn: lsn(lmax)?,
                size,
                owner: None,
                upto: None,
                home: None,
            },
            (LayerKind::Image, [kmin, kmax, l]) => {
                let at = lsn(l)?;
                LayerDesc {
                    kind,
                    min_key: key(kmin)?,
                    max_key: key(kmax)?,
                    min_lsn: at,
                    max_lsn: at,
                    size,
                    owner: None,
                    upto: None,
                    home: None,
                }
            }
            _ => return Err(malformed()),
        };
        Ok(desc)
    }
}

/// What one lookup may touch: every image layer covering the key at
/// or below the read lsn, newest first, then every delta layer that
/// can hold records for the key at or below the read lsn, ascending
/// by lsn range. No image at all means the key's first record must
/// initialize the page, which Postgres full page images and init
/// records do.
///
/// The base is the newest image that actually holds the key, which is
/// not always the newest image covering it. An image layer's key
/// range is the span of what it holds, not a claim to hold all of it:
/// compaction materializes only the pages redo can rebuild from what
/// the store has, so an image cut over a busy shard has interior gaps
/// where a page's base lives somewhere else. Only the reader, footer
/// in hand, can tell a gap from a hit, so the map hands over the
/// candidates and [`ReadPlan::above`] takes the floor it settled on.
#[derive(Debug, PartialEq, Eq)]
pub struct ReadPlan<'a> {
    pub images: Vec<&'a LayerDesc>,
    deltas: Vec<&'a LayerDesc>,
}

impl<'a> ReadPlan<'a> {
    /// The delta layers that can hold records above `floor`, in apply
    /// order. `floor` is the lsn of the image the base came from, or
    /// zero when no image held the key: records at or below it are
    /// already folded into the base and must not be applied again.
    pub fn above(&self, floor: Lsn) -> impl Iterator<Item = &'a LayerDesc> {
        self.deltas
            .iter()
            .copied()
            .filter(move |l| l.max_lsn > floor && l.upto.is_none_or(|u| u > floor))
    }

    /// The worst case layer count for this lookup, before the floor
    /// takes anything off. Compaction aims at five layers or less,
    /// and the map reports the count so the aim is measured rather
    /// than assumed;
    /// [`super::pageread::Reconstruction::layers_touched`] reports
    /// what the read actually paid.
    ///
    /// Every image in the plan counts, not one. Images are sparse:
    /// a fold images the keys it saw and leaves the rest where they
    /// are, so a key nobody has written for a while is in none of the
    /// recent images and [`super::pageread::LayerReader::reconstruct`]
    /// probes them newest first until one carries it. Counting a
    /// single image said the amp was one while a read walked twenty,
    /// which is a gauge that goes green as the store gets slower.
    pub fn read_amp(&self) -> usize {
        self.deltas.len() + self.images.len()
    }
}

/// An interval tree node over the key space. Layers overlapping the
/// center key live here, sorted both ways for one sided stabs; the
/// rest split cleanly left and right.
struct Node {
    center: LayerKey,
    /// Overlapping the center, ascending by min_key.
    by_min: Vec<usize>,
    /// The same layers, descending by max_key.
    by_max: Vec<usize>,
    left: Option<Box<Node>>,
    right: Option<Box<Node>>,
}

/// The in-memory index over one shard's layers, built from the
/// manifest layer list, immutable between manifest versions. Flush and
/// compaction publish a new layer list and the reader swaps in a new
/// map.
pub struct LayerMap {
    layers: Vec<LayerDesc>,
    images: Option<Box<Node>>,
    deltas: Option<Box<Node>>,
}

fn build(layers: &[LayerDesc], mut idx: Vec<usize>) -> Option<Box<Node>> {
    if idx.is_empty() {
        return None;
    }
    // The median of the min keys keeps splits balanced enough; layers
    // are key partitioned by compaction so heavy nesting is rare.
    idx.sort_by_key(|&i| layers[i].min_key);
    let center = layers[idx[idx.len() / 2]].min_key;
    let mut left = Vec::new();
    let mut right = Vec::new();
    let mut here = Vec::new();
    for i in idx {
        if layers[i].max_key < center {
            left.push(i);
        } else if layers[i].min_key > center {
            right.push(i);
        } else {
            here.push(i);
        }
    }
    let mut by_min = here.clone();
    by_min.sort_by_key(|&i| layers[i].min_key);
    let mut by_max = here;
    by_max.sort_by_key(|&i| std::cmp::Reverse(layers[i].max_key));
    Some(Box::new(Node {
        center,
        by_min,
        by_max,
        left: build(layers, left),
        right: build(layers, right),
    }))
}

fn stab(layers: &[LayerDesc], node: &Option<Box<Node>>, key: &LayerKey, out: &mut Vec<usize>) {
    let Some(node) = node else { return };
    match key.cmp(&node.center) {
        std::cmp::Ordering::Less => {
            for &i in &node.by_min {
                if layers[i].min_key > *key {
                    break;
                }
                out.push(i);
            }
            stab(layers, &node.left, key, out);
        }
        std::cmp::Ordering::Greater => {
            for &i in &node.by_max {
                if layers[i].max_key < *key {
                    break;
                }
                out.push(i);
            }
            stab(layers, &node.right, key, out);
        }
        std::cmp::Ordering::Equal => out.extend(node.by_min.iter().copied()),
    }
}

impl LayerMap {
    pub fn new(layers: Vec<LayerDesc>) -> Result<Self, LayerMapError> {
        for l in &layers {
            if l.min_key > l.max_key || l.min_lsn > l.max_lsn {
                return Err(LayerMapError::Inverted { name: l.name() });
            }
            if l.kind == LayerKind::Image && l.min_lsn != l.max_lsn {
                return Err(LayerMapError::ImageSpansLsns { name: l.name() });
            }
        }
        let images: Vec<usize> = (0..layers.len())
            .filter(|&i| layers[i].kind == LayerKind::Image)
            .collect();
        let deltas: Vec<usize> = (0..layers.len())
            .filter(|&i| layers[i].kind == LayerKind::Delta)
            .collect();
        let images = build(&layers, images);
        let deltas = build(&layers, deltas);
        Ok(Self {
            layers,
            images,
            deltas,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.layers.is_empty()
    }

    pub fn layers(&self) -> &[LayerDesc] {
        &self.layers
    }

    /// The read algorithm for `(key, lsn)`. Candidate bases are every
    /// covering image at or below the lsn, newest first, and the
    /// reader takes the first that holds the key. Deltas are every
    /// covering layer at or below the lsn, ascending by (min_lsn,
    /// max_lsn) so their records apply in order after a merge on the
    /// actual record lsns; [`ReadPlan::above`] drops the ones the base
    /// already covers. An inherited layer plans against `min(lsn,
    /// upto)`: the branch cut caps what it may serve.
    pub fn plan(&self, key: &LayerKey, lsn: Lsn) -> ReadPlan<'_> {
        let mut hits = Vec::new();
        stab(&self.layers, &self.images, key, &mut hits);
        let mut images: Vec<&LayerDesc> = hits
            .iter()
            .map(|&i| &self.layers[i])
            .filter(|l| l.min_lsn <= l.clamp(lsn))
            .collect();
        images.sort_by_key(|l| std::cmp::Reverse(l.min_lsn));

        hits.clear();
        stab(&self.layers, &self.deltas, key, &mut hits);
        let mut deltas: Vec<&LayerDesc> = hits
            .iter()
            .map(|&i| &self.layers[i])
            .filter(|l| l.min_lsn <= l.clamp(lsn))
            .collect();
        deltas.sort_by_key(|l| (l.min_lsn, l.max_lsn));
        ReadPlan { images, deltas }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(block: u32) -> LayerKey {
        LayerKey::page(1663, 5, 16384, 0, block)
    }

    #[test]
    fn names_round_trip_and_sort_by_kind_then_key() {
        let d = LayerDesc::delta(k(0), k(99), Lsn(0x100), Lsn(0x2fff));
        let i = LayerDesc::image(k(0), k(99), Lsn(0x100));
        for l in [&d, &i] {
            let mut sized = LayerDesc::parse(&l.name(), 7).unwrap();
            assert_eq!(sized.size, 7);
            sized.size = l.size;
            assert_eq!(&sized, l);
        }
        assert!(d.name().ends_with(".dl"));
        assert!(i.name().ends_with(".il"));
        assert_eq!(
            LayerDesc::parse("garbage", 0),
            Err(LayerNameError::Malformed("garbage".into()))
        );
        assert!(LayerDesc::parse(&d.name().replace(".dl", ".il"), 0).is_err());
    }

    #[test]
    fn the_plan_is_the_newest_image_and_the_deltas_above_it() {
        let map = LayerMap::new(vec![
            LayerDesc::image(k(0), k(999), Lsn(100)),
            LayerDesc::image(k(0), k(999), Lsn(500)),
            LayerDesc::delta(k(0), k(999), Lsn(101), Lsn(500)),
            LayerDesc::delta(k(0), k(999), Lsn(501), Lsn(800)),
            LayerDesc::delta(k(0), k(999), Lsn(801), Lsn(900)),
        ])
        .unwrap();
        let plan = map.plan(&k(5), Lsn(850));
        let images: Vec<Lsn> = plan.images.iter().map(|i| i.min_lsn).collect();
        assert_eq!(images, vec![Lsn(500), Lsn(100)], "newest candidate first");
        let lsns: Vec<Lsn> = plan.above(Lsn(500)).map(|d| d.min_lsn).collect();
        assert_eq!(lsns, vec![Lsn(501), Lsn(801)]);

        // A reader that found no base in the newer image, because the
        // image has a gap where this key would be, keeps the whole
        // delta run instead of the two layers above the gap.
        let lsns: Vec<Lsn> = plan.above(Lsn(0)).map(|d| d.min_lsn).collect();
        assert_eq!(lsns, vec![Lsn(101), Lsn(501), Lsn(801)]);

        // Reading before the newer image sees only the older one and
        // the delta that spans the gap.
        let plan = map.plan(&k(5), Lsn(300));
        let images: Vec<Lsn> = plan.images.iter().map(|i| i.min_lsn).collect();
        assert_eq!(images, vec![Lsn(100)]);
        let above: Vec<Lsn> = plan.above(Lsn(100)).map(|d| d.min_lsn).collect();
        assert_eq!(above, vec![Lsn(101)]);

        // Reading below every image has no base and every delta that
        // starts at or below the lsn.
        let plan = map.plan(&k(5), Lsn(99));
        assert!(plan.images.is_empty());
        assert_eq!(plan.above(Lsn(0)).count(), 0);
    }

    #[test]
    fn key_ranges_exclude_layers_that_cannot_hold_the_key() {
        let map = LayerMap::new(vec![
            LayerDesc::image(k(0), k(99), Lsn(100)),
            LayerDesc::image(k(100), k(199), Lsn(100)),
            LayerDesc::delta(k(0), k(99), Lsn(101), Lsn(200)),
            LayerDesc::delta(k(100), k(199), Lsn(101), Lsn(200)),
            LayerDesc::delta(k(50), k(149), Lsn(201), Lsn(300)),
        ])
        .unwrap();
        let plan = map.plan(&k(120), Lsn(300));
        assert_eq!(plan.images[0].min_key, k(100));
        let deltas: Vec<&LayerDesc> = plan.above(Lsn(100)).collect();
        assert_eq!(deltas.len(), 2);
        assert!(deltas.iter().all(|d| d.covers(&k(120))));
        let plan = map.plan(&k(10), Lsn(300));
        assert_eq!(plan.images[0].max_key, k(99));
        let deltas: Vec<&LayerDesc> = plan.above(Lsn(100)).collect();
        assert_eq!(deltas.len(), 1, "the straddling delta misses key 10");
        assert_eq!(deltas[0].min_key, k(0));
        // A key outside every layer plans nothing.
        let plan = map.plan(&k(5000), Lsn(300));
        assert!(plan.images.is_empty() && plan.above(Lsn(0)).count() == 0);
        assert_eq!(plan.read_amp(), 0);
    }

    #[test]
    fn every_image_a_read_may_probe_is_counted() {
        // Twenty folds over the same key range, each one imaging the
        // keys it merged. A key none of the recent ones held sends
        // the reader through all twenty footers before it finds a
        // base, so the amp for that lookup is twenty, not one.
        let mut layers = Vec::new();
        for g in 0..20u64 {
            layers.push(LayerDesc::image(k(0), k(99), Lsn(100 + g * 10)));
        }
        layers.push(LayerDesc::delta(k(0), k(99), Lsn(300), Lsn(400)));
        let map = LayerMap::new(layers).unwrap();
        let plan = map.plan(&k(5), Lsn(500));
        assert_eq!(plan.images.len(), 20);
        assert_eq!(plan.read_amp(), 21);
        // Reading below most of them counts only what that read can
        // reach, so the gauge follows the lsn and not the manifest.
        let plan = map.plan(&k(5), Lsn(150));
        assert_eq!(plan.read_amp(), 6);
    }

    #[test]
    fn a_compacted_layout_stays_inside_the_read_bound() {
        // The shape compaction maintains: key partitioned images with
        // at most four delta generations above any range, across many
        // partitions. Every key at every lsn plans at most 1 + 4.
        let mut layers = Vec::new();
        for part in 0..16u32 {
            let lo = k(part * 100);
            let hi = k(part * 100 + 99);
            layers.push(LayerDesc::image(lo, hi, Lsn(1000)));
            for g in 0..4u64 {
                layers.push(LayerDesc::delta(
                    lo,
                    hi,
                    Lsn(1001 + g * 100),
                    Lsn(1100 + g * 100),
                ));
            }
        }
        let map = LayerMap::new(layers).unwrap();
        for block in (0..1600).step_by(7) {
            for lsn in [1000, 1050, 1150, 1400, 9999] {
                let plan = map.plan(&k(block), Lsn(lsn));
                assert!(
                    plan.read_amp() <= 5,
                    "block {block} at {lsn} touches {}",
                    plan.read_amp()
                );
                assert!(!plan.images.is_empty());
            }
        }
    }

    #[test]
    fn the_tree_agrees_with_a_linear_scan() {
        // Adversarial nesting and overlap; the tree must return
        // exactly what brute force returns, for every probe.
        let mut layers = Vec::new();
        for i in 0..60u32 {
            let lo = (i * 13) % 300;
            let hi = lo + (i * 7) % 120;
            let l1 = 10 + (i as u64 * 17) % 400;
            let l2 = l1 + 1 + (i as u64 * 29) % 200;
            layers.push(LayerDesc::delta(k(lo), k(hi), Lsn(l1), Lsn(l2)));
            if i % 3 == 0 {
                layers.push(LayerDesc::image(k(lo), k(hi), Lsn(l1)));
            }
        }
        let map = LayerMap::new(layers.clone()).unwrap();
        for block in 0..420u32 {
            for lsn in [1u64, 50, 137, 300, 700] {
                let plan = map.plan(&k(block), Lsn(lsn));
                let image = layers
                    .iter()
                    .filter(|l| {
                        l.kind == LayerKind::Image && l.covers(&k(block)) && l.min_lsn <= Lsn(lsn)
                    })
                    .max_by_key(|l| l.min_lsn);
                assert_eq!(
                    plan.images.first().copied(),
                    image,
                    "image for block {block} lsn {lsn}"
                );
                let floor = image.map(|i| i.min_lsn).unwrap_or(Lsn(0));
                let mut deltas: Vec<&LayerDesc> = layers
                    .iter()
                    .filter(|l| {
                        l.kind == LayerKind::Delta
                            && l.covers(&k(block))
                            && l.max_lsn > floor
                            && l.min_lsn <= Lsn(lsn)
                    })
                    .collect();
                deltas.sort_by_key(|l| (l.min_lsn, l.max_lsn));
                assert_eq!(
                    plan.above(floor).collect::<Vec<_>>(),
                    deltas,
                    "deltas for block {block} lsn {lsn}"
                );
            }
        }
    }

    #[test]
    fn an_inherited_layer_plans_against_its_branch_cut() {
        // A child branched at lsn 250: the parent's image and both
        // parent deltas are inherited, the second delta spans the cut.
        let mut image = LayerDesc::image(k(0), k(99), Lsn(100));
        image.owner = Some("p".into());
        let mut d1 = LayerDesc::delta(k(0), k(99), Lsn(101), Lsn(200));
        d1.owner = Some("p".into());
        let mut d2 = LayerDesc::delta(k(0), k(99), Lsn(201), Lsn(400));
        d2.owner = Some("p".into());
        d2.upto = Some(Lsn(250));
        let own = LayerDesc::delta(k(0), k(99), Lsn(251), Lsn(300));
        let map = LayerMap::new(vec![image, d1, d2, own]).unwrap();

        // A read far past the cut still plans the spanning delta, the
        // reader clamps its records at 250, and the child's own delta
        // stacks above it.
        let plan = map.plan(&k(5), Lsn(1000));
        assert_eq!(plan.images[0].min_lsn, Lsn(100));
        let above: Vec<&LayerDesc> = plan.above(Lsn(100)).collect();
        let lsns: Vec<Lsn> = above.iter().map(|d| d.min_lsn).collect();
        assert_eq!(lsns, vec![Lsn(101), Lsn(201), Lsn(251)]);
        assert_eq!(above[1].clamp(Lsn(1000)), Lsn(250));

        // A child image above the cut floors the plan past everything
        // the inherited spanning delta may serve, so it drops out.
        let child_image = LayerDesc::image(k(0), k(99), Lsn(300));
        let mut d2 = LayerDesc::delta(k(0), k(99), Lsn(201), Lsn(400));
        d2.owner = Some("p".into());
        d2.upto = Some(Lsn(250));
        let map = LayerMap::new(vec![child_image, d2]).unwrap();
        let plan = map.plan(&k(5), Lsn(1000));
        assert_eq!(plan.images[0].min_lsn, Lsn(300));
        assert_eq!(
            plan.above(Lsn(300)).count(),
            0,
            "a clamped delta below the floor is never fetched"
        );
    }

    #[test]
    fn bad_layers_are_refused() {
        let inverted = LayerDesc::delta(k(9), k(1), Lsn(1), Lsn(2));
        assert!(matches!(
            LayerMap::new(vec![inverted]),
            Err(LayerMapError::Inverted { .. })
        ));
        let mut wide = LayerDesc::image(k(0), k(9), Lsn(5));
        wide.max_lsn = Lsn(9);
        assert!(matches!(
            LayerMap::new(vec![wide]),
            Err(LayerMapError::ImageSpansLsns { .. })
        ));
        assert!(LayerMap::new(Vec::new()).unwrap().is_empty());
    }
}
