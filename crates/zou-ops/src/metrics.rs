//! A metrics registry that renders in the Prometheus text format.
//!
//! Three kinds, which is what the exposition format has: a counter that
//! only goes up, a gauge that goes both ways, and a histogram of
//! observations into fixed buckets. A metric is a name, a help string
//! and a set of labels, and asking for the same three twice hands back
//! the same series, so a call site can look one up or hold one and both
//! mean the same numbers.
//!
//! There is no client library under this, and there does not need to
//! be: the format is line oriented text over http, and writing it is a
//! page of code against a spec that has not changed in a decade. What a
//! library would add here is a dependency tree, a registry macro, and a
//! second opinion about what a metric name is.
//!
//! Names are ours, not input. A name that is not a legal Prometheus
//! name, or a name used for two different kinds, is a bug in this tree
//! and is refused loudly at registration rather than rendered into
//! something a scraper will reject at midnight.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

/// Latency buckets, seconds, from a hundred microseconds to ten
/// seconds. The spread a request path wants: enough resolution under a
/// millisecond to see an in process call, and enough headroom above a
/// second to see the cold attach that is the whole point of watching.
pub const SECONDS: &[f64] = &[
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
    5.0, 10.0,
];

/// Size buckets, bytes, from a kilobyte to a gigabyte. Powers of eight
/// rather than two, because a scrape carries every bucket of every
/// series and thirty of them per histogram is a bill, not a graph.
pub const BYTES: &[f64] = &[
    1024.0,
    8192.0,
    65536.0,
    524_288.0,
    4_194_304.0,
    33_554_432.0,
    268_435_456.0,
    2_147_483_648.0,
];

/// The process registry, which is what everything instruments into
/// unless it is a test.
///
/// A global is the right shape here for the reason it usually is not:
/// the alternative is threading a handle from `main` through the http
/// server, the attach manager and the storage engine to reach the one
/// line that counts a PUT, and a parameter that exists only to be
/// passed on is worse than a global that is append only and read by one
/// endpoint.
pub fn registry() -> &'static Registry {
    static PROCESS: OnceLock<Registry> = OnceLock::new();
    PROCESS.get_or_init(Registry::new)
}

/// A set of metric families, rendered together.
#[derive(Default)]
pub struct Registry {
    families: Mutex<BTreeMap<&'static str, Family>>,
}

struct Family {
    help: &'static str,
    kind: Kind,
    series: BTreeMap<Vec<(String, String)>, Arc<Series>>,
}

#[derive(PartialEq, Eq)]
enum Kind {
    Counter,
    Gauge,
    Histogram(Vec<u64>),
}

impl Kind {
    fn name(&self) -> &'static str {
        match self {
            Kind::Counter => "counter",
            Kind::Gauge => "gauge",
            Kind::Histogram(_) => "histogram",
        }
    }
}

/// One labelled line of a family, or one histogram's worth of them.
///
/// `value` is the count or the level. A histogram leaves it as the
/// observation count and fills `sum` and `buckets` as well. `sum` holds
/// the bits of an f64 because an observation is a float and there is no
/// atomic for one, which is a compare and swap loop and nothing more.
struct Series {
    value: AtomicU64,
    sum: AtomicU64,
    buckets: Vec<AtomicU64>,
}

impl Series {
    fn new(width: usize) -> Series {
        Series {
            value: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            buckets: (0..width).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    fn add_to_sum(&self, v: f64) {
        let mut seen = self.sum.load(Ordering::Relaxed);
        loop {
            let next = (f64::from_bits(seen) + v).to_bits();
            match self
                .sum
                .compare_exchange_weak(seen, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(now) => seen = now,
            }
        }
    }
}

/// A number that only goes up: requests served, bytes written, attaches
/// attempted. A rate over it is the useful reading, which is why it is
/// never reset.
#[derive(Clone)]
pub struct Counter(Arc<Series>);

impl Counter {
    pub fn inc(&self) {
        self.add(1);
    }

    pub fn add(&self, n: u64) {
        self.0.value.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.0.value.load(Ordering::Relaxed)
    }
}

/// A level: tenants attached, connections open, bytes cached. Unsigned,
/// because every level in this tree is a count of things that exist and
/// a negative one of those is a bug worth clamping rather than
/// reporting.
#[derive(Clone)]
pub struct Gauge(Arc<Series>);

impl Gauge {
    pub fn set(&self, n: u64) {
        self.0.value.store(n, Ordering::Relaxed);
    }

    pub fn inc(&self) {
        self.0.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec(&self) {
        let _ = self
            .0
            .value
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(1))
            });
    }

    pub fn get(&self) -> u64 {
        self.0.value.load(Ordering::Relaxed)
    }
}

/// Observations counted into buckets, plus their sum and their count,
/// which is what a quantile is estimated from at query time.
///
/// The buckets belong to the family and are shared by every series in
/// it, so two label sets of one histogram are comparable, which is the
/// only reason to put them in one family at all.
#[derive(Clone)]
pub struct Histogram {
    series: Arc<Series>,
    edges: Arc<Vec<f64>>,
}

impl Histogram {
    /// Count one observation. Buckets are cumulative in the format, not
    /// in memory: what is stored is the count that fell in each band,
    /// and the running total is done at render, because a scrape is
    /// once every fifteen seconds and an observation can be every
    /// microsecond.
    pub fn observe(&self, v: f64) {
        self.observe_count(v, 1);
    }

    /// Count `n` observations of `v` at once, which is how a histogram
    /// that was counted somewhere else is folded in: the store's own
    /// counter file keeps power of two buckets shared by several
    /// processes, and a scrape reads them rather than recounting.
    ///
    /// The sum is charged at `v` for each, so folding a bucket in at
    /// its upper bound makes `_sum` a ceiling rather than an exact
    /// total. The buckets themselves stay exact, which is what a
    /// quantile is read from.
    pub fn observe_count(&self, v: f64, n: u64) {
        if n == 0 {
            return;
        }
        let at = self.edges.partition_point(|edge| *edge < v);
        self.series.buckets[at].fetch_add(n, Ordering::Relaxed);
        self.series.value.fetch_add(n, Ordering::Relaxed);
        self.series.add_to_sum(v * n as f64);
    }

    /// Count the time since `start`, in seconds. The spelling every
    /// latency call site wants, so that none of them has to remember
    /// which unit the buckets are in.
    pub fn since(&self, start: Instant) {
        self.observe(start.elapsed().as_secs_f64());
    }

    pub fn count(&self) -> u64 {
        self.series.value.load(Ordering::Relaxed)
    }

    pub fn sum(&self) -> f64 {
        f64::from_bits(self.series.sum.load(Ordering::Relaxed))
    }
}

impl Registry {
    pub fn new() -> Registry {
        Registry::default()
    }

    /// The counter for this name and these labels, made if it is the
    /// first time anyone asked.
    ///
    /// # Panics
    ///
    /// If the name or a label name is not a legal Prometheus name, or
    /// if the name is already a gauge or a histogram. Both are bugs in
    /// this tree rather than anything a request can cause.
    pub fn counter(
        &self,
        name: &'static str,
        help: &'static str,
        labels: &[(&str, &str)],
    ) -> Counter {
        Counter(self.series(name, help, labels, Kind::Counter, 0))
    }

    /// The gauge for this name and these labels. Panics on the same two
    /// things [`Registry::counter`] does.
    pub fn gauge(&self, name: &'static str, help: &'static str, labels: &[(&str, &str)]) -> Gauge {
        Gauge(self.series(name, help, labels, Kind::Gauge, 0))
    }

    /// The histogram for this name and these labels, over `edges`.
    ///
    /// # Panics
    ///
    /// On the same two things [`Registry::counter`] does, and if the
    /// edges are not sorted, or are empty, or disagree with the edges
    /// this name was first registered with, since two series of one
    /// family that count into different bands cannot be added up.
    pub fn histogram(
        &self,
        name: &'static str,
        help: &'static str,
        edges: &[f64],
        labels: &[(&str, &str)],
    ) -> Histogram {
        assert!(!edges.is_empty(), "histogram {name} has no buckets");
        assert!(
            edges.windows(2).all(|pair| pair[0] < pair[1]),
            "histogram {name} has buckets that are not in ascending order"
        );
        let kind = Kind::Histogram(edges.iter().map(|e| e.to_bits()).collect());
        // One more bucket than there are edges: everything above the
        // last edge, which the format calls +Inf.
        let series = self.series(name, help, labels, kind, edges.len() + 1);
        Histogram {
            series,
            edges: Arc::new(edges.to_vec()),
        }
    }

    fn series(
        &self,
        name: &'static str,
        help: &'static str,
        labels: &[(&str, &str)],
        kind: Kind,
        width: usize,
    ) -> Arc<Series> {
        assert!(legal(name), "{name} is not a legal metric name");
        for (label, _) in labels {
            assert!(legal(label), "{label} is not a legal label name");
        }
        let key: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        let mut families = self.families.lock().expect("the registry lock");
        match families.get(name) {
            Some(family) => {
                assert!(
                    family.kind.name() == kind.name(),
                    "metric {name} is a {} and cannot also be a {}",
                    family.kind.name(),
                    kind.name()
                );
                assert!(
                    family.kind == kind,
                    "histogram {name} is already registered with different buckets"
                );
            }
            None => {
                families.insert(
                    name,
                    Family {
                        help,
                        kind,
                        series: BTreeMap::new(),
                    },
                );
            }
        }
        let family = families.get_mut(name).expect("it is there either way");
        Arc::clone(
            family
                .series
                .entry(key)
                .or_insert_with(|| Arc::new(Series::new(width))),
        )
    }

    /// Every family this registry holds, in the Prometheus text
    /// exposition format, name ordered so that two scrapes of an
    /// unchanged process differ only in their numbers.
    pub fn render(&self) -> String {
        let families = self.families.lock().expect("the registry lock");
        let mut out = String::new();
        for (name, family) in families.iter() {
            out.push_str(&format!(
                "# HELP {name} {}\n",
                family.help.replace('\n', " ")
            ));
            out.push_str(&format!("# TYPE {name} {}\n", family.kind.name()));
            for (labels, series) in &family.series {
                match &family.kind {
                    Kind::Counter | Kind::Gauge => {
                        out.push_str(&format!(
                            "{name}{} {}\n",
                            tags(labels, None),
                            series.value.load(Ordering::Relaxed)
                        ));
                    }
                    Kind::Histogram(edges) => {
                        let mut running = 0u64;
                        for (at, edge) in edges.iter().enumerate() {
                            running += series.buckets[at].load(Ordering::Relaxed);
                            let le = number(f64::from_bits(*edge));
                            out.push_str(&format!(
                                "{name}_bucket{} {running}\n",
                                tags(labels, Some(&le))
                            ));
                        }
                        running += series.buckets[edges.len()].load(Ordering::Relaxed);
                        out.push_str(&format!(
                            "{name}_bucket{} {running}\n",
                            tags(labels, Some("+Inf"))
                        ));
                        out.push_str(&format!(
                            "{name}_sum{} {}\n",
                            tags(labels, None),
                            number(f64::from_bits(series.sum.load(Ordering::Relaxed)))
                        ));
                        out.push_str(&format!("{name}_count{} {running}\n", tags(labels, None)));
                    }
                }
            }
        }
        out
    }
}

/// `{a="1",b="2"}`, or nothing when there are no labels, with `le` on
/// the end when it is a histogram bucket. Empty rather than `{}`
/// because both are legal and the shorter one is what every exporter
/// writes.
fn tags(labels: &[(String, String)], le: Option<&str>) -> String {
    if labels.is_empty() && le.is_none() {
        return String::new();
    }
    let mut out = String::from("{");
    for (at, (key, value)) in labels.iter().enumerate() {
        if at > 0 {
            out.push(',');
        }
        out.push_str(&format!("{key}=\"{}\"", escape(value)));
    }
    if let Some(le) = le {
        if !labels.is_empty() {
            out.push(',');
        }
        out.push_str(&format!("le=\"{le}\""));
    }
    out.push('}');
    out
}

/// A label value is arbitrary text, and the three characters the format
/// cannot carry raw are the three escaped here.
fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// A float the way the format wants it: no exponent for anything a
/// bucket edge is, and no trailing zeroes to make a diff noisy.
fn number(v: f64) -> String {
    if v == v.trunc() && v.abs() < 1e15 {
        return format!("{v:.0}");
    }
    let mut out = format!("{v:.9}");
    while out.ends_with('0') {
        out.pop();
    }
    if out.ends_with('.') {
        out.pop();
    }
    out
}

/// The format's own rule for a name: a letter, an underscore or a
/// colon, then those and digits.
fn legal(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_' || first == ':') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_counter_renders_as_one_line() {
        let reg = Registry::new();
        let hits = reg.counter("zou_hits_total", "how many", &[]);
        hits.inc();
        hits.add(4);
        assert_eq!(
            reg.render(),
            "# HELP zou_hits_total how many\n# TYPE zou_hits_total counter\nzou_hits_total 5\n"
        );
    }

    #[test]
    fn the_same_name_and_labels_are_the_same_series() {
        let reg = Registry::new();
        reg.counter("zou_hits_total", "how many", &[("op", "get")])
            .inc();
        let again = reg.counter("zou_hits_total", "how many", &[("op", "get")]);
        assert_eq!(again.get(), 1, "a second lookup is the same numbers");
        again.inc();
        assert!(reg.render().contains("zou_hits_total{op=\"get\"} 2\n"));
    }

    #[test]
    fn different_labels_are_different_lines_of_one_family() {
        let reg = Registry::new();
        reg.counter("zou_ops_total", "store ops", &[("op", "put")])
            .add(2);
        reg.counter("zou_ops_total", "store ops", &[("op", "get")])
            .add(7);
        let out = reg.render();
        assert_eq!(out.matches("# TYPE").count(), 1, "one family: {out}");
        // Sorted, so a scrape of an unchanged process is byte stable.
        let get = out.find("op=\"get\"").expect("the get line");
        let put = out.find("op=\"put\"").expect("the put line");
        assert!(get < put, "{out}");
    }

    #[test]
    fn a_gauge_goes_both_ways_and_stops_at_zero() {
        let reg = Registry::new();
        let up = reg.gauge("zou_attached", "tenants up", &[]);
        up.inc();
        up.inc();
        up.dec();
        assert_eq!(up.get(), 1);
        up.dec();
        up.dec();
        assert_eq!(up.get(), 0, "a level of minus one is a bug, not a reading");
        up.set(9);
        assert!(reg.render().contains("zou_attached 9\n"));
    }

    #[test]
    fn a_histogram_renders_cumulative_buckets() {
        let reg = Registry::new();
        let ms = reg.histogram("zou_wait_seconds", "waits", &[0.01, 0.1, 1.0], &[]);
        ms.observe(0.005);
        ms.observe(0.5);
        ms.observe(30.0);
        let out = reg.render();
        assert!(out.contains("# TYPE zou_wait_seconds histogram\n"), "{out}");
        assert!(
            out.contains("zou_wait_seconds_bucket{le=\"0.01\"} 1\n"),
            "{out}"
        );
        assert!(
            out.contains("zou_wait_seconds_bucket{le=\"0.1\"} 1\n"),
            "{out}"
        );
        assert!(
            out.contains("zou_wait_seconds_bucket{le=\"1\"} 2\n"),
            "{out}"
        );
        assert!(
            out.contains("zou_wait_seconds_bucket{le=\"+Inf\"} 3\n"),
            "{out}"
        );
        assert!(out.contains("zou_wait_seconds_sum 30.505\n"), "{out}");
        assert!(out.contains("zou_wait_seconds_count 3\n"), "{out}");
    }

    #[test]
    fn an_observation_on_a_bucket_edge_falls_inside_it() {
        // le means less than or equal, and an off by one here is the
        // kind of thing nobody notices until a p99 is a lie.
        let reg = Registry::new();
        let h = reg.histogram("zou_edge_seconds", "edges", &[0.1], &[]);
        h.observe(0.1);
        assert!(
            reg.render()
                .contains("zou_edge_seconds_bucket{le=\"0.1\"} 1\n"),
            "{}",
            reg.render()
        );
    }

    #[test]
    fn a_histogram_with_labels_keeps_le_last() {
        let reg = Registry::new();
        reg.histogram("zou_call_seconds", "calls", &[1.0], &[("surface", "rest")])
            .observe(0.5);
        let out = reg.render();
        assert!(
            out.contains("zou_call_seconds_bucket{surface=\"rest\",le=\"1\"} 1\n"),
            "{out}"
        );
        assert!(
            out.contains("zou_call_seconds_count{surface=\"rest\"} 1\n"),
            "{out}"
        );
    }

    #[test]
    fn since_counts_a_duration_in_seconds() {
        let reg = Registry::new();
        let h = reg.histogram("zou_since_seconds", "since", SECONDS, &[]);
        h.since(Instant::now());
        assert_eq!(h.count(), 1);
        assert!(h.sum() < 1.0, "a call to itself does not take a second");
    }

    #[test]
    fn a_label_value_that_would_break_the_format_is_escaped() {
        let reg = Registry::new();
        reg.counter("zou_odd_total", "odd", &[("why", "a \"quote\"")])
            .inc();
        assert!(
            reg.render()
                .contains("zou_odd_total{why=\"a \\\"quote\\\"\"} 1\n"),
            "{}",
            reg.render()
        );
    }

    #[test]
    #[should_panic(expected = "is not a legal metric name")]
    fn a_name_a_scraper_would_reject_is_refused_at_registration() {
        Registry::new().counter("zou-hits-total", "dashes are not names", &[]);
    }

    #[test]
    #[should_panic(expected = "is not a legal label name")]
    fn a_label_name_a_scraper_would_reject_is_refused_too() {
        Registry::new().counter("zou_hits_total", "fine", &[("the op", "get")]);
    }

    #[test]
    #[should_panic(expected = "buckets that are not in ascending order")]
    fn buckets_out_of_order_are_refused() {
        Registry::new().histogram("zou_bad_seconds", "bad", &[1.0, 0.5], &[]);
    }

    #[test]
    #[should_panic(expected = "cannot also be a")]
    fn one_name_cannot_mean_two_kinds() {
        let reg = Registry::new();
        reg.counter("zou_both_total", "a counter", &[]);
        reg.gauge("zou_both_total", "and a gauge", &[]);
    }

    #[test]
    #[should_panic(expected = "different buckets")]
    fn two_series_of_one_histogram_cannot_count_into_different_bands() {
        // Otherwise the family would render buckets that cannot be
        // summed, and a scraper would have no way to know.
        let reg = Registry::new();
        reg.histogram("zou_span_seconds", "spans", &[1.0], &[("op", "get")]);
        reg.histogram("zou_span_seconds", "spans", &[2.0], &[("op", "put")]);
    }

    #[test]
    fn the_process_registry_is_one_registry() {
        registry()
            .counter("zou_process_probe_total", "probe", &[])
            .inc();
        assert_eq!(
            registry()
                .counter("zou_process_probe_total", "probe", &[])
                .get(),
            1
        );
    }
}
