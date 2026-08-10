//! What an operator gets to see: numbers, lines and traces.
//!
//! Three surfaces, all of them dependency light on purpose. [`metrics`]
//! is a registry of counters, gauges and histograms that renders in the
//! Prometheus text format, which is a format any scraper reads and no
//! client library is needed to write. [`logs`] is the same records the
//! process already emits through the `log` facade, spelled as one json
//! object per line instead of a sentence, for the deployments where
//! something else is doing the reading. [`trace`] is W3C trace context
//! and spans over OTLP, which is the half a counter cannot do: which
//! part of a request was slow, and whose request it was.
//!
//! It holds no policy and starts one thread, and only when a collector
//! has been named. It is the vocabulary
//! the rest of the tree instruments itself in, so a counter can be bumped
//! from the storage engine without the engine knowing whether anything is
//! scraping, and the surface that serves those numbers lives where the
//! http server does.

pub mod logs;
pub mod metrics;
pub mod trace;

pub use metrics::{Counter, Gauge, Histogram, Registry, registry};
