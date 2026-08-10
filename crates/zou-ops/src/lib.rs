//! What an operator gets to see: numbers and lines.
//!
//! Two surfaces, both of them dependency free on purpose. [`metrics`]
//! is a registry of counters, gauges and histograms that renders in the
//! Prometheus text format, which is a format any scraper reads and no
//! client library is needed to write. [`logs`] is the same records the
//! process already emits through the `log` facade, spelled as one json
//! object per line instead of a sentence, for the deployments where
//! something else is doing the reading.
//!
//! This crate holds no policy and starts no threads. It is the vocabulary
//! the rest of the tree instruments itself in, so a counter can be bumped
//! from the storage engine without the engine knowing whether anything is
//! scraping, and the surface that serves those numbers lives where the
//! http server does.

pub mod logs;
pub mod metrics;

pub use metrics::{Counter, Gauge, Histogram, Registry, registry};
