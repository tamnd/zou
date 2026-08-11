//! Where the milliseconds before the first row go.
//!
//! A serverless node is judged on the request that arrives when
//! nothing is running, and that request pays for two things it did not
//! ask for: starting the binary, and attaching the project. Both are
//! made of steps, and a total tells you nothing about which step to go
//! and fix, so both are timed step by step and said out loud once.
//!
//! This is deliberately a stopwatch and not a tracing span. The steps
//! it times happen before there is a request to hang a span on, some of
//! them before there is a runtime, and the answer wanted is a line in
//! the log a person can read on a node that has no collector pointed at
//! it.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// When the process reached `main`, which is as early as a program can
/// know about itself. What happened before it, the exec and the dynamic
/// linker, is real and is not visible from in here, so a cold start
/// measured from outside is always a little larger than a total from
/// this and that difference is the point of measuring both.
static ENTRY: OnceLock<Instant> = OnceLock::new();

/// Called first thing in `main`.
pub fn entered() {
    let _ = ENTRY.set(Instant::now());
}

/// A stopwatch with named laps. Each lap is the time since the last
/// one, so the laps add up to the total and each names the step that
/// spent it.
pub struct Boot {
    start: Instant,
    last: Instant,
    laps: Vec<(&'static str, Duration)>,
}

impl Boot {
    /// A stopwatch running from now.
    pub fn now() -> Boot {
        let start = Instant::now();
        Boot {
            start,
            last: start,
            laps: Vec::new(),
        }
    }

    /// A stopwatch running from the top of `main`, for the part of a
    /// cold start that is the program starting rather than the work it
    /// was started to do.
    pub fn from_entry() -> Boot {
        let start = *ENTRY.get().unwrap_or(&Instant::now());
        Boot {
            start,
            last: Instant::now(),
            laps: Vec::new(),
        }
    }

    /// Close the step that has been running and name it.
    pub fn lap(&mut self, what: &'static str) {
        let now = Instant::now();
        self.laps
            .push((what, now.saturating_duration_since(self.last)));
        self.last = now;
    }

    pub fn total(&self) -> Duration {
        self.last.saturating_duration_since(self.start)
    }

    /// The laps as prose: `store 12.1 ms, doors 2.0 ms`. A lap that
    /// rounds to nothing is still printed, because a step that is
    /// missing from a breakdown reads as a step that was skipped.
    pub fn laps(&self) -> String {
        self.laps
            .iter()
            .map(|(what, took)| format!("{what} {}", ms(*took)))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// A duration the way a person reads one, which below a second means
/// milliseconds with one decimal and above it means seconds.
pub fn ms(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs >= 1.0 {
        format!("{secs:.2} s")
    } else {
        format!("{:.1} ms", secs * 1000.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn laps_add_up_to_the_total() {
        let mut boot = Boot::now();
        std::thread::sleep(Duration::from_millis(5));
        boot.lap("first");
        std::thread::sleep(Duration::from_millis(5));
        boot.lap("second");
        let summed: Duration = boot.laps.iter().map(|(_, took)| *took).sum();
        assert!(boot.total() >= Duration::from_millis(10));
        // The laps are the total, give or take the clock reads between
        // them, which is what makes a breakdown worth reading.
        assert!(boot.total().saturating_sub(summed) < Duration::from_millis(1));
        assert!(boot.laps().starts_with("first "));
        assert!(boot.laps().contains(", second "));
    }

    #[test]
    fn a_duration_reads_the_way_a_person_says_it() {
        assert_eq!(ms(Duration::from_micros(1500)), "1.5 ms");
        assert_eq!(ms(Duration::from_millis(999)), "999.0 ms");
        assert_eq!(ms(Duration::from_millis(1500)), "1.50 s");
    }
}
