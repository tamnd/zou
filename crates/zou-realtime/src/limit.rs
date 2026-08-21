//! What a project is allowed: how many sockets, how many joins a
//! second, how many channels one socket may hold, how many messages a
//! second the whole project may move, how big one of them may be, and
//! how much presence is allowed on top of all that.
//!
//! These are Supabase's tenant limits, the numbers and the refusals
//! both. Upstream keeps them on the tenant row and reads them out of
//! the environment when a tenant is made, which is where the defaults
//! here come from: two hundred sockets, a hundred joins a second, a
//! hundred channels per socket, a hundred messages a second, three
//! megabytes a message, a thousand presence events a second, and five
//! tracks per socket every thirty seconds. They are the shape of a
//! hosted project on the
//! free plan, and a self hosted zou serving one project it owns has
//! nobody to be fair to and may well want them off, which is what a
//! zero means.
//!
//! Three of these can be answered by one socket on its own, so the
//! session answers them: how many channels this socket is on, how big
//! the payload in front of it is, and how often it has tracked its own
//! presence. The rest are about every socket on the project at once,
//! which is not something a session can see, so they arrive as a
//! [`Counters`] the transport implements and the session asks.
//!
//! The counting is upstream's shape too. A rate counter there is a
//! bucket per five seconds, twelve of them kept, and the limit is
//! compared against the average events per second across the window,
//! so a burst is forgiven and a sustained rate is not. The one
//! difference is when the comparison happens: upstream evaluates on the
//! tick and latches the answer until the next one, and this evaluates
//! when it is asked, which refuses a moment sooner and forgives a
//! moment sooner.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// How wide one bucket is, upstream's tick for the join and event
/// counters.
const BUCKET: Duration = Duration::from_secs(5);

/// How many buckets are kept, upstream's max_bucket_len for those two,
/// so the window is a minute.
const BUCKETS: u64 = 12;

/// The numbers, as upstream's tenant row holds them. Zero is off,
/// which is not a value upstream's tenant row can hold: it has no
/// place to say a project is not limited, and a server somebody runs
/// for themselves needs one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// `max_concurrent_users`: how many sockets may be connected at
    /// once. The one over it is refused at the handshake with a 429,
    /// before there is a socket to say anything down.
    pub concurrent_users: u64,
    /// `max_joins_per_second`: how many channel joins the whole
    /// project may make a second, averaged over the last minute.
    pub joins_per_second: u64,
    /// `max_channels_per_client`: how many channels one socket may be
    /// on. This one is a count and not a rate.
    pub channels_per_client: u64,
    /// `max_events_per_second`: how many messages the project may move
    /// a second, which counts what is sent and what is delivered, so
    /// one broadcast to a thousand sockets is a thousand and one.
    pub events_per_second: u64,
    /// `max_payload_size_in_kb`: how big one message may be.
    pub payload_size_kb: u64,
    /// `max_presence_events_per_second`: how many presence events the
    /// project may move a second, counted apart from the messages
    /// budget rather than out of it. A track, an untrack and the diff
    /// that follows one all cost it.
    pub presence_events_per_second: u64,
    /// `max_client_presence_events_per_window`: how many times one
    /// socket may track inside its window. This is the only limit here
    /// that belongs to a connection rather than to a project, so it is
    /// the only one a session can answer without asking anybody.
    pub presence_calls_per_window: u64,
    /// `client_presence_window_ms`: how wide that window is.
    pub presence_window_ms: u64,
}

impl Default for Limits {
    /// Upstream's, which are the `TENANT_MAX_*` defaults a realtime
    /// makes a tenant with and the `max_payload_size_in_kb` its table
    /// carries.
    fn default() -> Limits {
        Limits {
            concurrent_users: 200,
            joins_per_second: 100,
            channels_per_client: 100,
            events_per_second: 100,
            payload_size_kb: 3000,
            presence_events_per_second: 1000,
            presence_calls_per_window: 5,
            presence_window_ms: 30_000,
        }
    }
}

impl Limits {
    /// Nothing counted, which is what a project that is its own
    /// operator usually wants and what an embedded zou takes.
    pub fn none() -> Limits {
        Limits {
            concurrent_users: 0,
            joins_per_second: 0,
            channels_per_client: 0,
            events_per_second: 0,
            payload_size_kb: 0,
            presence_events_per_second: 0,
            presence_calls_per_window: 0,
            presence_window_ms: 30_000,
        }
    }

    /// How many bytes one message may be, which is upstream's
    /// kilobytes and upstream's five hundred bytes of slack on top for
    /// what wraps the payload. None means no limit.
    pub fn payload_bytes(&self) -> Option<u64> {
        match self.payload_size_kb {
            0 => None,
            kb => Some(kb * 1000 + 500),
        }
    }
}

/// The counters one session cannot hold, because they are the whole
/// project's rather than this socket's.
///
/// The session asks and does not count: what a message costs is known
/// where it is fanned, which is the transport, and a session that
/// counted its own sends would miss every delivery.
pub trait Counters: Send + Sync {
    /// One channel is about to be joined. False is over budget, and
    /// nothing is counted for a join that did not happen.
    fn join(&self) -> bool;
    /// Whether the project has moved more messages a second than it is
    /// allowed. Asked by the socket that is about to send one and by
    /// the socket that is about to be handed one, which is where
    /// upstream notices too.
    fn over_events(&self) -> bool;
    /// One presence event is about to happen. False is over budget,
    /// and nothing is counted for one that did not happen.
    ///
    /// This is a join's shape rather than an event's: presence has its
    /// own meter, and the only things that touch it are the three
    /// paths that ask here, so counting and asking are one step. The
    /// messages budget is spent where a message is fanned instead,
    /// which is somewhere a session cannot see.
    fn presence(&self) -> bool;
}

/// Counters for a project with no limits on it, which is what a
/// session built without any gets.
pub struct Unlimited;

impl Counters for Unlimited {
    fn join(&self) -> bool {
        true
    }

    fn over_events(&self) -> bool {
        false
    }

    fn presence(&self) -> bool {
        true
    }
}

/// A rolling count of how often something happens, in upstream's
/// shape: a bucket per five seconds, a minute of them kept, and an
/// average per second over however many have been filled.
#[derive(Debug)]
pub struct Meter {
    /// The buckets, and which five second slot the newest of them is.
    /// One lock rather than an atomic per bucket, because rolling the
    /// window and adding to it have to be one step or a burst on the
    /// boundary lands in a bucket that is about to be dropped.
    counts: Mutex<Window>,
    /// When this meter started, since a bucket is named by how many of
    /// them have gone by since then.
    from: Instant,
}

#[derive(Debug)]
struct Window {
    /// Newest first, so the oldest falls off the end.
    buckets: Vec<u64>,
    /// The slot the front bucket is counting.
    slot: u64,
}

impl Default for Meter {
    fn default() -> Meter {
        Meter::new()
    }
}

impl Meter {
    pub fn new() -> Meter {
        Meter {
            counts: Mutex::new(Window {
                buckets: vec![0],
                slot: 0,
            }),
            from: Instant::now(),
        }
    }

    /// Count `events` of it, now.
    pub fn count(&self, events: u64) {
        self.count_at(events, Instant::now());
    }

    /// The average per second over the window, now.
    pub fn per_second(&self) -> f64 {
        self.per_second_at(Instant::now())
    }

    /// Whether that average is at or over `limit`, which is upstream's
    /// comparison: at the limit is over it. Zero is no limit.
    pub fn over(&self, limit: u64) -> bool {
        match limit {
            0 => false,
            limit => self.per_second() >= limit as f64,
        }
    }

    /// The same three with the clock handed in, which is what the
    /// tests use and what keeps a rate testable without sleeping
    /// through a minute of it.
    pub fn count_at(&self, events: u64, now: Instant) {
        let mut counts = self.counts.lock().expect("the meter");
        self.roll(&mut counts, now);
        counts.buckets[0] += events;
    }

    pub fn per_second_at(&self, now: Instant) -> f64 {
        let mut counts = self.counts.lock().expect("the meter");
        self.roll(&mut counts, now);
        let filled = counts.buckets.len() as f64;
        let sum: u64 = counts.buckets.iter().sum();
        sum as f64 / filled / BUCKET.as_secs_f64()
    }

    pub fn over_at(&self, limit: u64, now: Instant) -> bool {
        match limit {
            0 => false,
            limit => self.per_second_at(now) >= limit as f64,
        }
    }

    /// Bring the window up to `now`: every five second slot that has
    /// gone by since the front bucket is a bucket of its own, and only
    /// the last twelve are kept.
    ///
    /// A window that has been idle for longer than it is wide is
    /// emptied rather than filled with zeroes one at a time, which is
    /// the same answer and does not walk a million buckets after an
    /// idle hour.
    fn roll(&self, counts: &mut Window, now: Instant) {
        let slot = now.saturating_duration_since(self.from).as_secs() / BUCKET.as_secs();
        let gone = slot.saturating_sub(counts.slot);
        if gone == 0 {
            return;
        }
        counts.slot = slot;
        if gone >= BUCKETS {
            counts.buckets = vec![0];
            return;
        }
        for _ in 0..gone {
            counts.buckets.insert(0, 0);
        }
        counts.buckets.truncate(BUCKETS as usize);
    }
}

/// How often one socket has done something, in a window that slides
/// with it rather than in buckets.
///
/// This is not a [`Meter`] and should not be one. A meter answers what
/// a project's sustained rate is, which is a question about a lot of
/// sockets and is deliberately forgiving of a burst. This answers
/// whether one client has called something more than N times in the
/// last W milliseconds, which is a question about a burst and nothing
/// else, and it is upstream's own shape for the one limit that belongs
/// to a connection. Five calls is what it holds, so the memory is the
/// limit rather than the traffic.
#[derive(Debug, Default)]
pub struct Calls {
    /// When each of the last calls happened, oldest first.
    at: Vec<Instant>,
}

impl Calls {
    pub fn new() -> Calls {
        Calls { at: Vec::new() }
    }

    /// Count one call now and say whether it is over the window.
    pub fn over(&mut self, max: u64, window: Duration) -> bool {
        self.over_at(max, window, Instant::now())
    }

    /// The same with the clock handed in. The call is counted whether
    /// or not it is allowed, which is upstream's order and the one that
    /// matters: a client hammering track does not get its window back
    /// by being refused.
    pub fn over_at(&mut self, max: u64, window: Duration, now: Instant) -> bool {
        if max == 0 {
            return false;
        }
        self.at
            .retain(|&at| now.saturating_duration_since(at) < window);
        self.at.push(now);
        self.at.len() as u64 > max
    }
}

/// How many sockets are connected, which is a number and not a rate:
/// upstream counts the live connections of a tenant rather than how
/// fast they arrived.
#[derive(Debug, Default)]
pub struct Sockets(AtomicU64);

impl Sockets {
    /// One more, and how many there are now.
    pub fn joined(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// One fewer.
    pub fn left(&self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn now(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    /// Whether another socket would be one too many. Zero is no limit.
    pub fn full(&self, limit: u64) -> bool {
        match limit {
            0 => false,
            limit => self.now() >= limit,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A rate is what happened over the window and not what happened
    /// in the last second, so a burst inside one bucket is divided by
    /// the whole window rather than counted against it.
    #[test]
    fn a_burst_is_averaged_over_the_window_it_is_in() {
        let meter = Meter::new();
        let start = meter.from;
        meter.count_at(100, start);
        // One bucket has been filled, so the average is over five
        // seconds and not over the minute the window will grow to.
        assert_eq!(meter.per_second_at(start), 20.0);
        // A minute later that same burst is spread over twelve
        // buckets, which is what makes a limit about a sustained rate
        // rather than a moment.
        let later = start + Duration::from_secs(55);
        assert!((meter.per_second_at(later) - 100.0 / 60.0).abs() < 0.001);
    }

    #[test]
    fn a_meter_nobody_has_touched_for_a_minute_is_back_to_nothing() {
        let meter = Meter::new();
        let start = meter.from;
        meter.count_at(600, start);
        assert!(meter.over_at(100, start));
        let idle = start + Duration::from_secs(120);
        assert_eq!(meter.per_second_at(idle), 0.0);
        assert!(!meter.over_at(100, idle));
    }

    /// Upstream trips at the limit rather than past it, and a limit of
    /// zero is not a limit at all.
    #[test]
    fn the_limit_itself_is_over_the_limit() {
        let meter = Meter::new();
        let start = meter.from;
        // Five hundred in one bucket is a hundred a second.
        meter.count_at(500, start);
        assert!(meter.over_at(100, start));
        assert!(!meter.over_at(101, start));
        assert!(!meter.over_at(0, start));
    }

    /// The events that fall off the end of the window stop counting,
    /// which is what lets a project that was over its budget come back
    /// under it without being restarted.
    #[test]
    fn what_falls_out_of_the_window_stops_counting() {
        let meter = Meter::new();
        let start = meter.from;
        for second in 0..60 {
            meter.count_at(500, start + Duration::from_secs(second));
        }
        assert!(meter.over_at(100, start + Duration::from_secs(59)));
        // Nothing for the next minute, and the whole window has rolled
        // out from under it.
        let quiet = start + Duration::from_secs(125);
        assert!(!meter.over_at(100, quiet));
    }

    #[test]
    fn a_payload_budget_is_kilobytes_and_upstreams_slack() {
        let limits = Limits::default();
        assert_eq!(limits.payload_bytes(), Some(3_000_500));
        assert_eq!(Limits::none().payload_bytes(), None);
    }

    /// The per socket window is about a burst, so it counts calls and
    /// not an average: five in thirty seconds is fine and the sixth is
    /// not, however the five were spread.
    #[test]
    fn the_sixth_call_in_the_window_is_over_it() {
        let mut calls = Calls::new();
        let window = Duration::from_millis(30_000);
        let start = Instant::now();
        for i in 0..5 {
            assert!(
                !calls.over_at(5, window, start + Duration::from_millis(i * 100)),
                "call {i} of the five"
            );
        }
        assert!(calls.over_at(5, window, start + Duration::from_millis(500)));
        // And the window slides: once the first five have aged out
        // there is room again.
        assert!(!calls.over_at(5, window, start + Duration::from_millis(31_000)));
    }

    /// A refused call still counts, which is what stops a client from
    /// getting its window back by being told no.
    #[test]
    fn being_refused_does_not_buy_room() {
        let mut calls = Calls::new();
        let window = Duration::from_millis(1000);
        let start = Instant::now();
        for _ in 0..2 {
            calls.over_at(1, window, start);
        }
        assert!(calls.over_at(1, window, start + Duration::from_millis(500)));
        assert!(!calls.over_at(0, window, start), "zero is no limit");
    }

    #[test]
    fn sockets_are_counted_up_and_down() {
        let sockets = Sockets::default();
        assert!(!sockets.full(2));
        assert_eq!(sockets.joined(), 1);
        assert_eq!(sockets.joined(), 2);
        assert!(sockets.full(2));
        // No limit configured is no limit, however many are on.
        assert!(!sockets.full(0));
        sockets.left();
        assert!(!sockets.full(2));
    }
}
