//! What one call may use, and what stops it when it has used more.
//!
//! Three numbers and a fourth, and every one of them is upstream's,
//! read off `supabase/edge-runtime` 1.74.2 with the main worker the
//! pinned CLI 2.111.0 actually ships rather than off the documentation:
//! `memoryLimitMb: 256`, `workerTimeoutMs` of 400 seconds unless the
//! environment says otherwise, `cpuTimeSoftLimitMs: 1000` and
//! `cpuTimeHardLimitMs: 2000`. The fourth is this project's own, the
//! thirty seconds `EdgeRuntime.waitUntil` gets, which lived in the
//! isolate until there was somewhere for it to belong.
//!
//! What a caller is told when one of them is reached was measured the
//! same way, by running that runtime with a function per limit: 546
//! with `{"code":"WORKER_LIMIT","message":"Worker failed to respond due
//! to a resource limit (please check logs)"}` for all three, and the
//! reason in the log rather than in the answer. A limit reached after
//! the head of a streamed answer has already gone out truncates the
//! body instead, because there is no status code left to change.
//!
//! # The three of them are stopped in two different ways
//!
//! Memory is v8's own, and then it is not. The isolate is created with
//! a heap limit, and the callback v8 calls as it approaches that limit
//! is where execution is terminated. Returning a slightly larger limit
//! from it is not generosity, it is what keeps v8 from aborting the
//! process before the termination it was just asked for has had a
//! chance to happen. But an array buffer is not on that heap, and a
//! function pushing mebibyte `Uint8Array`s past a 64 MiB heap limit was
//! measured here reaching a hundred gigabytes without v8 saying a word,
//! so buffers are allocated through an allocator that counts them
//! against the same number. What the count is worth is decided by the
//! watchdog rather than by the allocator, because most of what a
//! function has allocated at any moment is usually rubbish it has
//! finished with: going over asks the isolate for a collection first,
//! and only what is still held after that is a limit reached.
//!
//! Wall clock and cpu are a thread watching a clock, because the thing
//! being watched may be a function that never gives the thread back:
//! `while (true) {}` yields to nothing, so nothing on the isolate's own
//! thread can time it out and only `terminate_execution` from outside
//! ends it. A timer is kept as well as the watchdog, because the other
//! shape of a call that overruns is one that is asleep rather than
//! busy, and terminating execution does not wake a sleeper.
//!
//! # What cpu time means here
//!
//! Not the operating system's. This is the time the isolate spent
//! being polled: the sum of every poll of its event loop, which is v8
//! running javascript plus whatever a synchronous op did while it was
//! there, and which excludes everything the call was waiting for. A
//! `fetch` and a `setTimeout` are both awaits and neither is polled
//! while it waits, so neither is counted, which is the property that
//! makes this worth measuring at all. A thread the operating system
//! descheduled is counted, which the operating system's clock would
//! not do, and that is the difference and it is written down rather
//! than hidden: a busy machine can stop a function slightly earlier
//! than a quiet one would.

use std::ffi::c_void;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use deno_core::v8;

/// What a call may use before something stops it.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// The isolate's v8 heap, in bytes.
    pub memory: usize,
    /// How long the whole call may take, answer and anything after it.
    pub wall: Duration,
    /// How long the call may spend running, as opposed to waiting.
    pub cpu: Duration,
    /// How long work left behind with `EdgeRuntime.waitUntil` may go on
    /// after the caller has been answered.
    pub background: Duration,
}

/// A mebibyte, because upstream's number is in megabytes and this one
/// is in bytes and the difference should be visible.
const MIB: usize = 1024 * 1024;

impl Default for Limits {
    /// Upstream's three, and this project's fourth.
    fn default() -> Limits {
        Limits {
            memory: 256 * MIB,
            wall: Duration::from_secs(400),
            // Upstream's hard limit. Its soft limit of one second is
            // not here, because what upstream does with it is dispatch
            // `beforeunload` into the function at ninety percent of it
            // and this runtime has no such event yet. A function that
            // burns a second and a half and then answers is answered
            // there and is answered here.
            cpu: Duration::from_secs(2),
            background: Duration::from_secs(30),
        }
    }
}

impl Limits {
    /// The memory limit in the unit upstream states it in, for a log
    /// line and for the sentence an operator reads.
    pub fn memory_mib(&self) -> usize {
        self.memory / MIB
    }
}

/// Which limit was reached, which decides only what the log says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reached {
    Memory,
    Wall,
    Cpu,
}

const NOTHING: u8 = 0;
const MEMORY: u8 = 1;
const WALL: u8 = 2;
const CPU: u8 = 3;

/// How often the watchdog looks.
///
/// Ten milliseconds is the granularity of the cpu limit and of a wall
/// clock reached by a function that is busy rather than asleep, and it
/// is one wakeup per hundredth of a second per call in flight. Upstream
/// answered a two second cpu limit in 2.13 seconds on the machine this
/// was measured on, so a hundredth of a second of slack is well inside
/// what the thing being copied does.
const TICK: Duration = Duration::from_millis(10);

/// The clock a call is watched against.
///
/// Shared between the isolate's thread, which is the only one that adds
/// to it, and the watchdog thread, which only ever reads it.
pub(crate) struct Watch {
    base: Instant,
    /// Nanoseconds of polling that has already finished.
    spent: AtomicU64,
    /// Nanoseconds since `base` at which the poll now running began, or
    /// zero when nothing is running.
    entered: AtomicU64,
    hit: AtomicU8,
    /// Bytes of array buffer the function has out, which is not on the
    /// heap v8 was given a limit for.
    buffers: AtomicUsize,
    /// Whether a collection has been asked for and not yet happened.
    collecting: std::sync::atomic::AtomicBool,
    limits: Limits,
}

impl Watch {
    pub(crate) fn new(limits: Limits) -> Arc<Watch> {
        Arc::new(Watch {
            base: Instant::now(),
            spent: AtomicU64::new(0),
            entered: AtomicU64::new(0),
            hit: AtomicU8::new(NOTHING),
            buffers: AtomicUsize::new(0),
            collecting: std::sync::atomic::AtomicBool::new(false),
            limits,
        })
    }

    /// A poll started. Nanoseconds since `base` are stored rather than
    /// the `Instant` itself, because the watchdog reads this without a
    /// lock.
    fn entered(&self) {
        let now = self.base.elapsed().as_nanos() as u64;
        // Zero means nothing is running, so a poll that starts in the
        // same nanosecond the watch did says one instead.
        self.entered.store(now.max(1), Ordering::Release);
    }

    /// A poll finished, and what it took is now part of the total.
    fn left(&self) {
        let started = self.entered.swap(0, Ordering::AcqRel);
        if started == 0 {
            return;
        }
        let now = self.base.elapsed().as_nanos() as u64;
        self.spent
            .fetch_add(now.saturating_sub(started), Ordering::AcqRel);
    }

    /// Time spent running, including the poll that is running now,
    /// which is the whole reason this is readable from another thread.
    pub(crate) fn cpu(&self) -> Duration {
        let spent = self.spent.load(Ordering::Acquire);
        let running = match self.entered.load(Ordering::Acquire) {
            0 => 0,
            started => (self.base.elapsed().as_nanos() as u64).saturating_sub(started),
        };
        Duration::from_nanos(spent.saturating_add(running))
    }

    /// Buffer bytes handed out, and the total afterwards.
    fn took(&self, len: usize) -> usize {
        self.buffers.fetch_add(len, Ordering::AcqRel) + len
    }

    fn gave(&self, len: usize) {
        self.buffers.fetch_sub(len, Ordering::AcqRel);
    }

    /// Buffer bytes the function is holding, rubbish included.
    pub(crate) fn buffered(&self) -> usize {
        self.buffers.load(Ordering::Acquire)
    }

    /// One collection is asked for at a time, because a full one is
    /// expensive and a function churning buffers would otherwise be
    /// asked for another every tick.
    fn wants_collection(&self) -> bool {
        self.collecting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn collected(&self) {
        self.collecting.store(false, Ordering::Release);
    }

    /// Which limit stopped this call, if one did.
    pub(crate) fn reached(&self) -> Option<Reached> {
        match self.hit.load(Ordering::Acquire) {
            MEMORY => Some(Reached::Memory),
            WALL => Some(Reached::Wall),
            CPU => Some(Reached::Cpu),
            _ => None,
        }
    }

    /// The first limit reached is the one reported, because after the
    /// first one everything else about the call is a consequence.
    fn record(&self, what: u8) -> bool {
        self.hit
            .compare_exchange(NOTHING, what, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// What the operator is told, which names the limit and the number
    /// it was.
    pub(crate) fn sentence(&self, what: Reached) -> String {
        match what {
            Reached::Memory => format!(
                "it wanted more memory than the {} MiB it is allowed",
                self.limits.memory_mib()
            ),
            Reached::Wall => format!(
                "it was still running after the {:?} it is allowed",
                self.limits.wall
            ),
            Reached::Cpu => format!(
                "it spent more than the {:?} of cpu time it is allowed",
                self.limits.cpu
            ),
        }
    }

    /// Count the time until the guard is dropped, which is what a call
    /// into v8 that is not a future needs: `call` and `mod_evaluate`
    /// run the synchronous half of a handler before there is any future
    /// to poll, and a handler that never comes back never comes back
    /// from there.
    pub(crate) fn running(&self) -> Running<'_> {
        self.entered();
        Running(self)
    }

    /// Run `fut` and count the time it spends being polled.
    pub(crate) async fn timing<F: std::future::Future>(self: &Arc<Watch>, fut: F) -> F::Output {
        Timed {
            watch: Arc::clone(self),
            inner: fut,
        }
        .await
    }
}

/// A call into v8 that is not a future, counted the same way.
pub(crate) struct Running<'a>(&'a Watch);

impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.0.left();
    }
}

/// A future that says when it is running, so that something else can
/// notice it has been running for too long.
struct Timed<F> {
    watch: Arc<Watch>,
    inner: F,
}

impl<F: std::future::Future> std::future::Future for Timed<F> {
    type Output = F::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<F::Output> {
        // Safe because nothing here moves the inner future: it is
        // projected in place and the watch is not pinned data.
        let this = unsafe { self.get_unchecked_mut() };
        let inner = unsafe { std::pin::Pin::new_unchecked(&mut this.inner) };
        this.watch.entered();
        let polled = inner.poll(cx);
        this.watch.left();
        polled
    }
}

/// The thread watching one call, which stops watching when this is
/// dropped.
pub(crate) struct Watchdog {
    /// Dropping the sending end is how the thread is told the call is
    /// over: its next wait ends in a disconnection rather than in a
    /// timeout.
    _over: std::sync::mpsc::Sender<()>,
}

/// Start watching an isolate.
///
/// The handle is v8's thread safe one, which is the only thing about an
/// isolate that may be touched from another thread at all, and
/// `terminate_execution` is the only thing done with it.
pub(crate) fn watch(handle: v8::IsolateHandle, watch: Arc<Watch>, limits: Limits) -> Watchdog {
    let (over, until) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let deadline = Instant::now() + limits.wall;
        loop {
            match until.recv_timeout(TICK) {
                // The call is over, whichever way it went.
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) | Ok(()) => return,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            }
            // Somebody else may have got there first: the allocator
            // records the memory it refused rather than reaching into
            // v8 from underneath it, and this is the thread that turns
            // that into a stopped function.
            if watch.reached().is_some() {
                handle.terminate_execution();
                return;
            }
            // Over on buffers is a question and not an answer, so it is
            // put to the isolate: collect, and then say whether it is
            // still holding this much.
            if watch.buffered() > limits.memory && watch.wants_collection() {
                let asked = Arc::into_raw(Arc::clone(&watch)) as *mut c_void;
                if !handle.request_interrupt(collect, asked) {
                    // The isolate is gone, so nobody will take that
                    // reference back and there is nothing left to
                    // watch either.
                    drop(unsafe { Arc::from_raw(asked as *const Watch) });
                    return;
                }
            }
            let over = if Instant::now() >= deadline {
                WALL
            } else if watch.cpu() >= limits.cpu {
                CPU
            } else {
                continue;
            };
            if watch.record(over) {
                handle.terminate_execution();
            }
            return;
        }
    });
    Watchdog { _over: over }
}

/// Asked for from the watchdog and run on the isolate's own thread,
/// where v8 may be spoken to: collect what the function has finished
/// with, and stop it if what is left is still more than it is allowed.
unsafe extern "C" fn collect(mut isolate: v8::UnsafeRawIsolatePtr, asked: *mut c_void) {
    let watch = unsafe { Arc::from_raw(asked as *const Watch) };
    let isolate = unsafe { v8::Isolate::ref_from_raw_isolate_ptr_mut(&mut isolate) };
    isolate.low_memory_notification();
    if watch.buffered() > watch.limits.memory && watch.record(MEMORY) {
        isolate.terminate_execution();
    }
    watch.collected();
}

/// What v8 is given as its near heap limit callback: stop the isolate,
/// and hand back a limit large enough that the allocation in flight can
/// finish rather than abort the process.
pub(crate) fn near_heap_limit(
    handle: v8::IsolateHandle,
    watch: Arc<Watch>,
) -> impl FnMut(usize, usize) -> usize + 'static {
    move |current, _initial| {
        if watch.record(MEMORY) {
            handle.terminate_execution();
        }
        // Half as much again, and never less than eight mebibytes more.
        // This is headroom for the unwinding rather than a new limit: v8
        // only reaches it if the function keeps allocating after having
        // been terminated, and then it calls this again.
        current + (current / 2).max(8 * MIB)
    }
}

/// The count of array buffer memory one call has out, which is kept on
/// the watch because that is what the watchdog can read.
///
/// The budget is the same number v8 was given for its heap rather than
/// a share of it, because there is no way to ask v8 how much of the
/// heap is in use from in here without calling back into it, which is
/// the one thing an allocator may not do. So a call may hold its limit
/// in buffers and its limit on the heap, and that is written down
/// rather than pretended away.
struct Buffers {
    limit: usize,
    watch: Arc<Watch>,
}

/// How far past the budget the allocator itself will go before it
/// refuses. It is not one, because most of what a busy function is
/// holding is rubbish the collector has not been asked for yet and the
/// watchdog is what asks. It is not large, because this is the only
/// thing standing between a burst and the host's memory: at ten
/// milliseconds a tick and a gigabyte a second of zeroed pages, a
/// second budget is more room than a tick can use.
const BURST: usize = 2;

impl Buffers {
    /// Take `len` bytes out of the budget, or refuse and say why.
    fn take(&self, len: usize) -> bool {
        let held = self.watch.took(len);
        // One allocation larger than the whole budget is a limit
        // reached whatever the collector would say, and so is a total
        // past the burst, which is the only case the watchdog is too
        // slow for.
        if len > self.limit || held > self.limit.saturating_mul(BURST) {
            self.watch.gave(len);
            self.watch.record(MEMORY);
            return false;
        }
        true
    }

    fn give(&self, len: usize) {
        self.watch.gave(len);
    }
}

/// What is asked of the system allocator for a buffer of `len` bytes.
/// Zero is asked for as sixteen so that there is a real pointer to hand
/// back and a matching layout to free it with.
fn layout(len: usize) -> std::alloc::Layout {
    std::alloc::Layout::from_size_align(len.max(16), 16).expect("a buffer layout")
}

unsafe extern "C" fn allocate(buffers: &Buffers, len: usize) -> *mut c_void {
    if !buffers.take(len) {
        return std::ptr::null_mut();
    }
    let got = unsafe { std::alloc::alloc_zeroed(layout(len)) };
    if got.is_null() {
        buffers.give(len);
    }
    got as *mut c_void
}

unsafe extern "C" fn allocate_uninitialized(buffers: &Buffers, len: usize) -> *mut c_void {
    if !buffers.take(len) {
        return std::ptr::null_mut();
    }
    let got = unsafe { std::alloc::alloc(layout(len)) };
    if got.is_null() {
        buffers.give(len);
    }
    got as *mut c_void
}

unsafe extern "C" fn release(buffers: &Buffers, data: *mut c_void, len: usize) {
    unsafe { std::alloc::dealloc(data as *mut u8, layout(len)) };
    buffers.give(len);
}

/// v8 is finished with the allocator, so the count it was keeping can
/// go too.
unsafe extern "C" fn forget(buffers: *const Buffers) {
    drop(unsafe { Arc::from_raw(buffers) });
}

static VTABLE: &v8::RustAllocatorVtable<Buffers> = &v8::RustAllocatorVtable {
    allocate,
    allocate_uninitialized,
    free: release,
    drop: forget,
};

/// The allocator an isolate is created with, which is how buffers get
/// counted at all.
///
/// A refusal is a null pointer, which v8 turns into a `RangeError` in
/// the function rather than into anything happening to the isolate: the
/// limit was recorded on the way past, so the watchdog stops the call
/// on its next tick and the caller is told a limit and not a throw.
/// Nothing in here touches v8, because an allocator calling back into
/// v8 is not allowed to.
pub(crate) fn buffers(watch: Arc<Watch>, limits: Limits) -> v8::UniqueRef<v8::Allocator> {
    let counting = Arc::new(Buffers {
        limit: limits.memory,
        watch,
    });
    unsafe { v8::new_rust_allocator(Arc::into_raw(counting), VTABLE) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_numbers_are_upstreams() {
        let limits = Limits::default();
        assert_eq!(limits.memory_mib(), 256, "memoryLimitMb");
        assert_eq!(limits.wall, Duration::from_secs(400), "workerTimeoutMs");
        assert_eq!(limits.cpu, Duration::from_secs(2), "cpuTimeHardLimitMs");
        assert_eq!(
            limits.background,
            Duration::from_secs(30),
            "this project's own"
        );
    }

    #[test]
    fn what_is_waited_for_is_not_counted_as_cpu() {
        let watch = Watch::new(Limits::default());
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime");
        tokio.block_on(async {
            watch
                .timing(async {
                    // Being asleep is the whole of this, so the poll
                    // that starts it and the poll that finishes it are
                    // all that can be counted.
                    tokio::time::sleep(Duration::from_millis(200)).await;
                })
                .await;
        });
        assert!(
            watch.cpu() < Duration::from_millis(50),
            "a call that waited spent {:?}",
            watch.cpu()
        );
    }

    #[test]
    fn what_is_run_is_counted_as_cpu() {
        let watch = Watch::new(Limits::default());
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime");
        tokio.block_on(async {
            watch.timing(std::future::ready(())).await;
            // A poll that does not return is what the watchdog is for,
            // so the in flight part has to be visible from outside.
            let held = Arc::clone(&watch);
            watch
                .timing(std::future::poll_fn(move |_| {
                    std::thread::sleep(Duration::from_millis(50));
                    assert!(
                        held.cpu() >= Duration::from_millis(50),
                        "a poll that has not finished is still running"
                    );
                    std::task::Poll::Ready(())
                }))
                .await;
        });
        assert!(watch.cpu() >= Duration::from_millis(50));
    }

    #[test]
    fn the_first_limit_reached_is_the_one_reported() {
        let watch = Watch::new(Limits::default());
        assert_eq!(watch.reached(), None);
        assert!(watch.record(CPU));
        assert!(!watch.record(WALL), "the second one changes nothing");
        assert_eq!(watch.reached(), Some(Reached::Cpu));
        assert_eq!(
            watch.sentence(Reached::Cpu),
            "it spent more than the 2s of cpu time it is allowed"
        );
        assert_eq!(
            watch.sentence(Reached::Memory),
            "it wanted more memory than the 256 MiB it is allowed"
        );
    }
}
