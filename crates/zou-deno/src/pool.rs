//! Isolates kept between calls, which is upstream's `per_worker`.
//!
//! What it buys is the cold start: an isolate that has already loaded,
//! transpiled and evaluated a function's module, and already built
//! whatever that module built at the top of itself, answers the second
//! call without doing any of it again. What it costs is that the second
//! call runs in the first call's isolate, which is the same bargain
//! upstream makes and the reason it is a policy and not a default.
//!
//! # A worker is a thread, because an isolate is
//!
//! V8 isolates are thread bound, so a kept isolate is a kept thread:
//! one per function that has been called and not yet gone idle, holding
//! its `Ready` and taking calls down a channel. A call that arrives
//! while that worker is busy gets a worker of its own rather than
//! waiting behind it, because a function that answers slowly should not
//! make the next caller answer slowly too, and the extra workers are
//! dropped when the burst is over.
//!
//! # What ends an isolate
//!
//! Three things, and none of them is a call that merely failed.
//!
//! A limit reached is the end of it: a terminated isolate is not
//! somewhere the next call should start from, and memory is counted for
//! as long as the isolate lives rather than per call, so an isolate
//! that reached the memory limit would reach it again immediately.
//!
//! A file it was built out of changing is the end of it, and that is
//! hot reload, which the pinned CLI's own `config.toml` says is what
//! `per_worker` is for: "`per_worker` (default) enables hot reload
//! during local development". The loader records every file off this
//! disk that went into the isolate, so a `_shared` module the function
//! imports counts the same as the `index.ts` itself.
//!
//! Being idle is the end of it, after a minute. That number is this
//! project's, not upstream's, and the thing it trades is a cold start
//! nobody is waiting on against a quarter of a gigabyte of address
//! space per function nobody is calling.
//!
//! What is deliberately not the end of it is a handler that threw. That
//! is an ordinary answer, the isolate is intact, and throwing it away
//! would mean a broken function is also a slow one.

use std::collections::HashMap;
use std::sync::{Mutex, mpsc};
use std::time::Duration;

use zou_functions::Failed;

use crate::isolate::{Held, Ready, Source};
use crate::limits::Limits;

/// How long a worker waits for another call before it gives its isolate
/// back to the operating system.
const IDLE: Duration = Duration::from_secs(60);

/// One call on its way to a worker, and the way back.
struct Job {
    held: Held,
    done: mpsc::Sender<Result<(), Failed>>,
}

/// One thread with one isolate for one function.
struct Worker {
    calls: mpsc::Sender<Job>,
}

/// Every worker that is not in the middle of a call, by the function it
/// is for.
///
/// A busy worker is not in here: it is on the stack of the call that
/// took it, and it comes back when that call is over. So the map is the
/// idle list rather than the census, and nothing has to be marked or
/// unmarked as a call starts and ends.
#[derive(Default)]
pub(crate) struct Pool {
    idle: Mutex<HashMap<String, Vec<Worker>>>,
}

impl Pool {
    /// Run one call in a kept isolate, building one if there is none.
    pub(crate) fn run(&self, source: &Source, held: Held, limits: Limits) -> Result<(), Failed> {
        let specifier = &source.specifier;
        let key = source.key();
        let (done, answered) = mpsc::channel();
        let mut job = Job { held, done };
        // Twice, because a worker taken out of the map may have gone
        // home in the moment between being idle and being handed a
        // call, and that is not an error, it is a worker to make.
        for attempt in 0..2 {
            let worker = match self.take(&key) {
                Some(worker) => worker,
                None => spawn(source.clone(), limits)?,
            };
            match worker.calls.send(job) {
                Ok(()) => {
                    let out = answered.recv().map_err(|_| {
                        Failed::Threw(format!(
                            "{specifier}: the isolate running it stopped without saying why"
                        ))
                    });
                    self.give(&key, worker);
                    return out?;
                }
                // The channel gives the job back rather than dropping
                // it, which is the only reason this can be retried.
                Err(mpsc::SendError(returned)) if attempt == 0 => job = returned,
                Err(_) => {
                    return Err(Failed::Threw(format!(
                        "{specifier}: no isolate would take the call"
                    )));
                }
            }
        }
        unreachable!("the loop either sends the job or returns")
    }

    fn take(&self, key: &str) -> Option<Worker> {
        let mut idle = self.idle.lock().expect("nothing panics holding this");
        idle.get_mut(key)?.pop()
    }

    /// A worker with nothing left to do, kept if there is room for it.
    ///
    /// The room is the number of calls that could have been running at
    /// once, because that is the most workers a burst can have made and
    /// the point past which keeping one is keeping an isolate for a
    /// caller who is not there.
    fn give(&self, key: &str, worker: Worker) {
        let mut idle = self.idle.lock().expect("nothing panics holding this");
        let waiting = idle.entry(key.to_string()).or_default();
        if waiting.len() < room() {
            waiting.push(worker);
        }
    }
}

/// How many idle isolates a function may keep.
fn room() -> usize {
    std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get)
}

fn spawn(source: Source, limits: Limits) -> Result<Worker, Failed> {
    let (calls, jobs) = mpsc::channel();
    let named = source.specifier.to_string();
    std::thread::Builder::new()
        .name("zou-function".to_string())
        .spawn(move || work(&source, limits, &jobs))
        .map_err(|e| Failed::Threw(format!("{named}: it could not have a thread: {e}")))?;
    Ok(Worker { calls })
}

/// One worker's whole life: build an isolate when there is a call for
/// it, keep it while it is fit, and go home when nobody calls.
fn work(source: &Source, limits: Limits, jobs: &mpsc::Receiver<Job>) {
    let specifier = &source.specifier;
    let tokio = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(tokio) => tokio,
        Err(e) => {
            // Everybody who asks is told, rather than one caller
            // hearing about it and the rest waiting on a thread that
            // has quietly given up.
            let why = format!("{specifier}: the isolate could not have a runtime: {e}");
            while let Ok(job) = jobs.recv() {
                let _ = job.done.send(Err(Failed::Threw(why.clone())));
            }
            return;
        }
    };
    let mut ready: Option<Ready> = None;
    while let Ok(job) = jobs.recv_timeout(IDLE) {
        let out = tokio.block_on(once(&mut ready, source, limits, job.held));
        if matches!(out, Err(Failed::Limit(_))) || ready.as_ref().is_some_and(Ready::spent) {
            ready = None;
        }
        // A caller that has given up on the answer is not a reason to
        // stop being a worker.
        let _ = job.done.send(out);
    }
}

/// One call, in the isolate this worker has or in the one it makes.
async fn once(
    slot: &mut Option<Ready>,
    source: &Source,
    limits: Limits,
    held: Held,
) -> Result<(), Failed> {
    let specifier = &source.specifier;
    if slot.as_ref().is_some_and(Ready::stale) {
        *slot = None;
    }
    let call = async {
        if slot.is_none() {
            *slot = Some(Ready::new(source.clone(), limits).await?);
        }
        slot.as_mut().expect("it was just built").once(held).await
    };
    match tokio::time::timeout(limits.wall, call).await {
        Ok(ran) => ran,
        Err(_) => Err(Failed::Limit(format!(
            "{specifier}: it was still running after the {:?} it is allowed",
            limits.wall
        ))),
    }
}
