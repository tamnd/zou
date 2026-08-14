//! `setTimeout` and the rest, which is the first thing in this runtime
//! that makes a function wait on the clock rather than on somebody
//! else's answer.
//!
//! A timer is an async op that sleeps, so the sleeping is tokio's and
//! the event loop deno_core already runs is what wakes it. What the
//! prelude keeps is the callback and whether the timer is still wanted.
//!
//! Clearing is a cancel and not a flag. A flag would be enough to stop
//! the callback running, and it would leave the sleep pending, and a
//! function that sets a timer for an hour and clears it would hold a
//! future for an hour. So a pending timer has a cancel handle in op
//! state and `clearTimeout` reaches it.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use deno_core::{CancelFuture, CancelHandle, OpState, op2};

/// Every timer that is sleeping, by the id the prelude gave it.
#[derive(Default)]
pub struct Pending(HashMap<u32, Rc<CancelHandle>>);

/// Whether the timer got to the end of its wait, which is false when
/// `clearTimeout` reached it first.
///
/// `deferred` rather than the default eager poll: a zero delay is
/// already elapsed the moment it is polled, so an eager op would hand
/// the answer back inside the call and `setTimeout(f, 0)` in a loop
/// would never let anything else run. Deferring it puts the resolution
/// on the next turn of the event loop, which is what a timer is.
#[op2(async(deferred), fast)]
pub async fn op_zou_sleep(state: Rc<RefCell<OpState>>, #[smi] id: u32, millis: f64) -> bool {
    let cancel = CancelHandle::new_rc();
    state
        .borrow_mut()
        .borrow_mut::<Pending>()
        .0
        .insert(id, cancel.clone());
    let waited = tokio::time::sleep(delay(millis))
        .or_cancel(cancel)
        .await
        .is_ok();
    state.borrow_mut().borrow_mut::<Pending>().0.remove(&id);
    waited
}

#[op2(fast)]
pub fn op_zou_clear(state: &mut OpState, #[smi] id: u32) {
    if let Some(cancel) = state.borrow_mut::<Pending>().0.remove(&id) {
        cancel.cancel();
    }
}

/// How long to sleep for, out of a number javascript is allowed to
/// hand over and this is not allowed to panic on.
///
/// A delay is a signed 32 bit integer in the spec, so a number past
/// that wraps into one rather than being refused, and everything that
/// lands at or below zero fires as soon as it can. That is a strange
/// rule to write down and it is the one every browser has: a function
/// asking for `setTimeout(f, 2 ** 31)` gets it immediately, not in
/// twenty five days.
fn delay(millis: f64) -> Duration {
    match wrapped(millis) {
        wait if wait > 0 => Duration::from_millis(wait as u64),
        _ => Duration::ZERO,
    }
}

/// The number as a `long` in the sense web idl means it, which is
/// javascript's own ToInt32: truncate, take it modulo two to the
/// thirty two, and read the result as signed. Nothing here is finite
/// unless it says it is.
fn wrapped(millis: f64) -> i32 {
    if !millis.is_finite() {
        return 0;
    }
    (millis.trunc().rem_euclid(4_294_967_296.0) as u32) as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_wait_is_the_number_of_milliseconds_it_says() {
        assert_eq!(delay(0.0), Duration::ZERO);
        assert_eq!(delay(250.0), Duration::from_millis(250));
        // A fraction of a millisecond is not a thing to sleep for.
        assert_eq!(delay(1.9), Duration::from_millis(1));
    }

    #[test]
    fn a_wait_that_is_not_a_wait_is_no_wait_at_all() {
        for millis in [-1.0, -0.0, f64::NAN, f64::NEG_INFINITY] {
            assert_eq!(delay(millis), Duration::ZERO, "{millis}");
        }
    }

    /// The ceiling is twenty five days, and a millisecond past it is
    /// not twenty five days and a millisecond, it is now.
    #[test]
    fn a_wait_past_the_ceiling_wraps_the_way_the_spec_says_it_does() {
        let ceiling = Duration::from_millis(i32::MAX as u64);
        assert_eq!(delay(f64::from(i32::MAX)), ceiling);
        // 2^31 read as signed is the most negative number there is,
        // and everything at or below zero is no wait.
        assert_eq!(delay(2_147_483_648.0), Duration::ZERO);
        // Round the whole way and it is the number it started as.
        assert_eq!(delay(4_294_967_296.0 + 250.0), Duration::from_millis(250));
    }
}
