//! Checks for panics in threads that an operation does not join.

use std::{
    iter, panic,
    sync::{
        Arc, Once,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use rustic_testing::TestResult;

/// The number of panics since [`count_panics`] installed its hook.
static PANICS: AtomicUsize = AtomicUsize::new(0);

/// The time between two checks in [`wait_until_released`].
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Gives the number of panics in this process.
///
/// The first call installs a panic hook for the process.
/// The hook counts each panic in each thread, and then calls the previous hook.
/// The count starts at the first call.
///
/// # Returns
///
/// The number of panics since the first call.
#[must_use]
pub fn count_panics() -> usize {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let previous = panic::take_hook();
        panic::set_hook(Box::new(move |info| {
            _ = PANICS.fetch_add(1, Ordering::SeqCst);
            previous(info);
        }));
    });
    PANICS.load(Ordering::SeqCst)
}

/// Waits until the caller holds the only strong reference to `value`.
///
/// Use this function after an operation that gave clones of `value` to threads that the operation does not join.
/// A thread drops its clones when it stops.
/// A panic hook runs before the thread drops its clones.
/// Thus, when this function returns `Ok`, [`count_panics`] includes each panic of these threads.
///
/// # Arguments
///
/// * `value` - The value that the threads share
/// * `timeout` - The maximum time to wait
///
/// # Errors
///
/// * If other strong references to `value` exist after `timeout`.
pub fn wait_until_released<T: ?Sized>(value: &Arc<T>, timeout: Duration) -> TestResult<()> {
    let deadline = Instant::now() + timeout;
    let is_released = || Arc::strong_count(value) == 1;
    let released_in_time = iter::successors(Some(Instant::now()), |_| {
        thread::sleep(POLL_INTERVAL);
        Some(Instant::now())
    })
    .take_while(|now| *now < deadline)
    .any(|_| is_released());

    if released_in_time || is_released() {
        Ok(())
    } else {
        Err(format!(
            "{} other strong references exist after {timeout:?}",
            Arc::strong_count(value) - 1
        )
        .into())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread, time::Duration};

    use super::{count_panics, wait_until_released};

    #[test]
    fn count_panics_counts_a_panic_in_another_thread() {
        let before = count_panics();
        let joined = thread::spawn(|| panic!("panic for the test")).join();
        assert!(joined.is_err());
        assert!(count_panics() > before);
    }

    #[test]
    fn wait_until_released_waits_for_a_thread() {
        let value = Arc::new(());
        let clone = Arc::clone(&value);
        let _handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            drop(clone);
        });
        assert!(wait_until_released(&value, Duration::from_secs(10)).is_ok());
    }

    #[test]
    fn wait_until_released_fails_after_the_timeout() {
        let value = Arc::new(());
        let _clone = Arc::clone(&value);
        assert!(wait_until_released(&value, Duration::from_millis(50)).is_err());
    }
}
