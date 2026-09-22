//! A logger that keeps the warnings of this process, so that a scenario can count them.

use std::sync::{Mutex, OnceLock, PoisonError};

use log::{Level, LevelFilter, Log, Metadata, Record};
use rustic_testing::TestResult;

/// The message of each warning that [`CapturingLogger`] kept.
static WARNINGS: Mutex<Vec<Box<str>>> = Mutex::new(Vec::new());

/// Whether this process installed [`CapturingLogger`].
static INSTALLED: OnceLock<bool> = OnceLock::new();

/// A logger that keeps the message of each record of the level [`Level::Warn`] or higher.
#[derive(Debug)]
struct CapturingLogger;

impl Log for CapturingLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= Level::Warn
    }

    fn log(&self, record: &Record<'_>) {
        if self.enabled(record.metadata()) {
            WARNINGS
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(record.args().to_string().into_boxed_str());
        }
    }

    fn flush(&self) {}
}

/// Makes this process keep its warnings, so that [`warnings_containing`] can count them.
///
/// A process that installs no logger has the maximum level `Off`, and `log` then gives no record to a logger.
/// So this function also sets the maximum level to [`LevelFilter::Warn`]. It keeps that level, because the
/// scenarios of this crate need no other level, and a test harness runs several scenarios in one process.
///
/// A process can install only one logger, so this function installs the logger one time.
///
/// # Errors
///
/// * If the process already has another logger. Then the scenario cannot count the warnings, and it must stop
///   instead of giving a result that shows nothing.
pub fn capture_warnings() -> TestResult<()> {
    if *INSTALLED.get_or_init(|| log::set_boxed_logger(Box::new(CapturingLogger)).is_ok()) {
        log::set_max_level(LevelFilter::Warn);
        Ok(())
    } else {
        Err("This process has another logger, so the scenario cannot count the warnings.".into())
    }
}

/// Gives the number of warnings of this process whose message contains `text`.
///
/// Call [`capture_warnings`] before the operation that writes the warnings. A scenario uses a `text` that only
/// its own warnings hold, so a test harness that runs several scenarios at the same time gives the same count.
#[must_use]
pub fn warnings_containing(text: &str) -> usize {
    WARNINGS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter(|warning| warning.contains(text))
        .count()
}
