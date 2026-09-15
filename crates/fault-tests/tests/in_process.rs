//! Runs the fault scenarios in the test harness.
//!
//! The test harness catches a panic, so a scenario that panics fails its test.
//! A panic in a thread that no test joins does not fail a test.
//! Thus the scenarios for such threads count the panics of these threads.
//! The binary of this crate runs the same scenarios in processes that are built with `panic = "abort"`.
//! The binary also runs the scenario for a full volume, which needs a process that has one thread.

use rustic_fault_tests::scenarios;
use rustic_testing::TestResult;

#[test]
fn restore_without_faults() -> TestResult<()> {
    scenarios::restore_without_faults()
}

#[test]
fn restore_pack_read() -> TestResult<()> {
    scenarios::restore_pack_read()
}

#[test]
fn restore_decrypt() -> TestResult<()> {
    scenarios::restore_decrypt()
}

#[test]
fn restore_existing_file_read() -> TestResult<()> {
    scenarios::restore_existing_file_read()
}

#[test]
fn restore_set_length() -> TestResult<()> {
    scenarios::restore_set_length()
}

#[test]
fn restore_stops_after_first_error() -> TestResult<()> {
    scenarios::restore_stops_after_first_error()
}

#[test]
fn prune_tree_read() -> TestResult<()> {
    scenarios::prune_tree_read()
}
