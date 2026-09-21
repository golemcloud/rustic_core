//! Runs the fault scenarios in the test harness.
//!
//! The test harness catches a panic, so a scenario that panics fails its test.
//! A panic in a thread that no test joins does not fail a test.
//! Thus the scenarios for such threads count the panics of these threads.
//! The binary of this crate runs the same scenarios in child processes, and the `panic-abort` profile builds that binary with `panic = "abort"`.
//! Only the binary runs the scenarios `restore-full-volume` and `property-full-volume`, because they need a process that has one thread.
//! The property tests here run fewer cases than the property scenarios of the binary, and check the same coverage.

#[cfg(target_os = "linux")]
use rustic_fault_tests::property;
use rustic_fault_tests::scenarios;
use rustic_testing::TestResult;

/// The number of cases of each property test in the test harness.
#[cfg(target_os = "linux")]
const PROPERTY_CASES: usize = 32;

#[test]
fn restore_without_faults() -> TestResult<()> {
    scenarios::restore_without_faults()
}

#[test]
fn restore_sparse_without_faults() -> TestResult<()> {
    scenarios::restore_sparse_without_faults()
}

#[test]
fn restore_pack_read() -> TestResult<()> {
    scenarios::restore_pack_read()
}

#[test]
fn restore_short_pack_read() -> TestResult<()> {
    scenarios::restore_short_pack_read()
}

#[test]
fn cached_tree_read_short_pack() -> TestResult<()> {
    scenarios::cached_tree_read_short_pack()
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
fn restore_stops_after_first_error_with_one_thread() -> TestResult<()> {
    scenarios::restore_stops_after_first_error_with_one_thread()
}

#[test]
fn restore_write_stops_after_first_error() -> TestResult<()> {
    scenarios::restore_write_stops_after_first_error()
}

#[test]
fn restore_reader_threads() -> TestResult<()> {
    scenarios::restore_reader_threads()
}

#[test]
fn restore_default_reader_threads() -> TestResult<()> {
    scenarios::restore_default_reader_threads()
}

#[cfg(target_os = "linux")]
#[test]
fn restore_directory_metadata_after_entries() -> TestResult<()> {
    scenarios::restore_directory_metadata_after_entries()
}

#[cfg(target_os = "linux")]
#[test]
fn backup_unreadable_file() -> TestResult<()> {
    scenarios::backup_unreadable_file()
}

#[cfg(target_os = "linux")]
#[test]
fn backup_unreadable_dir() -> TestResult<()> {
    scenarios::backup_unreadable_dir()
}

#[cfg(target_os = "linux")]
#[test]
fn backup_skips_unreadable_file_without_option() -> TestResult<()> {
    scenarios::backup_skips_unreadable_file_without_option()
}

#[cfg(target_os = "linux")]
#[test]
fn backup_stops_after_first_error() -> TestResult<()> {
    scenarios::backup_stops_after_first_error()
}

#[cfg(target_os = "linux")]
#[test]
fn metadata_symlink() -> TestResult<()> {
    scenarios::metadata_symlink()
}

#[cfg(target_os = "linux")]
#[test]
fn metadata_ownership() -> TestResult<()> {
    scenarios::metadata_ownership()
}

#[cfg(target_os = "linux")]
#[test]
fn metadata_permission() -> TestResult<()> {
    scenarios::metadata_permission()
}

#[cfg(target_os = "linux")]
#[test]
fn metadata_extended_attributes() -> TestResult<()> {
    scenarios::metadata_extended_attributes()
}

#[cfg(target_os = "linux")]
#[test]
fn metadata_times() -> TestResult<()> {
    scenarios::metadata_times()
}

#[cfg(target_os = "linux")]
#[test]
fn metadata_errors_are_warnings_without_option() -> TestResult<()> {
    scenarios::metadata_errors_are_warnings_without_option()
}

#[test]
fn prune_tree_read() -> TestResult<()> {
    scenarios::prune_tree_read()
}

#[test]
fn prune_stops_after_first_error() -> TestResult<()> {
    scenarios::prune_stops_after_first_error()
}

#[cfg(target_os = "linux")]
#[test]
fn property_no_cache() -> TestResult<()> {
    property::run_cases(false, PROPERTY_CASES)
}

#[cfg(target_os = "linux")]
#[test]
fn property_cache() -> TestResult<()> {
    property::run_cases(true, PROPERTY_CASES)
}
