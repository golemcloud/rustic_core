//! Scenarios that inject I/O faults into `rustic_core` operations.
//!
//! A scenario gives `Ok` only when the operation gives the expected result: the saved data without faults, or the expected error with faults.
//! The `panic-abort` profile builds the binary of this crate with `panic = "abort"`.
//! The binary runs each scenario in a child process.
//! A panic thus stops the child process, and the binary reports the scenario as failed.
//! Tests can run the same scenarios in the test harness.
//!
//! Run all scenarios with `cargo run -p rustic_fault_tests --profile panic-abort`.

pub mod fixtures;
pub mod panics;
pub mod scenarios;
#[cfg(target_os = "linux")]
pub mod volume;
