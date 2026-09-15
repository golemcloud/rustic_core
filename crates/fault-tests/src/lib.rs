//! Scenarios that inject I/O faults into `rustic_core` operations.
//!
//! A scenario gives `Ok` only when the operation returns the expected error.
//! The binary of this crate runs each scenario in a child process that is built with `panic = "abort"`.
//! A panic thus stops the child process, and the binary reports the scenario as failed.
//! Tests can run the same scenarios in the test harness.
//!
//! Run all scenarios with `cargo run -p rustic_fault_tests --profile panic-abort`.

pub mod fixtures;
pub mod panics;
pub mod scenarios;
#[cfg(target_os = "linux")]
pub mod volume;
