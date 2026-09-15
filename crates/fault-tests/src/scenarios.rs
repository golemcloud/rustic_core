//! Scenarios that inject faults into `rustic_core` operations.
//!
//! A scenario gives `Ok` only when the operation returns the expected error, and no panic occurs.

use rustic_testing::TestResult;

/// A scenario: its name on the command line, and its function.
pub type Scenario = (&'static str, fn() -> TestResult<()>);

/// The scenarios that the binary of this crate runs, in this order.
pub const SCENARIOS: &[Scenario] = &[];
