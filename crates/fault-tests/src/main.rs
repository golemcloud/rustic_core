//! Runs the fault scenarios, each in a child process that is built with `panic = "abort"`.
//!
//! Without arguments, the binary starts itself one time for each scenario, and checks the exit status of each child process.
//! A scenario passes when its child process exits with the status 0.
//! A panic stops the child process with the signal `SIGABRT`, and the scenario fails.
//!
//! With the arguments `--scenario <name>`, the binary runs one scenario in this process.
//!
//! Run all scenarios with `cargo run -p rustic_fault_tests --profile panic-abort`.

use std::{
    env, io,
    process::{Command, ExitCode, ExitStatus},
};

use rustic_fault_tests::scenarios::SCENARIOS;

fn main() -> ExitCode {
    if !cfg!(panic = "abort") {
        eprintln!(
            "Build this binary with `--profile panic-abort`. Without it, a panic does not stop the process."
        );
        return ExitCode::from(2);
    }

    let args: Box<[Box<str>]> = env::args().skip(1).map(String::into_boxed_str).collect();
    match args.as_ref() {
        [] => run_all(),
        [flag, name] if &**flag == "--scenario" => run_one(name),
        _ => {
            eprintln!("Usage: rustic_fault_tests [--scenario <name>]");
            ExitCode::from(2)
        }
    }
}

/// Runs the scenario `name` in this process.
fn run_one(name: &str) -> ExitCode {
    let Some((_, scenario)) = SCENARIOS.iter().find(|(candidate, _)| *candidate == name) else {
        eprintln!("Unknown scenario `{name}`.");
        return ExitCode::from(2);
    };
    match scenario() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("Scenario `{name}` failed: {err}");
            ExitCode::FAILURE
        }
    }
}

/// Runs each scenario in a child process, and reports the result of each scenario.
fn run_all() -> ExitCode {
    let exe = match env::current_exe() {
        Ok(exe) => exe,
        Err(err) => {
            eprintln!("Cannot find the path of this binary: {err}");
            return ExitCode::from(2);
        }
    };

    let failed = SCENARIOS
        .iter()
        .map(|(name, _)| {
            let status = Command::new(&exe).args(["--scenario", name]).status();
            println!("{name}: {}", describe(&status));
            status.is_ok_and(|status| status.success())
        })
        .filter(|passed| !passed)
        .count();

    println!("{} scenarios, {failed} failed", SCENARIOS.len());
    if failed == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// Describes the exit status of a child process.
fn describe(status: &io::Result<ExitStatus>) -> String {
    match status {
        Ok(status) if status.success() => "passed".to_string(),
        Ok(status) => format!("FAILED ({status})"),
        Err(err) => format!("FAILED (the child process did not start: {err})"),
    }
}
