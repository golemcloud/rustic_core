//! Runs each fault scenario in a child process.
//!
//! The `panic-abort` profile builds this binary with `panic = "abort"`.
//! Without arguments, the binary starts itself one time for each scenario, and checks the exit status of each child process.
//! A scenario passes when its child process exits with the status 0.
//! A panic stops the child process with the signal `SIGABRT`, and the scenario fails.
//! A child process that runs longer than [`SCENARIO_TIMEOUT`] is stopped, and the scenario fails.
//!
//! With the arguments `--scenario <name>`, the binary runs one scenario in this process.
//!
//! Run all scenarios with `cargo run -p rustic_fault_tests --profile panic-abort`.

use std::{
    env, io, iter,
    path::Path,
    process::{Child, Command, ExitCode, ExitStatus},
    thread,
    time::{Duration, Instant},
};

use rustic_fault_tests::scenarios::SCENARIOS;

/// The maximum time of one scenario. The binary stops a child process that runs longer.
const SCENARIO_TIMEOUT: Duration = Duration::from_mins(20);

/// The time between two checks of the status of a child process.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

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
    let exe: Box<Path> = match env::current_exe() {
        Ok(exe) => exe.into_boxed_path(),
        Err(err) => {
            eprintln!("Cannot find the path of this binary: {err}");
            return ExitCode::from(2);
        }
    };

    let failed = SCENARIOS
        .iter()
        .map(|(name, _)| {
            let status = Command::new(&*exe)
                .args(["--scenario", name])
                .spawn()
                .and_then(|mut child| wait_with_timeout(&mut child));
            println!("{name}: {}", describe(&status));
            status.is_ok_and(|status| status.is_some_and(|status| status.success()))
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

/// Waits for `child` for at most [`SCENARIO_TIMEOUT`].
///
/// # Returns
///
/// The exit status of `child`, or `None` if `child` did not stop in time. Then this function stops `child`.
///
/// # Errors
///
/// * If the function cannot get the status of `child`, or cannot stop it.
fn wait_with_timeout(child: &mut Child) -> io::Result<Option<ExitStatus>> {
    let deadline = Instant::now() + SCENARIO_TIMEOUT;
    let status = iter::repeat_with(|| {
        let status = child.try_wait();
        if matches!(status, Ok(None)) {
            thread::sleep(POLL_INTERVAL);
        }
        status
    })
    .find_map(|status| match status {
        Ok(None) if Instant::now() < deadline => None,
        status => Some(status),
    })
    .unwrap_or(Ok(None))?;
    if status.is_none() {
        child.kill()?;
        _ = child.wait()?;
    }
    Ok(status)
}

/// Describes the exit status of a child process.
fn describe(status: &io::Result<Option<ExitStatus>>) -> Box<str> {
    match status {
        Ok(Some(status)) if status.success() => "passed".into(),
        Ok(Some(status)) => format!("FAILED ({status})").into_boxed_str(),
        Ok(None) => format!(
            "FAILED (the child process did not stop in {} seconds)",
            SCENARIO_TIMEOUT.as_secs()
        )
        .into_boxed_str(),
        Err(err) => format!("FAILED (the child process did not start: {err})").into_boxed_str(),
    }
}
