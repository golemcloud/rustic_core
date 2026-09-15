//! Scenarios that inject faults into `rustic_core` operations.
//!
//! A scenario gives `Ok` only when the operation returns the expected error, and no panic occurs.

use std::{
    fs,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use rustic_core::{FileType, RestoreOptions};
use rustic_testing::{
    TestResult,
    backend::fault_injection_backend::{BackendCall, BackendOp, Fault, INJECTED_FAULT},
};
use tempfile::tempdir;

use crate::fixtures::{
    SavedRepo, backup, content, expect_error, fault_injection_backend, init_repo, restore_with,
    save_files, write_files,
};
#[cfg(target_os = "linux")]
use crate::volume::{Tmpfs, enter_user_mount_namespace};

/// A scenario: its name on the command line, and its function.
pub type Scenario = (&'static str, fn() -> TestResult<()>);

/// The scenarios that the binary of this crate runs, in this order.
pub const SCENARIOS: &[Scenario] = &[
    ("restore-without-faults", restore_without_faults),
    ("restore-pack-read", restore_pack_read),
    ("restore-decrypt", restore_decrypt),
    ("restore-existing-file-read", restore_existing_file_read),
    ("restore-set-length", restore_set_length),
    (
        "restore-stops-after-first-error",
        restore_stops_after_first_error,
    ),
    #[cfg(target_os = "linux")]
    ("restore-full-volume", restore_full_volume),
];

/// The number of reader threads of a restore.
///
/// This value is `MAX_READER_THREADS_NUM` in `rustic_core`.
const READER_THREADS: usize = 20;

/// Tells if `call` reads a part of a pack file.
///
/// The content phase of a restore reads the packs in this way.
fn is_pack_read(call: &BackendCall) -> bool {
    call.op == BackendOp::ReadPartial && call.tpe == FileType::Pack
}

/// A restore without faults gives the saved files.
///
/// # Errors
///
/// * If the restore fails.
/// * If a restored file is not equal to the saved file.
pub fn restore_without_faults() -> TestResult<()> {
    let (a, b) = (content(1, 300_000), content(2, 5_000));
    let files: [(&str, &[u8]); 2] = [("a", &a), ("sub/b", &b)];
    let saved = save_files(&files)?;
    let dir = tempdir()?;

    restore_with(&saved, dir.path(), &RestoreOptions::default(), |_| Ok(()))??;

    files.iter().try_for_each(|(name, data)| {
        let restored = fs::read(dir.path().join("data").join(name))?;
        if restored.as_slice() == *data {
            Ok(())
        } else {
            Err(format!("The restored file `{name}` is not equal to the saved file.").into())
        }
    })
}

/// A restore whose pack reads fail returns the injected backend error.
///
/// # Errors
///
/// * If the restore does not return the injected error.
pub fn restore_pack_read() -> TestResult<()> {
    let data = content(3, 300_000);
    let saved = save_files(&[("a", &data)])?;
    let dir = tempdir()?;

    let result = restore_with(&saved, dir.path(), &RestoreOptions::default(), |_| {
        saved
            .backend
            .inject(|call| is_pack_read(call).then_some(Fault::Error));
        Ok(())
    })?;

    expect_error(result, INJECTED_FAULT)
}

/// A restore whose pack reads give corrupt data returns the error of the decryption.
///
/// # Errors
///
/// * If the restore does not return the decryption error.
pub fn restore_decrypt() -> TestResult<()> {
    let data = content(4, 300_000);
    let saved = save_files(&[("a", &data)])?;
    let dir = tempdir()?;

    let result = restore_with(&saved, dir.path(), &RestoreOptions::default(), |_| {
        saved
            .backend
            .inject(|call| is_pack_read(call).then_some(Fault::Corrupt));
        Ok(())
    })?;

    expect_error(result, "Data decryption failed")
}

/// A restore that cannot read an existing file returns an error with the path of that file.
///
/// The files `a` and `b` have the same content.
/// The destination has `a` with the saved content, and `b` with other content of the same size.
/// Thus the restore reads the content for `b` from the existing file `a`.
/// The scenario removes `a` after the plan.
///
/// # Errors
///
/// * If the restore does not return an error with the path of `a`.
pub fn restore_existing_file_read() -> TestResult<()> {
    let data = content(5, 300_000);
    let other = content(6, data.len());
    let saved = save_files(&[("a", &data), ("b", &data)])?;
    let dir = tempdir()?;
    write_files(&dir.path().join("data"), &[("a", &data), ("b", &other)])?;
    let opts = RestoreOptions::default().verify_existing(true);

    let result = restore_with(&saved, dir.path(), &opts, |_| {
        fs::remove_file(dir.path().join("data").join("a"))?;
        Ok(())
    })?;

    expect_error(result, &Path::new("data").join("a").display().to_string())
}

/// A restore that cannot set the length of a file returns an error with the path of that file.
///
/// The plan makes the directory `data/sub`.
/// The scenario replaces that directory with a file, so the restore cannot make `data/sub/a`.
///
/// # Errors
///
/// * If the restore does not return an error with the path of `data/sub/a`.
pub fn restore_set_length() -> TestResult<()> {
    let data = content(7, 300_000);
    let saved = save_files(&[("sub/a", &data)])?;
    let dir = tempdir()?;

    let result = restore_with(&saved, dir.path(), &RestoreOptions::default(), |_| {
        let sub = dir.path().join("data").join("sub");
        fs::remove_dir(&sub)?;
        fs::write(&sub, b"not a directory")?;
        Ok(())
    })?;

    let path = Path::new("data").join("sub").join("a");
    expect_error(result, &path.display().to_string())
}

/// A restore stops its remaining pack reads after the first error.
///
/// The scenario saves 40 files, each in its own backup, so each file is in its own pack.
/// A last backup of all files adds no data, so its snapshot uses all 40 packs.
/// Then each pack read of the restore fails.
///
/// # Errors
///
/// * If the snapshot uses too few packs for the check.
/// * If the restore does not return the injected error.
/// * If the restore reads more packs than it has reader threads.
pub fn restore_stops_after_first_error() -> TestResult<()> {
    let backend = fault_injection_backend();
    let source = tempdir()?;
    let files: Box<[_]> = (0..40)
        .map(|seed| {
            (
                format!("f{seed:02}").into_boxed_str(),
                content(100 + seed, 4_000),
            )
        })
        .collect();
    files
        .iter()
        .try_for_each(|(name, data)| write_files(source.path(), &[(name, data)]))?;

    let repo = files.iter().try_fold(
        init_repo(&backend)?.to_indexed_ids()?,
        |repo, (name, _)| -> TestResult<_> {
            let (repo, _) = backup(repo, &source.path().join(&**name), "single")?;
            Ok(repo)
        },
    )?;
    let (repo, snap) = backup(repo, source.path(), "data")?;
    let saved = SavedRepo {
        backend,
        repo: repo.to_indexed()?,
        snap,
    };

    let dir = tempdir()?;
    let reads = Arc::new(AtomicUsize::new(0));
    let mut packs = 0;
    let result = restore_with(&saved, dir.path(), &RestoreOptions::default(), |plan| {
        packs = plan.to_packs().len();
        let reads = Arc::clone(&reads);
        saved.backend.inject(move |call| {
            is_pack_read(call).then(|| {
                _ = reads.fetch_add(1, Ordering::SeqCst);
                Fault::Error
            })
        });
        Ok(())
    })?;
    expect_error(result, INJECTED_FAULT)?;

    // Only the reader threads of the restore run pack tasks. The thread that calls the restore is
    // not in the rayon pool of the restore, so `in_place_scope` makes it wait, and it runs no task.
    // Each read fails. Thus a reader thread stores or finds the first error before it takes its
    // next task, and that task stops before its read. Each reader thread reads at most one time.
    let reads = reads.load(Ordering::SeqCst);
    if packs <= READER_THREADS {
        return Err(format!(
            "The snapshot uses {packs} packs. The check needs more than {READER_THREADS} packs."
        )
        .into());
    }
    if reads > READER_THREADS {
        return Err(format!(
            "The restore read {reads} of {packs} packs. The maximum is {READER_THREADS} reads."
        )
        .into());
    }
    Ok(())
}

/// A restore into a full volume returns the error of the write.
///
/// The scenario moves this process into new namespaces.
/// Thus run it only in a process that has one thread.
///
/// # Errors
///
/// * If the process cannot mount the small volume.
/// * If the restore does not return a `StorageFull` error.
#[cfg(target_os = "linux")]
pub fn restore_full_volume() -> TestResult<()> {
    enter_user_mount_namespace()?;
    let data = content(8, 2_000_000);
    let saved = save_files(&[("a", &data)])?;
    let mount_dir = tempdir()?;
    let volume = Tmpfs::mount(mount_dir.path(), 256_000)?;

    let result = restore_with(
        &saved,
        volume.path(),
        &RestoreOptions::default(),
        |_| Ok(()),
    )?;

    expect_error(result, "StorageFull")
}
