//! Scenarios that inject faults into `rustic_core` operations.
//!
//! A scenario gives `Ok` only when the operation gives the expected result, and no panic occurs.
//! Without faults, the expected result is the saved data. With faults, it is the expected error.

use std::{
    fs, iter,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use rustic_core::{FileType, LsOptions, PruneOptions, RestoreOptions};
use rustic_testing::{
    TestResult,
    backend::fault_injection_backend::{BackendCall, BackendOp, Fault, INJECTED_FAULT},
};
use tempfile::tempdir;

#[cfg(target_os = "linux")]
use crate::volume::{Tmpfs, enter_user_mount_namespace};
use crate::{
    fixtures::{
        SavedRepo, backup, content, expect_error, fault_injection_backend, init_repo, restore_with,
        save_files, save_files_with_cache, shorten_files, write_files,
    },
    panics::{count_panics, wait_until_released},
};

/// A scenario: its name on the command line, and its function.
pub type Scenario = (&'static str, fn() -> TestResult<()>);

/// The scenarios that the binary of this crate runs, in this order.
pub const SCENARIOS: &[Scenario] = &[
    ("restore-without-faults", restore_without_faults),
    (
        "restore-sparse-without-faults",
        restore_sparse_without_faults,
    ),
    ("restore-pack-read", restore_pack_read),
    ("restore-short-pack-read", restore_short_pack_read),
    ("cached-tree-read-short-pack", cached_tree_read_short_pack),
    ("restore-decrypt", restore_decrypt),
    ("restore-existing-file-read", restore_existing_file_read),
    ("restore-set-length", restore_set_length),
    (
        "restore-stops-after-first-error",
        restore_stops_after_first_error,
    ),
    #[cfg(target_os = "linux")]
    ("restore-full-volume", restore_full_volume),
    ("prune-tree-read", prune_tree_read),
    (
        "prune-stops-after-first-error",
        prune_stops_after_first_error,
    ),
];

/// The number of reader threads of a restore.
///
/// This value is `MAX_READER_THREADS_NUM` in `rustic_core`.
const READER_THREADS: usize = 20;

/// The number of tree loader threads of a prune.
///
/// This value is `MAX_TREE_LOADER` in `rustic_core`. The channel from the loaders holds the same
/// number of trees.
const TREE_LOADERS: usize = 4;

/// Part of the message of the error for a backend read that gives too few bytes.
const SHORT_READ: &str = "The read of";

/// Part of the message of the error for a file that is shorter than the read needs.
const SHORT_FILE: &str = "The read needs";

/// The maximum time for the threads of an operation to stop after the operation returns.
const RELEASE_TIMEOUT: Duration = Duration::from_secs(30);

/// Tells if `call` reads a part of a pack file.
///
/// A restore reads the content of files in this way, and a prune reads trees in this way.
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
        let restored: Box<[u8]> = fs::read(dir.path().join("data").join(name))?.into_boxed_slice();
        if *restored == **data {
            Ok(())
        } else {
            Err(format!("The restored file `{name}` is not equal to the saved file.").into())
        }
    })
}

/// A sparse restore without faults gives a saved file that ends in zeros.
///
/// The file has 1 MiB of pseudo-random data, and then 9 MiB of zeros.
/// The default maximum size of a chunk is 8 MiB.
/// Thus the chunk that holds the first zero byte ends at 9 MiB or before, and the last chunks of the file hold only zeros.
/// A sparse restore does not write chunks that hold only zeros.
/// Thus the restored file has its full length only if the restore sets the length of the file one time, before the writes.
///
/// # Errors
///
/// * If the restore fails.
/// * If the restored file does not have the length or the content of the saved file.
/// * If the restored file uses blocks for its full length.
pub fn restore_sparse_without_faults() -> TestResult<()> {
    const MIB: usize = 1024 * 1024;
    let data: Box<[u8]> = content(9, MIB)
        .iter()
        .copied()
        .chain(iter::repeat_n(0, 9 * MIB))
        .collect();
    let saved = save_files(&[("a", &data)])?;
    let dir = tempdir()?;
    let mut opts = RestoreOptions::default();
    // `SparseRestore` is not public, so the scenario sets the option from its serialized name.
    opts.sparse = serde_json::from_str("\"ByContent\"")?;

    restore_with(&saved, dir.path(), &opts, |_| Ok(()))??;

    let path: Box<Path> = dir.path().join("data").join("a").into_boxed_path();
    let restored: Box<[u8]> = fs::read(&path)?.into_boxed_slice();
    if restored.len() != data.len() {
        return Err(format!(
            "The restored file has {} bytes. The saved file has {} bytes.",
            restored.len(),
            data.len()
        )
        .into());
    }
    if restored[..] != data[..] {
        return Err("The restored file is not equal to the saved file.".into());
    }
    check_is_sparse(&path, data.len())
}

/// Checks that the file at `path` uses blocks for less than `len` bytes.
///
/// A file system gives the blocks of a file only on Unix, so this function checks nothing on other
/// systems.
///
/// # Arguments
///
/// * `path` - The path of the file
/// * `len` - The length of the file in bytes
///
/// # Errors
///
/// * If the function cannot read the metadata of the file.
/// * If the file uses blocks for `len` bytes or more.
#[cfg(unix)]
fn check_is_sparse(path: &Path, len: usize) -> TestResult<()> {
    /// The number of bytes of a block that the metadata of a file counts.
    const BLOCK: u64 = 512;

    let blocks = fs::metadata(path)?.blocks();
    let allocated = blocks * BLOCK;
    if allocated >= len as u64 {
        return Err(format!(
            "The restored file uses {allocated} bytes in {blocks} blocks for {len} bytes. A sparse file uses less."
        )
        .into());
    }
    Ok(())
}

/// Checks that the file at `path` uses blocks for less than `len` bytes.
///
/// A file system gives the blocks of a file only on Unix, so this function checks nothing here.
///
/// # Errors
///
/// * This function returns no error.
#[cfg(not(unix))]
fn check_is_sparse(_path: &Path, _len: usize) -> TestResult<()> {
    Ok(())
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

/// A restore whose pack reads give too few bytes returns the error of the short read.
///
/// Each pack read gives one byte less than the restore asks for. The restore uses the data of a
/// read up to the full length, so it needs the check of the length in the backend.
///
/// # Errors
///
/// * If the restore does not return the error of the short read.
pub fn restore_short_pack_read() -> TestResult<()> {
    let data = content(10, 300_000);
    let saved = save_files(&[("a", &data)])?;
    let dir = tempdir()?;

    let result = restore_with(&saved, dir.path(), &RestoreOptions::default(), |_| {
        saved
            .backend
            .inject(|call| is_pack_read(call).then_some(Fault::Truncate));
        Ok(())
    })?;

    expect_error(result, SHORT_READ)
}

/// A tree read of a repository with a cache returns an error when the pack file is too short.
///
/// The scenario shortens each pack file in the backend to one byte, as an interrupted upload
/// leaves it. A repository with a cache reads the full pack file and gives the part that the
/// index names, so it needs the check of the range against the file.
///
/// # Errors
///
/// * If the function cannot shorten the pack files.
/// * If the tree read does not return the error of the short file.
pub fn cached_tree_read_short_pack() -> TestResult<()> {
    let saved = save_files_with_cache(&[("a", &content(11, 5_000))])?;
    shorten_files(&saved.backend, FileType::Pack, 1)?;

    let result = saved
        .repo
        .node_from_snapshot_and_path(&saved.snap, "")
        .and_then(|node| {
            _ = saved.repo.ls(&node, &LsOptions::default())?;
            Ok(())
        });

    expect_error(result, SHORT_FILE)
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
        let sub: Box<Path> = dir.path().join("data").join("sub").into_boxed_path();
        fs::remove_dir(&sub)?;
        fs::write(&sub, b"not a directory")?;
        Ok(())
    })?;

    expect_error(
        result,
        &Path::new("data")
            .join("sub")
            .join("a")
            .display()
            .to_string(),
    )
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

/// A prune whose tree reads fail returns the injected backend error, and no thread panics.
///
/// The scenario saves eight snapshots with different root trees, so the tree loader starts with eight trees.
/// The loader has four threads and a channel with space for four trees.
/// Thus loader threads still send trees after the prune stops at the first error and closes the channel.
///
/// # Errors
///
/// * If a loader thread does not stop in [`RELEASE_TIMEOUT`].
/// * If a thread panics while the prune stops.
/// * If the prune does not return the injected error.
pub fn prune_tree_read() -> TestResult<()> {
    let backend = fault_injection_backend();
    let source = tempdir()?;
    let repo = (0..8).try_fold(
        init_repo(&backend)?.to_indexed_ids()?,
        |repo, seed| -> TestResult<_> {
            let dir: Box<Path> = source.path().join(format!("s{seed}")).into_boxed_path();
            write_files(&dir, &[("a", &content(200 + seed, 1_000))])?;
            let (repo, _) = backup(repo, &dir, "data")?;
            Ok(repo)
        },
    )?;

    let panics = count_panics();
    backend.inject(|call| is_pack_read(call).then_some(Fault::Error));
    let result = repo.prune_plan(&PruneOptions::default());
    drop(repo);
    wait_until_released(&backend, RELEASE_TIMEOUT)?;

    let new_panics = count_panics() - panics;
    if new_panics > 0 {
        return Err(format!("{new_panics} threads panicked while the prune stopped.").into());
    }
    expect_error(result, INJECTED_FAULT)
}

/// A prune stops its remaining tree reads after the first error.
///
/// The scenario saves 40 snapshots with different root trees, so the tree loader starts with 40
/// trees. Then each tree read fails.
///
/// # Errors
///
/// * If a loader thread does not stop in [`RELEASE_TIMEOUT`].
/// * If the prune does not return the injected error.
/// * If the prune reads more trees than the loaders and their channel hold.
pub fn prune_stops_after_first_error() -> TestResult<()> {
    const TREES: usize = 40;

    let backend = fault_injection_backend();
    let source = tempdir()?;
    let repo = (0..TREES).try_fold(
        init_repo(&backend)?.to_indexed_ids()?,
        |repo, seed| -> TestResult<_> {
            let dir: Box<Path> = source.path().join(format!("s{seed}")).into_boxed_path();
            write_files(&dir, &[("a", &content(300 + seed as u64, 1_000))])?;
            let (repo, _) = backup(repo, &dir, "data")?;
            Ok(repo)
        },
    )?;

    let reads = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&reads);
    backend.inject(move |call| {
        is_pack_read(call).then(|| {
            _ = counted.fetch_add(1, Ordering::SeqCst);
            Fault::Error
        })
    });
    let result = repo.prune_plan(&PruneOptions::default());
    drop(repo);
    wait_until_released(&backend, RELEASE_TIMEOUT)?;
    expect_error(result, INJECTED_FAULT)?;

    // Each loader thread holds one tree while it waits to send it, and the channel holds as many
    // trees again. The prune takes one tree out of the channel, which lets one more loader send
    // and read again. Each further read needs a loader that goes on after a failed send.
    let reads = reads.load(Ordering::SeqCst);
    let maximum = 2 * TREE_LOADERS + 2;
    if reads > maximum {
        return Err(format!(
            "The prune read {reads} of {TREES} trees. The maximum is {maximum} reads."
        )
        .into());
    }
    Ok(())
}
