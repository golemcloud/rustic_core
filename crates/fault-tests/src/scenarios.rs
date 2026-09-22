//! Scenarios that inject faults into `rustic_core` operations.
//!
//! A scenario gives `Ok` only when the operation gives the expected result, and no panic occurs.
//! Without faults, the expected result is the saved data. With faults, it is the expected error.

use std::{
    collections::HashSet,
    fs, iter,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(target_os = "linux")]
use std::{
    ffi::OsStr,
    fs::{File, FileTimes},
    os::unix::fs::symlink,
    time::SystemTime,
};

use rustic_core::{BackupOptions, FileType, LsOptions, PruneOptions, RestoreOptions, TreeId};
#[cfg(target_os = "linux")]
use rustic_core::{
    ReadBackend, RusticResult,
    repofile::{MasterKey, Metadata, Node, NodeType, SnapshotFile},
};
#[cfg(target_os = "linux")]
use rustic_testing::backend::fault_injection_backend::FaultInjectionBackend;
use rustic_testing::{
    TestResult,
    backend::fault_injection_backend::{BackendCall, BackendOp, Fault, INJECTED_FAULT},
};
#[cfg(target_os = "linux")]
use tempfile::TempDir;
use tempfile::tempdir;

#[cfg(target_os = "linux")]
use crate::{
    fixtures::{
        ByteCounter, EXTENDED_ATTRIBUTE_VALUE, backup_with_options, expect_xattr,
        init_repo_with_progress, require_non_root, require_xattr_refused, restore_metadata,
        set_extended_attributes, set_mode,
    },
    logging::{capture_warnings, warnings_containing},
    volume::{Tmpfs, enter_nested_user_namespace, enter_user_mount_namespace},
};
use crate::{
    fixtures::{
        CountingSource, SavedRepo, archive_counting_source, backup, content, expect_error,
        fault_injection_backend, init_repo, restore_with, save_files, save_files_in_own_packs,
        save_files_with_cache, shorten_files, write_files,
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
    (
        "restore-stops-after-first-error-with-one-thread",
        restore_stops_after_first_error_with_one_thread,
    ),
    (
        "restore-write-stops-after-first-error",
        restore_write_stops_after_first_error,
    ),
    ("restore-reader-threads", restore_reader_threads),
    (
        "restore-default-reader-threads",
        restore_default_reader_threads,
    ),
    ("backup-threads", backup_threads),
    ("backup-one-thread-same-tree", backup_one_thread_same_tree),
    #[cfg(target_os = "linux")]
    (
        "restore-directory-metadata-after-entries",
        restore_directory_metadata_after_entries,
    ),
    #[cfg(target_os = "linux")]
    ("restore-full-volume", restore_full_volume),
    #[cfg(target_os = "linux")]
    ("backup-unreadable-file", backup_unreadable_file),
    #[cfg(target_os = "linux")]
    ("backup-unreadable-dir", backup_unreadable_dir),
    #[cfg(target_os = "linux")]
    (
        "backup-skips-unreadable-file-without-option",
        backup_skips_unreadable_file_without_option,
    ),
    #[cfg(target_os = "linux")]
    (
        "backup-stops-after-first-error",
        backup_stops_after_first_error,
    ),
    #[cfg(target_os = "linux")]
    ("metadata-symlink", metadata_symlink),
    #[cfg(target_os = "linux")]
    ("metadata-ownership", metadata_ownership),
    #[cfg(target_os = "linux")]
    ("metadata-permission", metadata_permission),
    #[cfg(target_os = "linux")]
    ("metadata-extended-attributes", metadata_extended_attributes),
    #[cfg(target_os = "linux")]
    ("metadata-times", metadata_times),
    #[cfg(target_os = "linux")]
    (
        "metadata-kernel-extended-attributes",
        metadata_kernel_extended_attributes,
    ),
    #[cfg(target_os = "linux")]
    (
        "metadata-privileged-extended-attributes",
        metadata_privileged_extended_attributes,
    ),
    #[cfg(target_os = "linux")]
    (
        "metadata-system-namespace-extended-attributes",
        metadata_system_namespace_extended_attributes,
    ),
    #[cfg(target_os = "linux")]
    (
        "metadata-namespace-prefix-extended-attributes",
        metadata_namespace_prefix_extended_attributes,
    ),
    #[cfg(target_os = "linux")]
    (
        "metadata-replaces-extended-attributes",
        metadata_replaces_extended_attributes,
    ),
    #[cfg(target_os = "linux")]
    (
        "metadata-kernel-attribute-warns-once",
        metadata_kernel_attribute_warns_once,
    ),
    #[cfg(target_os = "linux")]
    ("metadata-selinux-label", metadata_selinux_label),
    #[cfg(target_os = "linux")]
    (
        "metadata-errors-are-warnings-without-option",
        metadata_errors_are_warnings_without_option,
    ),
    #[cfg(target_os = "linux")]
    ("property-no-cache", crate::property::property_no_cache),
    #[cfg(target_os = "linux")]
    ("property-cache", crate::property::property_cache),
    #[cfg(target_os = "linux")]
    (
        "property-full-volume",
        crate::property::property_full_volume,
    ),
    ("prune-tree-read", prune_tree_read),
    (
        "prune-stops-after-first-error",
        prune_stops_after_first_error,
    ),
];

/// The number of reader threads that the scenarios for the stop after the first error set.
const READER_THREADS: usize = 4;

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

/// A restore with [`READER_THREADS`] reader threads stops its remaining pack reads after the first error.
///
/// # Errors
///
/// * If [`stops_after_first_error`] fails.
pub fn restore_stops_after_first_error() -> TestResult<()> {
    stops_after_first_error(READER_THREADS)
}

/// A restore with one reader thread stops its remaining pack reads after the first error.
///
/// The one thread reads the first pack. The read fails, so the thread stores the error, and each
/// next task stops before its read. Thus the restore reads exactly one pack.
///
/// # Errors
///
/// * If [`stops_after_first_error`] fails.
pub fn restore_stops_after_first_error_with_one_thread() -> TestResult<()> {
    stops_after_first_error(1)
}

/// A restore with `threads` reader threads stops its remaining pack reads after the first error.
///
/// The scenario saves 40 files, each in its own pack, and restores the snapshot that uses all 40
/// packs. Each pack read of the restore fails.
///
/// # Arguments
///
/// * `threads` - The number of reader threads of the restore
///
/// # Errors
///
/// * If the snapshot uses too few packs for the check.
/// * If the restore does not return the injected error.
/// * If the restore reads more packs than it has reader threads.
fn stops_after_first_error(threads: usize) -> TestResult<()> {
    let saved = save_files_in_own_packs(40, 100)?;
    let dir = tempdir()?;
    let reads = Arc::new(AtomicUsize::new(0));
    let mut packs = 0;
    let opts = RestoreOptions::default().reader_threads(NonZeroUsize::new(threads));
    let result = restore_with(&saved, dir.path(), &opts, |plan| {
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
    if packs <= threads {
        return Err(format!(
            "The snapshot uses {packs} packs. The check needs more than {threads} packs."
        )
        .into());
    }
    if reads > threads {
        return Err(format!(
            "The restore read {reads} of {packs} packs. The maximum is {threads} reads."
        )
        .into());
    }
    Ok(())
}

/// A restore with one reader thread stops its remaining writes after the first error.
///
/// The scenario saves five files in one backup, so the five blobs are in one pack.
/// Then it finds the file whose blob has the highest offset in the pack, and makes a directory at
/// the path of that file after the plan.
/// The one reader thread reads the pack, and starts one write task for each blob, in the order of
/// the offsets. rayon runs the tasks of a thread in the reverse order of their start, so the write
/// task of that file runs first and fails.
/// Thus each other write task stops before it creates its file.
///
/// # Errors
///
/// * If the five blobs are not in one pack.
/// * If the restore does not return an error with the path of the file whose write fails.
/// * If the restore creates one of the other files.
pub fn restore_write_stops_after_first_error() -> TestResult<()> {
    let files: Box<[_]> = (0..5)
        .map(|index| {
            (
                format!("f{index}").into_boxed_str(),
                content(500 + index, 4_000),
            )
        })
        .collect();
    let refs: Box<[(&str, &[u8])]> = files
        .iter()
        .map(|(name, data)| (&**name, &**data))
        .collect();
    let saved = save_files(&refs)?;

    let root = saved.repo.node_from_snapshot_and_path(&saved.snap, "")?;
    let blobs: Box<[_]> = saved
        .repo
        .ls(&root, &LsOptions::default())?
        .map(|item| -> TestResult<_> {
            let (path, node) = item?;
            node.content
                .as_ref()
                .and_then(|content| content.first())
                .map(|blob| -> TestResult<_> {
                    let entry = saved.repo.get_index_entry(blob)?;
                    Ok((path.into_boxed_path(), entry.pack, entry.location.offset))
                })
                .transpose()
        })
        .filter_map(Result::transpose)
        .collect::<TestResult<_>>()?;
    let (last, pack, _) = blobs
        .iter()
        .max_by_key(|(_, _, offset)| *offset)
        .ok_or("the snapshot has no file")?;
    if blobs.len() != files.len() || blobs.iter().any(|(_, other, _)| other != pack) {
        return Err("The check needs the blobs of all files in one pack.".into());
    }

    let dir = tempdir()?;
    let opts = RestoreOptions::default().reader_threads(NonZeroUsize::new(1));
    let result = restore_with(&saved, dir.path(), &opts, |_| {
        fs::create_dir(dir.path().join(last))?;
        Ok(())
    })?;
    expect_error(result, &last.display().to_string())?;

    blobs
        .iter()
        .filter(|(path, _, _)| path != last)
        .try_for_each(|(path, _, _)| {
            if dir.path().join(path).exists() {
                Err(format!(
                    "The restore created `{}` after the write of `{}` failed.",
                    path.display(),
                    last.display()
                )
                .into())
            } else {
                Ok(())
            }
        })
}

/// The number of reader threads of a restore without the option `reader_threads`.
///
/// This value is `MAX_READER_THREADS_NUM` in `rustic_core`. [`restore_default_reader_threads`] checks it.
const DEFAULT_READER_THREADS: usize = 20;

/// Restores a snapshot that uses 40 packs, and counts the threads that read packs.
///
/// Each pack read waits 10 milliseconds, so the reads of different threads overlap.
/// The function counts only the reads of threads other than the thread that calls the restore.
/// That thread reads the trees of the snapshot from pack files, but no file contents.
///
/// # Arguments
///
/// * `opts` - The restore options
///
/// # Returns
///
/// The number of threads that read packs, and the maximum number of reads that overlapped.
///
/// # Errors
///
/// * If the restore fails.
fn count_reader_threads(opts: &RestoreOptions) -> TestResult<(usize, usize)> {
    let saved = save_files_in_own_packs(40, 400)?;
    let dir = tempdir()?;
    let caller = thread::current().id();
    let readers = Arc::new(Mutex::new(HashSet::new()));
    let in_flight = Arc::new(AtomicUsize::new(0));
    let most = Arc::new(AtomicUsize::new(0));

    let result = restore_with(&saved, dir.path(), opts, |_| {
        let (readers, in_flight, most) = (
            Arc::clone(&readers),
            Arc::clone(&in_flight),
            Arc::clone(&most),
        );
        saved.backend.inject(move |call| {
            let current = thread::current().id();
            if is_pack_read(call) && current != caller {
                _ = readers
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(current);
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                _ = most.fetch_max(now, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(10));
                _ = in_flight.fetch_sub(1, Ordering::SeqCst);
            }
            None
        });
        Ok(())
    })?;
    result?;

    let readers = readers.lock().unwrap_or_else(PoisonError::into_inner).len();
    let most = most.load(Ordering::SeqCst);
    println!("{readers} threads read packs. At most {most} reads overlapped.");
    Ok((readers, most))
}

/// A restore with a number of reader threads uses at most that number of threads for its pack reads.
///
/// The restore has three reader threads. [`count_reader_threads`] describes the measurement.
///
/// # Errors
///
/// * If the restore fails.
/// * If more than three threads read packs, or more than three reads overlap.
pub fn restore_reader_threads() -> TestResult<()> {
    const THREADS: usize = 3;

    let opts = RestoreOptions::default().reader_threads(NonZeroUsize::new(THREADS));
    let (readers, most) = count_reader_threads(&opts)?;
    if readers > THREADS || most > THREADS {
        return Err(format!(
            "{readers} threads read packs, and {most} reads overlapped. The maximum is {THREADS}."
        )
        .into());
    }
    Ok(())
}

/// A restore without the option `reader_threads` uses more than three and at most 20 threads for its pack reads.
///
/// [`count_reader_threads`] describes the measurement. The 40 pack reads overlap, so the 20 reader threads of
/// the default all read packs. More than three threads shows that the default is not a small number of
/// threads, and at most 20 shows that it is not more than 20.
///
/// # Errors
///
/// * If the restore fails.
/// * If three threads or fewer read packs, or more than 20 threads read packs, or more than 20 reads overlap.
pub fn restore_default_reader_threads() -> TestResult<()> {
    const MINIMUM: usize = 4;

    let (readers, most) = count_reader_threads(&RestoreOptions::default())?;
    if !(MINIMUM..=DEFAULT_READER_THREADS).contains(&readers) || most > DEFAULT_READER_THREADS {
        return Err(format!(
            "{readers} threads read packs, and {most} reads overlapped. The default needs at least {MINIMUM} and at most {DEFAULT_READER_THREADS} threads."
        )
        .into());
    }
    Ok(())
}

/// The number of files of the backups that count their reads.
const BACKUP_FILES: usize = 64;

/// The number of threads of [`backup_threads`].
const BACKUP_THREADS: NonZeroUsize = NonZeroUsize::new(2).unwrap();

/// The number of threads of the control of [`backup_threads`]. It must be higher than [`BACKUP_THREADS`].
const BACKUP_CONTROL_THREADS: NonZeroUsize = NonZeroUsize::new(6).unwrap();

/// Backs up [`BACKUP_FILES`] files through `Repository::archive`, and counts the files that the backup reads at the same time.
///
/// The first read of each file waits [`READ_PAUSE`](crate::fixtures::READ_PAUSE), so the reads of different
/// threads overlap.
/// A read is live from `open` until the drop of the reader, which the archiver does in its parallel stage.
///
/// # Arguments
///
/// * `threads` - The value of `BackupOptions::threads`
///
/// # Returns
///
/// The maximum number of reads that overlapped, and the tree of the snapshot.
///
/// # Errors
///
/// * If the backup fails.
/// * If the backup did not open each file one time, or a file is still open after the backup.
fn count_backup_reads(threads: Option<NonZeroUsize>) -> TestResult<(usize, TreeId)> {
    let source = CountingSource::new(BACKUP_FILES as u64);
    let opts = BackupOptions::default().threads(threads);
    let snap = archive_counting_source(&source, &opts)?;
    let counter = source.counter();
    let (opened, live, peak) = (counter.opened(), counter.live(), counter.peak());
    println!("The backup opened {opened} files. At most {peak} reads overlapped.");
    if opened != BACKUP_FILES || live != 0 {
        return Err(format!(
            "The backup opened {opened} of {BACKUP_FILES} files, and {live} files are still open."
        )
        .into());
    }
    Ok((peak, snap.tree))
}

/// A backup with the option `threads` reads at most that number of files at the same time.
///
/// The backup has [`BACKUP_THREADS`] threads. A control with [`BACKUP_CONTROL_THREADS`] threads must read more
/// than [`BACKUP_THREADS`] files at the same time, so the measurement can see a number of threads that the
/// backup ignores. [`count_backup_reads`] describes the measurement.
///
/// # Errors
///
/// * If a backup fails.
/// * If the backup with [`BACKUP_THREADS`] threads reads more than [`BACKUP_THREADS`] files at the same time.
/// * If the backup with [`BACKUP_CONTROL_THREADS`] threads reads [`BACKUP_THREADS`] files or fewer, or more than
///   [`BACKUP_CONTROL_THREADS`] files, at the same time.
pub fn backup_threads() -> TestResult<()> {
    let (peak, _) = count_backup_reads(Some(BACKUP_THREADS))?;
    if !(1..=BACKUP_THREADS.get()).contains(&peak) {
        return Err(format!(
            "{peak} reads overlapped with {BACKUP_THREADS} threads. The maximum is {BACKUP_THREADS}."
        )
        .into());
    }
    let (control, _) = count_backup_reads(Some(BACKUP_CONTROL_THREADS))?;
    if !(BACKUP_THREADS.get() + 1..=BACKUP_CONTROL_THREADS.get()).contains(&control) {
        return Err(format!(
            "{control} reads overlapped with {BACKUP_CONTROL_THREADS} threads. The control needs more than {BACKUP_THREADS} and at most {BACKUP_CONTROL_THREADS}."
        )
        .into());
    }
    Ok(())
}

/// A backup with one thread reads one file at a time, and gives the tree of a backup without the option `threads`.
///
/// # Errors
///
/// * If a backup fails.
/// * If the backup with one thread reads more than one file at the same time.
/// * If the two backups give different trees.
pub fn backup_one_thread_same_tree() -> TestResult<()> {
    let (peak, one) = count_backup_reads(Some(NonZeroUsize::MIN))?;
    if peak != 1 {
        return Err(format!("{peak} reads overlapped with one thread.").into());
    }
    let (_, default) = count_backup_reads(None)?;
    if one != default {
        return Err(format!(
            "The backup with one thread gives the tree `{one}`, and the backup without the option gives `{default}`."
        )
        .into());
    }
    Ok(())
}

/// A restore sets the metadata of a directory after it restores the entries of the directory.
///
/// The directory `d` holds the symlink `d/l` and has an old modification time.
/// The restore creates the symlink in its metadata step, and the creation changes the modification
/// time of `d`. Thus `d` has the saved modification time only if the restore sets the times of `d`
/// after it creates `d/l`.
///
/// # Errors
///
/// * If the backup or the restore fails.
/// * If the restored `d/l` is not a symlink with the saved target.
/// * If the restored `d` does not have the saved modification time.
#[cfg(target_os = "linux")]
pub fn restore_directory_metadata_after_entries() -> TestResult<()> {
    let source = tempdir()?;
    let dir: Box<Path> = source.path().join("d").into_boxed_path();
    fs::create_dir(&dir)?;
    symlink("target", dir.join("l"))?;
    File::open(&dir)?.set_times(
        FileTimes::new().set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000_000)),
    )?;
    let saved_mtime = fs::symlink_metadata(&dir)?.modified()?;

    let backend = fault_injection_backend();
    let (repo, snap) = backup(init_repo(&backend)?, source.path(), "data")?;
    let saved = SavedRepo {
        backend,
        repo: repo.to_indexed()?,
        snap,
    };
    let dest = tempdir()?;
    restore_with(&saved, dest.path(), &RestoreOptions::default(), |_| Ok(()))??;

    let restored: Box<Path> = dest.path().join("data").join("d").into_boxed_path();
    let target = fs::read_link(restored.join("l"))?;
    if target != Path::new("target") {
        return Err(format!(
            "The restored symlink `d/l` points to `{}`. The saved target is `target`.",
            target.display()
        )
        .into());
    }
    let restored_mtime = fs::symlink_metadata(&restored)?.modified()?;
    if restored_mtime != saved_mtime {
        return Err(format!(
            "The restored directory `d` has the modification time {restored_mtime:?}. The saved time is {saved_mtime:?}."
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

/// Backs up `source` into a new repository, with the path `data` and with `fail_on_read_error`.
///
/// # Returns
///
/// The backend of the repository, and the result of the backup.
///
/// # Errors
///
/// * If the function cannot create the repository or read its index.
#[cfg(target_os = "linux")]
fn strict_backup(
    source: &Path,
) -> TestResult<(Arc<FaultInjectionBackend>, RusticResult<SnapshotFile>)> {
    let backend = fault_injection_backend();
    let opts = BackupOptions::default()
        .as_path(PathBuf::from("data"))
        .fail_on_read_error(true);
    let (_, result) = backup_with_options(init_repo(&backend)?, source, &opts)?;
    Ok((backend, result))
}

/// Checks that `backend` holds no snapshot file.
///
/// # Errors
///
/// * If the function cannot list the snapshot files.
/// * If `backend` holds a snapshot file.
#[cfg(target_os = "linux")]
fn expect_no_snapshot(backend: &Arc<FaultInjectionBackend>) -> TestResult<()> {
    let snapshots = backend.list(FileType::Snapshot)?.len();
    if snapshots == 0 {
        Ok(())
    } else {
        Err(format!("The failed backup wrote {snapshots} snapshot files.").into())
    }
}

/// Writes the files `a`, `b` and `c` into `dir`, and removes all permissions of `b`.
///
/// # Returns
///
/// The path of `b`.
///
/// # Errors
///
/// * If the function cannot write the files or set the permissions.
#[cfg(target_os = "linux")]
fn write_unreadable_file(dir: &Path) -> TestResult<Box<Path>> {
    write_files(
        dir,
        &[
            ("a", &content(20, 1_000)),
            ("b", &content(21, 1_000)),
            ("c", &content(22, 1_000)),
        ],
    )?;
    let unreadable: Box<Path> = dir.join("b").into_boxed_path();
    set_mode(&unreadable, 0o000)?;
    Ok(unreadable)
}

/// A backup with `fail_on_read_error` returns an error for a file that it cannot open, and writes no snapshot file.
///
/// # Errors
///
/// * If this process runs as root.
/// * If the backup does not return an error with the path of the file.
/// * If the backup writes a snapshot file.
#[cfg(target_os = "linux")]
pub fn backup_unreadable_file() -> TestResult<()> {
    require_non_root()?;
    let source = tempdir()?;
    let unreadable = write_unreadable_file(source.path())?;

    let result = strict_backup(source.path());
    set_mode(&unreadable, 0o644)?;
    let (backend, result) = result?;

    expect_error(result, &unreadable.display().to_string())?;
    expect_no_snapshot(&backend)
}

/// A backup with `fail_on_read_error` returns an error for a directory that it cannot list, and writes no snapshot file.
///
/// # Errors
///
/// * If this process runs as root.
/// * If the backup does not return an error with the path of the directory.
/// * If the backup writes a snapshot file.
#[cfg(target_os = "linux")]
pub fn backup_unreadable_dir() -> TestResult<()> {
    require_non_root()?;
    let source = tempdir()?;
    write_files(
        source.path(),
        &[("a", &content(23, 1_000)), ("d/b", &content(24, 1_000))],
    )?;
    let unreadable: Box<Path> = source.path().join("d").into_boxed_path();
    set_mode(&unreadable, 0o000)?;

    let result = strict_backup(source.path());
    set_mode(&unreadable, 0o755)?;
    let (backend, result) = result?;

    expect_error(result, &unreadable.display().to_string())?;
    expect_no_snapshot(&backend)
}

/// A backup without `fail_on_read_error` skips a file that it cannot open, and writes the snapshot file.
///
/// # Errors
///
/// * If this process runs as root.
/// * If the backup fails.
/// * If the snapshot contains the unreadable file.
#[cfg(target_os = "linux")]
pub fn backup_skips_unreadable_file_without_option() -> TestResult<()> {
    require_non_root()?;
    let source = tempdir()?;
    let unreadable = write_unreadable_file(source.path())?;
    let backend = fault_injection_backend();

    let result = backup(init_repo(&backend)?, source.path(), "data");
    set_mode(&unreadable, 0o644)?;
    let (repo, snap) = result?;

    let repo = repo.to_indexed()?;
    let node = repo.node_from_snapshot_and_path(&snap, "")?;
    let names: Box<[Box<Path>]> = repo
        .ls(&node, &LsOptions::default())?
        .map(|item| Ok(item?.0.into_boxed_path()))
        .collect::<RusticResult<_>>()?;
    if names.iter().any(|name| name.ends_with("b")) {
        return Err("The snapshot contains the unreadable file `b`.".into());
    }
    if backend.list(FileType::Snapshot)?.len() != 1 {
        return Err("The backend does not hold one snapshot file.".into());
    }
    Ok(())
}

/// A backup with `fail_on_read_error` stops reading its source after the first error.
///
/// The first file of the source cannot be read. Many files come after it.
/// The backup reads files in parallel, so it reads some files after the first error.
/// pariter gives its parallel map a buffer of 2 × `available_parallelism` items. The readahead
/// thread holds one more item, and the source can give one more item while the first error is
/// stored. That sum is the bound. The source has ten times as many files after the first file.
///
/// # Errors
///
/// * If this process runs as root.
/// * If the backup does not return an error with the path of the first file.
/// * If the backup reads more files after the first file than the bound.
#[cfg(target_os = "linux")]
pub fn backup_stops_after_first_error() -> TestResult<()> {
    const SIZE: usize = 100;

    require_non_root()?;
    let threads = thread::available_parallelism()?.get();
    let bound = 2 * threads + 2;
    let later = 10 * bound;
    let source = tempdir()?;
    write_files(source.path(), &[("a", b"unreadable")])?;
    (0..later).try_for_each(|index| {
        write_files(
            source.path(),
            &[(
                &format!("f{index:05}"),
                &content(1_000 + index as u64, SIZE),
            )],
        )
    })?;
    let unreadable: Box<Path> = source.path().join("a").into_boxed_path();
    set_mode(&unreadable, 0o000)?;

    let counter = ByteCounter::default();
    let backend = fault_injection_backend();
    let opts = BackupOptions::default()
        .as_path(PathBuf::from("data"))
        .fail_on_read_error(true);
    let result = init_repo_with_progress(&backend, &MasterKey::new(), counter.clone())
        .and_then(|repo| backup_with_options(repo, source.path(), &opts));
    set_mode(&unreadable, 0o644)?;
    let (_, result) = result?;
    expect_error(result, &unreadable.display().to_string())?;

    let read = counter.bytes() / SIZE as u64;
    println!(
        "The backup read {read} of {later} files after the first error. The bound is {bound}."
    );
    if read > bound as u64 {
        return Err(format!(
            "The backup read {read} of {later} files after the first error. The bound is {bound}."
        )
        .into());
    }
    Ok(())
}

/// Creates a node of a file with the name `name`, without metadata.
#[cfg(target_os = "linux")]
fn file_node(name: &str) -> Node {
    Node::new_node(OsStr::new(name), NodeType::File, Metadata::default())
}

/// The destination directory of a [`MetadataCase`].
#[cfg(target_os = "linux")]
#[derive(Debug)]
enum Destination {
    /// A temporary directory that the case owns.
    Temporary(TempDir),
    /// A directory of the system.
    System(&'static str),
}

#[cfg(target_os = "linux")]
impl Destination {
    /// Gives the path of the directory.
    fn path(&self) -> &Path {
        match self {
            Self::Temporary(dir) => dir.path(),
            Self::System(path) => Path::new(path),
        }
    }
}

/// A restore whose metadata step fails in one operation, with the error that it gives.
///
/// A metadata scenario runs the case with `fail_on_metadata_error`, and the control scenario runs the
/// same case without the option. Thus both use the same nodes and the same options.
#[cfg(target_os = "linux")]
#[derive(Debug)]
struct MetadataCase {
    /// The destination directory.
    dest: Destination,
    /// The restore options without `fail_on_metadata_error`.
    opts: RestoreOptions,
    /// The nodes to restore, with their paths relative to `root`.
    nodes: Box<[(PathBuf, Node)]>,
    /// Part of the message of the error that the case gives with `fail_on_metadata_error`.
    expected: &'static str,
}

#[cfg(target_os = "linux")]
impl MetadataCase {
    /// Restores the nodes of the case with the value `fail` for `fail_on_metadata_error`.
    ///
    /// # Returns
    ///
    /// The result of the restore.
    ///
    /// # Errors
    ///
    /// * If the function cannot create the empty repository or the destination.
    fn restore(&self, fail: bool) -> TestResult<RusticResult<()>> {
        restore_metadata(
            self.dest.path(),
            &self.opts.fail_on_metadata_error(fail),
            self.nodes.clone(),
        )
    }

    /// A case that cannot set the owner of a file.
    ///
    /// The node of the file has a user ID that is not the user ID of this process.
    ///
    /// # Errors
    ///
    /// * If the function cannot write the file or read its metadata.
    fn ownership() -> TestResult<Self> {
        let dir = tempdir()?;
        write_files(dir.path(), &[("a", b"data")])?;
        let mut node = file_node("a");
        node.meta.uid = Some(fs::metadata(dir.path().join("a"))?.uid() + 1);
        Ok(Self {
            dest: Destination::Temporary(dir),
            opts: RestoreOptions::default().numeric_id(true),
            nodes: Box::new([(PathBuf::from("a"), node)]),
            expected: "cannot set the owner",
        })
    }

    /// A case that cannot set the permissions of a file.
    ///
    /// The node has permissions, but the destination has no file at the path of the node.
    ///
    /// # Errors
    ///
    /// * If the function cannot create the destination.
    fn permission() -> TestResult<Self> {
        let dir = tempdir()?;
        let mut node = file_node("missing");
        node.meta.mode = Some(0o644);
        Ok(Self {
            dest: Destination::Temporary(dir),
            opts: RestoreOptions::default().no_ownership(true),
            nodes: Box::new([(PathBuf::from("missing"), node)]),
            expected: "cannot set the permissions",
        })
    }

    /// A case that cannot set an extended attribute.
    ///
    /// The node has an extended attribute whose name has no namespace. Linux refuses such a name.
    ///
    /// # Errors
    ///
    /// * If the function cannot write the file.
    fn extended_attributes() -> TestResult<Self> {
        Self::extended_attribute_case("golem")
    }

    /// A case that cannot set an extended attribute of a namespace that needs privilege.
    ///
    /// The node has an extended attribute of the namespace `trusted.`, which needs `CAP_SYS_ADMIN` in the
    /// initial user namespace. The kernel refuses that set with the same error as a set of an attribute that
    /// it owns, so the case shows that the rule of the restore follows the namespace and not the error.
    ///
    /// # Errors
    ///
    /// * If the function cannot write the file.
    fn privileged_extended_attributes() -> TestResult<Self> {
        Self::extended_attribute_case("trusted.rustic")
    }

    /// A case that cannot set an extended attribute of the namespace `system.`.
    ///
    /// The namespace `system.` is not in the rule of the restore, although the kernel uses it. A POSIX ACL
    /// lives there, under the names `system.posix_acl_access` and `system.posix_acl_default`, and the owner of
    /// a file sets an ACL without privilege. An ACL is part of the filesystem that the agent had, so a failed
    /// set of one must stay an error. The node of this case has a `system.` name that the filesystem does not
    /// implement, which every user, also root, cannot set.
    ///
    /// # Errors
    ///
    /// * If the function cannot write the file.
    fn system_namespace_extended_attributes() -> TestResult<Self> {
        Self::extended_attribute_case("system.rustic")
    }

    /// A case that cannot set an extended attribute whose name is the kernel namespace without its point.
    ///
    /// The name `security` has no namespace, and Linux refuses such a name. The case shows the boundary of
    /// the rule of the restore: the rule covers the namespace `security.`, and not each name that starts with
    /// the letters of that namespace.
    ///
    /// # Errors
    ///
    /// * If the function cannot write the file.
    fn namespace_prefix_extended_attributes() -> TestResult<Self> {
        Self::extended_attribute_case("security")
    }

    /// A case whose node has the extended attribute `name`, which the kernel refuses to set.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the extended attribute of the node
    ///
    /// # Errors
    ///
    /// * If the function cannot write the file or build the extended attribute.
    fn extended_attribute_case(name: &str) -> TestResult<Self> {
        let dir = tempdir()?;
        write_files(dir.path(), &[("a", b"data")])?;
        let mut node = file_node("a");
        set_extended_attributes(&mut node, &[name])?;
        Ok(Self {
            dest: Destination::Temporary(dir),
            opts: RestoreOptions::default().no_ownership(true),
            nodes: Box::new([(PathBuf::from("a"), node)]),
            expected: "cannot set the extended attributes",
        })
    }

    /// A case that cannot set the times of a file.
    ///
    /// Each step before the times needs the same permission on the same path as the times, so the
    /// case uses a file that this process does not own: `/proc/version`. The node has no
    /// permissions and no extended attributes, so the restore only lists the extended attributes of the
    /// file before it sets the times. The kernel refuses new times of a file to a user that does not
    /// own the file.
    ///
    /// # Errors
    ///
    /// * If the function cannot convert the time.
    fn times() -> TestResult<Self> {
        let mut node = file_node("version");
        node.meta.mtime = Some(SystemTime::UNIX_EPOCH.try_into()?);
        Ok(Self {
            dest: Destination::System("/proc"),
            opts: RestoreOptions::default().no_ownership(true),
            nodes: Box::new([(PathBuf::from("version"), node)]),
            expected: "cannot set the times",
        })
    }
}

/// Saves a snapshot with the symlink `link`, and creates a destination that already has a file at that path.
///
/// # Returns
///
/// The repository with the snapshot, and the destination.
///
/// # Errors
///
/// * If the function cannot write the source or the destination.
/// * If the function cannot create the repository, or the backup fails.
#[cfg(target_os = "linux")]
fn symlink_case() -> TestResult<(SavedRepo, TempDir)> {
    let backend = fault_injection_backend();
    let source = tempdir()?;
    write_files(source.path(), &[("target", &content(30, 1_000))])?;
    symlink("target", source.path().join("link"))?;
    let (repo, snap) = backup(init_repo(&backend)?, source.path(), "data")?;
    let saved = SavedRepo {
        backend,
        repo: repo.to_indexed()?,
        snap,
    };
    let dir = tempdir()?;
    write_files(&dir.path().join("data"), &[("link", b"not a symlink")])?;
    Ok((saved, dir))
}

/// A restore that returns metadata errors fails when it cannot create a symlink.
///
/// [`symlink_case`] describes the snapshot and the destination.
///
/// # Errors
///
/// * If the restore does not return the error for the symlink.
#[cfg(target_os = "linux")]
pub fn metadata_symlink() -> TestResult<()> {
    let (saved, dir) = symlink_case()?;
    let opts = RestoreOptions::default().fail_on_metadata_error(true);
    let result = restore_with(&saved, dir.path(), &opts, |_| Ok(()))?;
    expect_error(result, "cannot create the symlink or special file")
}

/// Runs `case` with `fail_on_metadata_error`, and checks that the restore gives the error of the case.
///
/// # Errors
///
/// * If the restore does not return the error of the case.
#[cfg(target_os = "linux")]
fn expect_metadata_error(case: &MetadataCase) -> TestResult<()> {
    expect_error(case.restore(true)?, case.expected)
}

/// A restore that returns metadata errors fails when it cannot set the owner of a file.
///
/// [`MetadataCase::ownership`] describes the case.
///
/// # Errors
///
/// * If this process runs as root.
/// * If the restore does not return the error for the owner.
#[cfg(target_os = "linux")]
pub fn metadata_ownership() -> TestResult<()> {
    require_non_root()?;
    expect_metadata_error(&MetadataCase::ownership()?)
}

/// A restore that returns metadata errors fails when it cannot set the permissions of a file.
///
/// [`MetadataCase::permission`] describes the case.
///
/// # Errors
///
/// * If the restore does not return the error for the permissions.
#[cfg(target_os = "linux")]
pub fn metadata_permission() -> TestResult<()> {
    expect_metadata_error(&MetadataCase::permission()?)
}

/// A restore that returns metadata errors fails when it cannot set an extended attribute.
///
/// [`MetadataCase::extended_attributes`] describes the case.
///
/// # Errors
///
/// * If the restore does not return the error for the extended attributes.
#[cfg(target_os = "linux")]
pub fn metadata_extended_attributes() -> TestResult<()> {
    expect_metadata_error(&MetadataCase::extended_attributes()?)
}

/// A restore that returns metadata errors fails when it cannot set the times of a file.
///
/// [`MetadataCase::times`] describes the case.
///
/// # Errors
///
/// * If this process runs as root.
/// * If the restore does not return the error for the times.
#[cfg(target_os = "linux")]
pub fn metadata_times() -> TestResult<()> {
    require_non_root()?;
    expect_metadata_error(&MetadataCase::times()?)
}

/// The name of the extended attribute that the kernel owns in the scenarios of the rule.
///
/// No security module claims the name, so a set of it needs `CAP_SYS_ADMIN` on each kernel, also on a kernel
/// that lets the owner of a file set `security.selinux`.
#[cfg(target_os = "linux")]
const KERNEL_XATTR_NAME: &str = "security.rustic_skip";

/// The name of the extended attribute that the restore must set on the file of the kernel-owned attribute.
#[cfg(target_os = "linux")]
const USER_XATTR_NAME: &str = "user.rustic_kept";

/// The name of the extended attribute that each file of [`metadata_kernel_attribute_warns_once`] has.
///
/// No other scenario uses this name, so the count of the warnings that name it holds for a test harness that
/// runs the scenarios at the same time.
#[cfg(target_os = "linux")]
const ONCE_XATTR_NAME: &str = "security.rustic_once";

/// The name of the second extended attribute that the last file of that scenario has.
///
/// The kernel owns this name too. A restore that keeps only the fact that it already wrote a warning would
/// skip this name without a line, and thus lose the report of a second kernel-owned attribute, for example a
/// file capability after a label.
#[cfg(target_os = "linux")]
const SECOND_XATTR_NAME: &str = "security.rustic_second";

/// The name of the label of a volume that is mounted with `seclabel`.
#[cfg(target_os = "linux")]
const SELINUX_XATTR_NAME: &str = "security.selinux";

/// The value that [`metadata_selinux_label`] gives to the label of the destination file.
#[cfg(target_os = "linux")]
const SELINUX_LABEL: &[u8] = b"system_u:object_r:container_file_t:s0\0";

/// The number of files of [`metadata_kernel_attribute_warns_once`].
#[cfg(target_os = "linux")]
const WARN_ONCE_FILES: usize = 10_000;

/// Restores a file whose node has an extended attribute that the kernel owns, and one of the namespace `user.`.
///
/// The destination has the file without extended attributes, so the restore sets both attributes. The kernel
/// refuses the set of the attribute that it owns. The restore must keep the file without that attribute, set
/// the attribute of the namespace `user.`, and give `Ok`.
///
/// # Arguments
///
/// * `fail_on_metadata_error` - The value of the option of the restore
///
/// # Errors
///
/// * If the kernel lets this process set the attribute that the kernel owns.
/// * If the restore fails.
/// * If the destination file has the attribute that the kernel owns, or does not have the other attribute.
#[cfg(target_os = "linux")]
fn restore_kernel_extended_attributes(fail_on_metadata_error: bool) -> TestResult<()> {
    let dir = tempdir()?;
    write_files(dir.path(), &[("a", b"data")])?;
    let path = dir.path().join("a");
    require_xattr_refused(&path, KERNEL_XATTR_NAME)?;

    let mut node = file_node("a");
    set_extended_attributes(&mut node, &[KERNEL_XATTR_NAME, USER_XATTR_NAME])?;
    let opts = RestoreOptions::default()
        .no_ownership(true)
        .fail_on_metadata_error(fail_on_metadata_error);
    restore_metadata(dir.path(), &opts, Box::new([(PathBuf::from("a"), node)]))??;

    expect_xattr(&path, KERNEL_XATTR_NAME, None)?;
    expect_xattr(&path, USER_XATTR_NAME, Some(EXTENDED_ATTRIBUTE_VALUE))
}

/// A restore that returns metadata errors gives `Ok` when it cannot set an attribute that the kernel owns.
///
/// [`restore_kernel_extended_attributes`] describes the case.
///
/// The destination file of the case does not have the attribute, so the scenario covers the set that follows
/// the listing. The other set, which replaces the value of an attribute that the destination file already
/// has, needs a kernel-owned attribute on that file, and only a process with privileges can give it. So only
/// [`metadata_selinux_label`], which the binary of this crate runs, covers that set with a kernel-owned
/// attribute. [`metadata_replaces_extended_attributes`] covers it with an attribute of the namespace `user.`.
///
/// # Errors
///
/// * If this process runs as root.
/// * If the restore does not give `Ok`, or does not set the other attribute of the file.
#[cfg(target_os = "linux")]
pub fn metadata_kernel_extended_attributes() -> TestResult<()> {
    require_non_root()?;
    restore_kernel_extended_attributes(true)
}

/// A restore that returns metadata errors fails when it cannot set an attribute of a privileged namespace.
///
/// [`MetadataCase::privileged_extended_attributes`] describes the case.
///
/// # Errors
///
/// * If this process runs as root.
/// * If the restore does not return the error for the extended attributes.
#[cfg(target_os = "linux")]
pub fn metadata_privileged_extended_attributes() -> TestResult<()> {
    require_non_root()?;
    expect_metadata_error(&MetadataCase::privileged_extended_attributes()?)
}

/// A restore that returns metadata errors fails when it cannot set an attribute of the namespace `system.`.
///
/// [`MetadataCase::system_namespace_extended_attributes`] describes the case.
///
/// # Errors
///
/// * If the restore does not return the error for the extended attributes.
#[cfg(target_os = "linux")]
pub fn metadata_system_namespace_extended_attributes() -> TestResult<()> {
    expect_metadata_error(&MetadataCase::system_namespace_extended_attributes()?)
}

/// A restore that returns metadata errors fails when it cannot set the attribute `security`.
///
/// [`MetadataCase::namespace_prefix_extended_attributes`] describes the case.
///
/// # Errors
///
/// * If the restore does not return the error for the extended attributes.
#[cfg(target_os = "linux")]
pub fn metadata_namespace_prefix_extended_attributes() -> TestResult<()> {
    expect_metadata_error(&MetadataCase::namespace_prefix_extended_attributes()?)
}

/// A restore replaces the value of an extended attribute that the destination file already has.
///
/// The destination file has the attribute of the namespace `user.` with another value, so the restore lists
/// the attributes of the file, reads the value, sees the difference, and sets the value of the node. This is
/// the path that a restore takes on a file that already has the attribute of the node, and it is the path
/// that fails on a volume that is mounted with `seclabel`.
///
/// # Errors
///
/// * If the restore fails, or does not replace the value.
#[cfg(target_os = "linux")]
pub fn metadata_replaces_extended_attributes() -> TestResult<()> {
    let dir = tempdir()?;
    write_files(dir.path(), &[("a", b"data")])?;
    let path = dir.path().join("a");
    xattr::set(&path, USER_XATTR_NAME, b"old")?;

    let mut node = file_node("a");
    set_extended_attributes(&mut node, &[USER_XATTR_NAME])?;
    let opts = RestoreOptions::default()
        .no_ownership(true)
        .fail_on_metadata_error(true);
    restore_metadata(dir.path(), &opts, Box::new([(PathBuf::from("a"), node)]))??;

    expect_xattr(&path, USER_XATTR_NAME, Some(EXTENDED_ATTRIBUTE_VALUE))
}

/// A restore of 10,000 files writes one warning for each attribute name that the kernel owns.
///
/// Each file of the destination gets the same extended attribute that the kernel owns, and the last file gets
/// a second such attribute. One warning for each file hides everything else in the log of an executor, so the
/// restore must write one warning for each name, and each warning must name its attribute. The second name
/// shows that the restore reports each name: a restore that only keeps whether it already wrote a warning
/// would skip the second name without a line.
///
/// # Errors
///
/// * If this process runs as root.
/// * If the process cannot keep its warnings.
/// * If the kernel lets this process set the attribute that the kernel owns.
/// * If the restore fails.
/// * If the number of warnings that name an attribute is not 1.
#[cfg(target_os = "linux")]
pub fn metadata_kernel_attribute_warns_once() -> TestResult<()> {
    require_non_root()?;
    capture_warnings()?;

    let dir = tempdir()?;
    let names: Box<[Box<str>]> = (0..WARN_ONCE_FILES)
        .map(|index| format!("f{index:05}").into_boxed_str())
        .collect();
    let files: Box<[(&str, &[u8])]> = names
        .iter()
        .map(|name| (&**name, b"data".as_slice()))
        .collect();
    write_files(dir.path(), &files)?;
    require_xattr_refused(&dir.path().join(&*names[0]), ONCE_XATTR_NAME)?;

    let last = WARN_ONCE_FILES - 1;
    let nodes: Box<[(PathBuf, Node)]> = names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let mut node = file_node(name);
            if index == last {
                set_extended_attributes(&mut node, &[ONCE_XATTR_NAME, SECOND_XATTR_NAME])?;
            } else {
                set_extended_attributes(&mut node, &[ONCE_XATTR_NAME])?;
            }
            Ok((PathBuf::from(&**name), node))
        })
        .collect::<TestResult<_>>()?;
    let opts = RestoreOptions::default()
        .no_ownership(true)
        .fail_on_metadata_error(true);
    restore_metadata(dir.path(), &opts, nodes)??;

    [ONCE_XATTR_NAME, SECOND_XATTR_NAME]
        .iter()
        .try_for_each(|name| -> TestResult<()> {
            let warnings = warnings_containing(name);
            if warnings == 1 {
                Ok(())
            } else {
                Err(format!(
                    "The restore of {WARN_ONCE_FILES} files wrote {warnings} warnings that name `{name}`, but one was expected."
                )
                .into())
            }
        })
}

/// A restore that returns metadata errors gives `Ok` when it cannot set the label of the destination file.
///
/// The scenario builds the case of the cluster. A volume that is mounted with `seclabel` gives each file the
/// label `security.selinux`, a backup records the label, and the restore then lists the attributes of the
/// destination file, reads its label, and sets the label of the snapshot, because the values differ. A pod
/// that runs without privileges cannot set that label, so the kernel refuses the call.
///
/// The process builds the same case without privileges. It mounts a tmpfs in its own user namespace, where it
/// can give the label to the destination file, and then it moves into a nested user namespace, which removes
/// its privileges over that tmpfs. The listing and the read of the label still work, the set fails, and the
/// process can still write the file and set its other extended attributes.
///
/// The nested user namespace also removes the privilege to unmount, so the scenario cannot unmount the tmpfs
/// and leaves its empty mount point in the directory for temporary files. The tmpfs itself stops with the
/// process. The scenario does not use a mount point of its own name, because two runs of the binary would
/// then use the same path.
///
/// Only the binary of this crate runs this scenario, because a new user namespace needs a process that has
/// one thread.
///
/// # Errors
///
/// * If the process cannot enter the namespaces, mount the tmpfs or give the label to the file.
/// * If the kernel lets this process set the label after the nested namespace.
/// * If the restore fails.
/// * If the label of the file changed, or the file does not have the other attribute.
#[cfg(target_os = "linux")]
pub fn metadata_selinux_label() -> TestResult<()> {
    enter_user_mount_namespace()?;
    let mount_dir = tempdir()?;
    let volume = Tmpfs::mount(mount_dir.path(), 1_000_000)?;
    write_files(volume.path(), &[("a", b"data")])?;
    let path = volume.path().join("a");
    xattr::set(&path, SELINUX_XATTR_NAME, SELINUX_LABEL)?;

    enter_nested_user_namespace()?;
    require_xattr_refused(&path, SELINUX_XATTR_NAME)?;

    let mut node = file_node("a");
    set_extended_attributes(&mut node, &[SELINUX_XATTR_NAME, USER_XATTR_NAME])?;
    let opts = RestoreOptions::default()
        .no_ownership(true)
        .fail_on_metadata_error(true);
    restore_metadata(volume.path(), &opts, Box::new([(PathBuf::from("a"), node)]))??;

    expect_xattr(&path, SELINUX_XATTR_NAME, Some(SELINUX_LABEL))?;
    expect_xattr(&path, USER_XATTR_NAME, Some(EXTENDED_ATTRIBUTE_VALUE))
}

/// A restore that does not return metadata errors succeeds when it cannot set metadata.
///
/// The scenario runs the cases of the other metadata scenarios without the option.
/// Root can set the owner and the times of these cases, so the scenario needs a user other than root.
///
/// # Errors
///
/// * If this process runs as root.
/// * If a restore fails.
#[cfg(target_os = "linux")]
pub fn metadata_errors_are_warnings_without_option() -> TestResult<()> {
    require_non_root()?;
    let (saved, dir) = symlink_case()?;
    restore_with(&saved, dir.path(), &RestoreOptions::default(), |_| Ok(()))??;
    restore_kernel_extended_attributes(false)?;
    [
        MetadataCase::ownership()?,
        MetadataCase::permission()?,
        MetadataCase::extended_attributes()?,
        MetadataCase::privileged_extended_attributes()?,
        MetadataCase::system_namespace_extended_attributes()?,
        MetadataCase::namespace_prefix_extended_attributes()?,
        MetadataCase::times()?,
    ]
    .iter()
    .try_for_each(|case| -> TestResult<()> {
        case.restore(false)?.map_err(|err| {
            format!(
                "The case for `{}` failed without the option:\n{err}",
                case.expected
            )
        })?;
        Ok(())
    })
}
