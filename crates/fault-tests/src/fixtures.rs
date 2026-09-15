//! Repositories, source trees and checks for the fault scenarios.

use std::{
    fs, iter,
    path::{Path, PathBuf},
    sync::Arc,
};

use rustic_core::{
    BackupOptions, ConfigOptions, Credentials, IndexedFullStatus, IndexedIdsStatus, KeyOptions,
    LocalDestination, LsOptions, Open, OpenStatus, PathList, Repository, RepositoryBackends,
    RepositoryOptions, RestoreOptions, RestorePlan, RusticResult,
    repofile::{MasterKey, SnapshotFile},
};
use rustic_testing::{
    TestResult,
    backend::{fault_injection_backend::FaultInjectionBackend, in_memory_backend::InMemoryBackend},
};
use tempfile::tempdir;

/// Creates a fault injection backend over an empty backend in memory.
#[must_use]
pub fn fault_injection_backend() -> Arc<FaultInjectionBackend> {
    Arc::new(FaultInjectionBackend::new(Arc::new(InMemoryBackend::new())))
}

/// Creates a repository in `backend`.
///
/// The repository uses a master key and no cache.
///
/// # Errors
///
/// * If the function cannot create the repository.
pub fn init_repo(backend: &Arc<FaultInjectionBackend>) -> TestResult<Repository<OpenStatus>> {
    let backends = RepositoryBackends::new(backend.clone(), None);
    let repo = Repository::new(&RepositoryOptions::default().no_cache(true), &backends)?;
    Ok(repo.init(
        &Credentials::Masterkey(MasterKey::new()),
        &KeyOptions::default(),
        &ConfigOptions::default(),
    )?)
}

/// Gives `len` bytes of pseudo-random data.
///
/// The same `seed` gives the same data. Different seeds give different data, so the repository does not deduplicate it.
#[must_use]
pub fn content(seed: u64, len: usize) -> Box<[u8]> {
    iter::successors(
        Some(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1),
        |state| {
            let state = state ^ (state << 13);
            let state = state ^ (state >> 7);
            Some(state ^ (state << 17))
        },
    )
    .map(|state| state.to_be_bytes()[0])
    .take(len)
    .collect()
}

/// Writes files below the directory `dir`.
///
/// # Arguments
///
/// * `dir` - The directory for the files
/// * `files` - The path of each file relative to `dir`, and the content of the file
///
/// # Errors
///
/// * If the function cannot write a directory or a file.
pub fn write_files(dir: &Path, files: &[(&str, &[u8])]) -> TestResult<()> {
    files.iter().try_for_each(|(name, data)| {
        let path: Box<Path> = dir.join(name).into_boxed_path();
        fs::create_dir_all(path.parent().ok_or("a file path has a parent")?)?;
        fs::write(&path, data)?;
        Ok(())
    })
}

/// Backs up the directory or file `source` into `repo`, with the path `as_path` in the snapshot.
///
/// # Returns
///
/// The repository with an index that contains the new data, and the new snapshot.
///
/// # Errors
///
/// * If the function cannot read the index.
/// * If the backup fails.
pub fn backup<S: Open>(
    repo: Repository<S>,
    source: &Path,
    as_path: &str,
) -> TestResult<(Repository<IndexedIdsStatus>, SnapshotFile)> {
    let repo = repo.to_indexed_ids()?;
    let opts = BackupOptions::default().as_path(PathBuf::from(as_path));
    let snap = repo.backup(
        &opts,
        &PathList::from_iter(Some(source.to_path_buf())),
        SnapshotFile::default(),
    )?;
    Ok((repo, snap))
}

/// Checks that `result` is an error whose text contains `expected`.
///
/// # Errors
///
/// * If `result` is `Ok`.
/// * If the text of the error does not contain `expected`.
pub fn expect_error<T>(result: RusticResult<T>, expected: &str) -> TestResult<()> {
    match result {
        Ok(_) => Err(format!(
            "The operation succeeded, but an error that contains `{expected}` was expected."
        )
        .into()),
        Err(err) => {
            let text: Box<str> = err.to_string().into_boxed_str();
            if text.contains(expected) {
                Ok(())
            } else {
                Err(format!("The error does not contain `{expected}`:\n{text}").into())
            }
        }
    }
}

/// A repository with a snapshot, and the backend of the repository.
#[derive(Debug)]
pub struct SavedRepo {
    /// The backend of the repository.
    pub backend: Arc<FaultInjectionBackend>,
    /// The repository with the full index.
    pub repo: Repository<IndexedFullStatus>,
    /// The snapshot to restore.
    pub snap: SnapshotFile,
}

/// Saves files in a new repository.
///
/// The function writes `files` into a new directory, and backs up the directory with the path `data`.
///
/// # Arguments
///
/// * `files` - The path of each file relative to the directory, and the content of the file
///
/// # Errors
///
/// * If the function cannot write the files.
/// * If the function cannot create the repository, or the backup fails.
pub fn save_files(files: &[(&str, &[u8])]) -> TestResult<SavedRepo> {
    let backend = fault_injection_backend();
    let source = tempdir()?;
    write_files(source.path(), files)?;
    let (repo, snap) = backup(init_repo(&backend)?, source.path(), "data")?;
    Ok(SavedRepo {
        backend,
        repo: repo.to_indexed()?,
        snap,
    })
}

/// Restores the snapshot of `saved` into the directory `dir`.
///
/// The function makes the restore plan, calls `before_restore` with the plan, and then restores.
/// Thus `before_restore` can inject faults or change `dir` after the plan.
///
/// # Arguments
///
/// * `saved` - The repository and the snapshot to restore
/// * `dir` - The destination directory
/// * `opts` - The restore options
/// * `before_restore` - The function that runs between the plan and the restore
///
/// # Returns
///
/// The result of the restore.
///
/// # Errors
///
/// * If the function cannot make the plan.
/// * If `before_restore` fails.
pub fn restore_with(
    saved: &SavedRepo,
    dir: &Path,
    opts: &RestoreOptions,
    before_restore: impl FnOnce(&RestorePlan) -> TestResult<()>,
) -> TestResult<RusticResult<()>> {
    let node = saved.repo.node_from_snapshot_and_path(&saved.snap, "")?;
    let ls = saved.repo.ls(&node, &LsOptions::default())?;
    let dest = LocalDestination::new(
        dir.to_str().ok_or("the directory path is not UTF-8")?,
        true,
        false,
    )?;
    let plan = saved.repo.prepare_restore(opts, ls.clone(), &dest, false)?;
    before_restore(&plan)?;
    Ok(saved.repo.restore(plan, opts, ls, &dest))
}
