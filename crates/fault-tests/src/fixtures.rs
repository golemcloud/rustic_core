//! Repositories, source trees and checks for the fault scenarios.

use std::{
    fs, iter,
    path::{Path, PathBuf},
    sync::Arc,
};

use rustic_core::{
    BackupOptions, ConfigOptions, Credentials, IndexedIdsStatus, KeyOptions, Open, OpenStatus,
    PathList, Repository, RepositoryBackends, RepositoryOptions, RusticResult,
    repofile::{MasterKey, SnapshotFile},
};
use rustic_testing::{
    TestResult,
    backend::{fault_injection_backend::FaultInjectionBackend, in_memory_backend::InMemoryBackend},
};

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
/// * If the repository cannot be created.
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
/// * If a directory or a file cannot be written.
pub fn write_files(dir: &Path, files: &[(&str, &[u8])]) -> TestResult<()> {
    files.iter().try_for_each(|(name, data)| {
        let path = dir.join(name);
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
/// * If the index cannot be read.
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
            let text = err.to_string();
            if text.contains(expected) {
                Ok(())
            } else {
                Err(format!("The error does not contain `{expected}`:\n{text}").into())
            }
        }
    }
}
