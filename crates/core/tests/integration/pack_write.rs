//! Tests of a backup whose pack write fails.
//!
//! The backend fails pack writes of the data packer and of the tree packer while the archiver still
//! sends blobs. The threads of a packer and of its file writer read their channels until the
//! archiver finalizes the packer, because pariter's readahead reads its input to the end. So a send
//! of the archiver does not find a closed channel, and the finalize gives the error of the first
//! failed write.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::Result;
use bytesize::ByteSize;
use tempfile::{TempDir, tempdir};

use rustic_core::{
    BackupOptions, ConfigOptions, Credentials, FileType, KeyOptions, PathList, ReadBackend,
    Repository, RepositoryBackends, RepositoryOptions, RusticResult,
    repofile::{MasterKey, SnapshotFile},
};
use rustic_testing::backend::{
    fault_injection_backend::{BackendOp, Fault, FaultInjectionBackend, INJECTED_FAULT},
    in_memory_backend::InMemoryBackend,
};

/// The number of directories of the source. Each directory holds one file.
const DIRECTORIES: usize = 2048;

/// Writes a source of [`DIRECTORIES`] directories, each with one file of other content.
fn source() -> Result<TempDir> {
    let source = tempdir()?;
    (0..DIRECTORIES).try_for_each(|index| {
        let dir = source.path().join(format!("d{index:04}"));
        fs::create_dir_all(&dir)?;
        fs::write(
            dir.join("f"),
            format!("the content of file {index}").repeat(64),
        )
    })?;
    Ok(source)
}

/// Which pack writes of the backend fail.
#[derive(Clone, Copy)]
enum Failing {
    /// Each pack holds one blob, and each write of a pack fails.
    EachPack,
    /// Each tree pack holds one blob, and a data pack holds all data blobs, so only tree packs are
    /// written before the archiver finalizes the packers. The first write of a pack fails.
    FirstTreePack,
}

/// Backs up `source` into a new repository whose backend fails the pack writes of `failing`.
fn backup_with_failed_pack_writes(
    source: &Path,
    opts: &BackupOptions,
    failing: Failing,
) -> Result<(Arc<FaultInjectionBackend>, RusticResult<SnapshotFile>)> {
    let backend = Arc::new(FaultInjectionBackend::new(Arc::new(InMemoryBackend::new())));
    let backends = RepositoryBackends::new(backend.clone(), None);
    let mut repo = Repository::new(&RepositoryOptions::default().no_cache(true), &backends)?.init(
        &Credentials::Masterkey(MasterKey::new()),
        &KeyOptions::default(),
        &ConfigOptions::default(),
    )?;
    let config = ConfigOptions::default().set_treepack_size(ByteSize::b(1));
    let config = match failing {
        Failing::EachPack => config.set_datapack_size(ByteSize::b(1)),
        Failing::FirstTreePack => config,
    };
    _ = repo.apply_config(&config)?;
    let failed = AtomicBool::new(false);
    backend.inject(move |call| {
        let pack_write = call.op == BackendOp::Write && call.tpe == FileType::Pack;
        let fails = match failing {
            Failing::EachPack => pack_write,
            Failing::FirstTreePack => pack_write && !failed.swap(true, Ordering::SeqCst),
        };
        fails.then_some(Fault::Error)
    });
    let repo = repo.to_indexed_ids()?;
    let snap = repo.backup(
        opts,
        &PathList::from_iter(Some(source.to_path_buf())),
        SnapshotFile::default(),
    );
    Ok((backend, snap))
}

/// Checks that `snap` is the error of the backend, and that `backend` holds no snapshot file.
fn expect_backend_error(
    backend: &FaultInjectionBackend,
    snap: RusticResult<SnapshotFile>,
) -> Result<()> {
    let err = snap.expect_err("the backup cannot write its packs");
    assert!(
        err.to_string().contains(INJECTED_FAULT),
        "the backup does not return the error of the backend: {err}"
    );
    assert_eq!(backend.list(FileType::Snapshot)?.len(), 0);
    Ok(())
}

#[test]
fn test_backup_returns_the_error_of_a_failed_pack_write() -> Result<()> {
    let source = source()?;
    let opts = BackupOptions::default().as_path(PathBuf::from("data"));

    let (backend, snap) = backup_with_failed_pack_writes(source.path(), &opts, Failing::EachPack)?;

    expect_backend_error(&backend, snap)
}

#[test]
fn test_strict_backup_returns_the_error_of_a_failed_pack_write() -> Result<()> {
    let source = source()?;
    let opts = BackupOptions::default()
        .as_path(PathBuf::from("data"))
        .fail_on_read_error(true);

    let (backend, snap) = backup_with_failed_pack_writes(source.path(), &opts, Failing::EachPack)?;

    expect_backend_error(&backend, snap)
}

#[test]
fn test_backup_returns_the_error_of_a_failed_tree_pack_write() -> Result<()> {
    let source = source()?;
    let opts = BackupOptions::default().as_path(PathBuf::from("data"));

    let (backend, snap) =
        backup_with_failed_pack_writes(source.path(), &opts, Failing::FirstTreePack)?;

    expect_backend_error(&backend, snap)
}

#[test]
fn test_strict_backup_returns_the_error_of_a_failed_tree_pack_write() -> Result<()> {
    let source = source()?;
    let opts = BackupOptions::default()
        .as_path(PathBuf::from("data"))
        .fail_on_read_error(true);

    let (backend, snap) =
        backup_with_failed_pack_writes(source.path(), &opts, Failing::FirstTreePack)?;

    expect_backend_error(&backend, snap)
}
