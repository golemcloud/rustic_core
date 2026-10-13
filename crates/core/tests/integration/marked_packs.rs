//! Tests of the full index with the option `index_marked_packs`.
//!
//! A prune marks the packs of the deleted snapshots for deletion. A snapshot that the prune did not
//! see can use these packs. The tests make such a snapshot: they save two snapshots of the same
//! files, hide the second snapshot from a prune that deletes the first snapshot, and then put the
//! second snapshot back.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use bytes::Bytes;
use jiff::Span;
use pretty_assertions::assert_eq;
use tempfile::{TempDir, tempdir};

use rustic_core::{
    BackupOptions, ConfigOptions, Credentials, FileType, KeyOptions, LocalDestination, LsOptions,
    PathList, PruneOptions, ReadBackend, Repository, RepositoryBackends, RepositoryOptions,
    RestoreOptions, WriteBackend,
    repofile::{BlobType, IndexFile, IndexId, MasterKey, PackId, SnapshotFile},
};
use rustic_testing::backend::in_memory_backend::InMemoryBackend;

use super::RepoOpen;

/// A repository with a snapshot whose packs a prune marked for deletion.
struct MarkedSnapshot {
    /// The backend of the repository.
    backend: Arc<InMemoryBackend>,
    /// The key of the repository.
    key: MasterKey,
    /// The saved files.
    source: TempDir,
    /// The snapshot that uses the marked packs.
    snap: SnapshotFile,
}

/// Opens the repository in `backend` with the option `index_marked_packs`.
fn open(
    backend: &Arc<InMemoryBackend>,
    key: &MasterKey,
    index_marked_packs: bool,
) -> Result<RepoOpen> {
    let opts = RepositoryOptions::default()
        .no_cache(true)
        .index_marked_packs(index_marked_packs);
    let backends = RepositoryBackends::new(backend.clone(), None);
    Ok(Repository::new(&opts, &backends)?.open(&Credentials::Masterkey(key.clone()))?)
}

/// Backs up `source` into the repository in `backend`.
fn backup(backend: &Arc<InMemoryBackend>, key: &MasterKey, source: &Path) -> Result<SnapshotFile> {
    let repo = open(backend, key, false)?.to_indexed_ids()?;
    let opts = BackupOptions::default().as_path(PathBuf::from("data"));
    Ok(repo.backup(
        &opts,
        &PathList::from_iter(Some(source.to_path_buf())),
        SnapshotFile::default(),
    )?)
}

/// Gives the packs that the index files of the repository in `backend` mark for deletion.
fn marked_packs(backend: &Arc<InMemoryBackend>, key: &MasterKey) -> Result<BTreeSet<PackId>> {
    let repo = open(backend, key, false)?;
    repo.list::<IndexId>()?
        .map(|id| {
            Ok(repo
                .get_file::<IndexFile>(&id)?
                .packs_to_delete
                .into_iter()
                .map(|pack| pack.id))
        })
        .collect::<Result<Vec<_>>>()
        .map(|packs| packs.into_iter().flatten().collect())
}

/// Saves two snapshots of the same files, and marks their packs with a prune that sees only the
/// first snapshot, after a delete of that snapshot.
fn marked_snapshot() -> Result<MarkedSnapshot> {
    let backend = Arc::new(InMemoryBackend::new());
    let key = MasterKey::new();
    let backends = RepositoryBackends::new(backend.clone(), None);
    let _ = Repository::new(&RepositoryOptions::default().no_cache(true), &backends)?.init(
        &Credentials::Masterkey(key.clone()),
        &KeyOptions::default(),
        &ConfigOptions::default(),
    )?;
    let source = tempdir()?;
    (0..8u8).try_for_each(|index| {
        let content: Vec<u8> = (0..20_000u32)
            .map(|byte| byte.to_le_bytes()[0].wrapping_mul(31) ^ index)
            .collect();
        fs::create_dir_all(source.path().join("dir"))?;
        fs::write(source.path().join("dir").join(format!("f{index}")), content)
    })?;

    let first = backup(&backend, &key, source.path())?;
    let snap = backup(&backend, &key, source.path())?;
    let hidden: Bytes = backend.read_full(FileType::Snapshot, &snap.id)?;
    backend.remove(FileType::Snapshot, &snap.id, false)?;
    let repo = open(&backend, &key, false)?;
    repo.delete_snapshots(&[first.id])?;
    let prune_opts = PruneOptions::default().keep_delete(Span::new().hours(1));
    let plan = repo.prune_plan(&prune_opts)?;
    repo.prune(&prune_opts, plan)?;
    backend.write_bytes(FileType::Snapshot, &snap.id, false, hidden.into())?;

    Ok(MarkedSnapshot {
        backend,
        key,
        source,
        snap,
    })
}

/// Restores `snap` with a full index from the repository in `backend`.
///
/// # Returns
///
/// The directory that holds the restored files.
fn restore(
    backend: &Arc<InMemoryBackend>,
    key: &MasterKey,
    index_marked_packs: bool,
    snap: &SnapshotFile,
) -> Result<TempDir> {
    let repo = open(backend, key, index_marked_packs)?.to_indexed()?;
    let node = repo.node_from_snapshot_and_path(snap, "")?;
    let ls = repo.ls(&node, &LsOptions::default())?;
    let into = tempdir()?;
    let dest = LocalDestination::new(into.path().to_str().expect("a UTF-8 path"), true, false)?;
    let opts = RestoreOptions::default();
    let plan = repo.prepare_restore(&opts, ls.clone(), &dest, false)?;
    repo.restore(plan, &opts, ls, &dest)?;
    Ok(into)
}

/// Checks that `into` holds the files of `source` below the path `data`.
fn expect_same_files(source: &Path, into: &Path) -> Result<()> {
    let names: BTreeSet<_> = fs::read_dir(source.join("dir"))?
        .map(|entry| Ok(entry?.file_name()))
        .collect::<Result<_>>()?;
    let restored: BTreeSet<_> = fs::read_dir(into.join("data").join("dir"))?
        .map(|entry| Ok(entry?.file_name()))
        .collect::<Result<_>>()?;
    assert_eq!(restored, names);
    names.iter().try_for_each(|name| {
        assert_eq!(
            fs::read(into.join("data").join("dir").join(name))?,
            fs::read(source.join("dir").join(name))?
        );
        Ok(())
    })
}

#[test]
fn test_full_index_without_option_misses_marked_packs() -> Result<()> {
    let marked = marked_snapshot()?;

    let restored = restore(&marked.backend, &marked.key, false, &marked.snap);

    assert!(!marked_packs(&marked.backend, &marked.key)?.is_empty());
    let err = restored.expect_err("the restore needs a marked pack");
    assert!(
        err.to_string().contains("not found in index"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[test]
fn test_full_index_with_option_reads_marked_packs() -> Result<()> {
    let marked = marked_snapshot()?;

    let into = restore(&marked.backend, &marked.key, true, &marked.snap)?;

    expect_same_files(marked.source.path(), into.path())
}

#[test]
fn test_full_index_with_option_prefers_packs_that_are_not_marked() -> Result<()> {
    let marked = marked_snapshot()?;
    let packs = marked_packs(&marked.backend, &marked.key)?;
    // The index of a backup does not hold the marked packs, so the backup writes each blob again.
    let _ = backup(&marked.backend, &marked.key, marked.source.path())?;
    let repo = open(&marked.backend, &marked.key, true)?.to_indexed()?;
    let blobs: Vec<_> = repo
        .list::<IndexId>()?
        .map(|id| Ok(repo.get_file::<IndexFile>(&id)?.packs_to_delete))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .flat_map(|pack| pack.blobs)
        .collect();

    let from_marked = blobs
        .iter()
        .filter(|blob| {
            let entry = match blob.tpe {
                BlobType::Tree => repo.get_index_entry(&rustic_core::TreeId::from(blob.id)),
                BlobType::Data => repo.get_index_entry(&rustic_core::DataId::from(blob.id)),
            };
            entry.map_or(true, |entry| packs.contains(&entry.pack))
        })
        .count();
    packs.iter().try_for_each(|pack| {
        marked
            .backend
            .remove(FileType::Pack, pack, false)
            .map_err(anyhow::Error::from)
    })?;
    let into = restore(&marked.backend, &marked.key, true, &marked.snap)?;

    assert!(!blobs.is_empty());
    assert_eq!(from_marked, 0);
    expect_same_files(marked.source.path(), into.path())
}
