//! Tests of a backup whose pack write fails.
//!
//! The backend fails pack writes of the data packer, of the tree packer, or of both, while the
//! archiver still sends blobs. The threads of a packer and of its file writer read their channels
//! until the archiver finalizes the packer, because pariter's readahead reads its input to the end.
//! So a send of the archiver does not find a closed channel, and the finalize gives the error of
//! the first failed write, with the error of the storage as its source.

use std::{
    error::Error,
    fmt, fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use aes256ctr_poly1305aes::{
    Aes256CtrPoly1305Aes, Key,
    aead::{Aead, Nonce},
};
use anyhow::Result;
use bytes::Bytes;
use bytesize::ByteSize;
use tempfile::{TempDir, tempdir};

use rustic_core::{
    BackupOptions, BytesList, ConfigOptions, Credentials, ErrorKind, FileType, Id, KeyOptions,
    PathList, ReadBackend, Repository, RepositoryBackends, RepositoryOptions, RusticError,
    RusticResult, WriteBackend,
    repofile::{MasterKey, SnapshotFile},
};
use rustic_testing::backend::in_memory_backend::InMemoryBackend;

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

/// The error of the storage that refuses a pack write.
#[derive(Debug)]
struct StorageFault;

impl fmt::Display for StorageFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the storage refuses the write")
    }
}

impl Error for StorageFault {}

/// Which pack writes of the backend fail.
#[derive(Clone, Copy)]
enum FailingPacks {
    /// Each pack holds one blob, and each write of a pack fails.
    Each,
    /// Each pack holds one blob, and each write of a data pack fails. Each write of a tree pack
    /// goes to the storage.
    EachData,
    /// Each tree pack holds one blob, and a data pack holds all data blobs, so only tree packs are
    /// written before the archiver finalizes the packers. The first write of a tree pack fails.
    FirstTree,
}

/// The writes of data packs and of tree packs that the backend refused or wrote.
#[derive(Default)]
struct PackWrites {
    refused_data: AtomicUsize,
    refused_tree: AtomicUsize,
    written_data: AtomicUsize,
    written_tree: AtomicUsize,
}

/// Backend that fails the pack writes of `failing` with [`StorageFault`] as the source of the error.
///
/// It decrypts the header of each pack to know if the pack holds data blobs or tree blobs.
struct PackWriteFaults {
    inner: InMemoryBackend,
    key: Aes256CtrPoly1305Aes,
    failing: FailingPacks,
    failed: AtomicBool,
    writes: PackWrites,
}

impl fmt::Debug for PackWriteFaults {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PackWriteFaults")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl PackWriteFaults {
    fn new(key: &MasterKey, failing: FailingPacks) -> Self {
        let key = [&key.encrypt[..], &key.mac.k[..], &key.mac.r[..]].concat();
        Self {
            inner: InMemoryBackend::new(),
            key: Aes256CtrPoly1305Aes::new(Key::from_slice(&key)),
            failing,
            failed: AtomicBool::new(false),
            writes: PackWrites::default(),
        }
    }

    /// Tells if the pack `content` holds tree blobs.
    ///
    /// The pack ends with its encrypted header and the length of that header. Each entry of the
    /// header starts with the type of its blob: 0 or 2 for a data blob, 1 or 3 for a tree blob.
    fn holds_trees(&self, content: &BytesList) -> bool {
        let pack = content.slice().concat();
        let (rest, length) = pack.split_at(pack.len() - 4);
        let length = u32::from_le_bytes(length.try_into().expect("the length has 4 bytes"));
        let header = &rest[rest.len() - length as usize..];
        let (nonce, sealed) = header.split_at(16);
        let header = self
            .key
            .decrypt(Nonce::<Aes256CtrPoly1305Aes>::from_slice(nonce), sealed)
            .expect("the header of the pack decrypts");
        matches!(header[0], 1 | 3)
    }

    /// Tells if the write of the pack `content` fails, and counts the write.
    fn fails(&self, content: &BytesList) -> bool {
        let tree = self.holds_trees(content);
        let fails = match self.failing {
            FailingPacks::Each => true,
            FailingPacks::EachData => !tree,
            FailingPacks::FirstTree => tree && !self.failed.swap(true, Ordering::SeqCst),
        };
        let counter = match (fails, tree) {
            (true, false) => &self.writes.refused_data,
            (true, true) => &self.writes.refused_tree,
            (false, false) => &self.writes.written_data,
            (false, true) => &self.writes.written_tree,
        };
        _ = counter.fetch_add(1, Ordering::SeqCst);
        fails
    }
}

impl ReadBackend for PackWriteFaults {
    fn location(&self) -> String {
        self.inner.location()
    }

    fn list_with_size(&self, tpe: FileType) -> RusticResult<Vec<(Id, u32)>> {
        self.inner.list_with_size(tpe)
    }

    fn read_full(&self, tpe: FileType, id: &Id) -> RusticResult<Bytes> {
        self.inner.read_full(tpe, id)
    }

    fn read_partial(
        &self,
        tpe: FileType,
        id: &Id,
        cacheable: bool,
        offset: u32,
        length: u32,
    ) -> RusticResult<Bytes> {
        self.inner.read_partial(tpe, id, cacheable, offset, length)
    }

    fn warmup_path(&self, tpe: FileType, id: &Id) -> String {
        self.inner.warmup_path(tpe, id)
    }
}

impl WriteBackend for PackWriteFaults {
    fn write_bytes(
        &self,
        tpe: FileType,
        id: &Id,
        cacheable: bool,
        content: BytesList,
    ) -> RusticResult<()> {
        if tpe == FileType::Pack && self.fails(&content) {
            return Err(RusticError::new(
                ErrorKind::Backend,
                "The storage refuses the write of the pack `{id}`.",
            )
            .attach_context("id", id.to_string())
            .attach_source(StorageFault));
        }
        self.inner.write_bytes(tpe, id, cacheable, content)
    }

    fn remove(&self, tpe: FileType, id: &Id, cacheable: bool) -> RusticResult<()> {
        self.inner.remove(tpe, id, cacheable)
    }
}

/// Backs up `source` into a new repository whose backend fails the pack writes of `failing`.
fn backup_with_failed_pack_writes(
    source: &Path,
    opts: &BackupOptions,
    failing: FailingPacks,
) -> Result<(Arc<PackWriteFaults>, RusticResult<SnapshotFile>)> {
    let key = MasterKey::new();
    let backend = Arc::new(PackWriteFaults::new(&key, failing));
    let backends = RepositoryBackends::new(backend.clone(), None);
    let mut repo = Repository::new(&RepositoryOptions::default().no_cache(true), &backends)?.init(
        &Credentials::Masterkey(key),
        &KeyOptions::default(),
        &ConfigOptions::default(),
    )?;
    let config = ConfigOptions::default().set_treepack_size(ByteSize::b(1));
    let config = match failing {
        FailingPacks::Each | FailingPacks::EachData => config.set_datapack_size(ByteSize::b(1)),
        FailingPacks::FirstTree => config,
    };
    _ = repo.apply_config(&config)?;
    let repo = repo.to_indexed_ids()?;
    let snap = repo.backup(
        opts,
        &PathList::from_iter(Some(source.to_path_buf())),
        SnapshotFile::default(),
    );
    Ok((backend, snap))
}

/// Checks that `snap` has the error of the storage in its chain of sources, and that `backend`
/// holds no snapshot file.
fn expect_storage_error(backend: &PackWriteFaults, snap: RusticResult<SnapshotFile>) -> Result<()> {
    let err = snap.expect_err("the backup cannot write its packs");
    let first: &(dyn Error + 'static) = &*err;
    assert!(
        std::iter::successors(Some(first), |&err| err.source())
            .any(<dyn Error>::is::<StorageFault>),
        "the backup does not return the error of the storage: {err:?}"
    );
    assert_eq!(backend.list(FileType::Snapshot)?.len(), 0);
    Ok(())
}

/// Checks that the backend refused more than one data pack, and wrote tree packs only.
fn expect_only_data_packs_refused(backend: &PackWriteFaults) {
    let writes = &backend.writes;
    assert!(writes.refused_data.load(Ordering::SeqCst) > 1);
    assert_eq!(writes.written_data.load(Ordering::SeqCst), 0);
    assert_eq!(writes.refused_tree.load(Ordering::SeqCst), 0);
    assert!(writes.written_tree.load(Ordering::SeqCst) > 0);
}

#[test]
fn test_backup_returns_the_error_of_a_failed_pack_write() -> Result<()> {
    let source = source()?;
    let opts = BackupOptions::default().as_path(PathBuf::from("data"));

    let (backend, snap) = backup_with_failed_pack_writes(source.path(), &opts, FailingPacks::Each)?;

    expect_storage_error(&backend, snap)
}

#[test]
fn test_strict_backup_returns_the_error_of_a_failed_pack_write() -> Result<()> {
    let source = source()?;
    let opts = BackupOptions::default()
        .as_path(PathBuf::from("data"))
        .fail_on_read_error(true);

    let (backend, snap) = backup_with_failed_pack_writes(source.path(), &opts, FailingPacks::Each)?;

    expect_storage_error(&backend, snap)
}

#[test]
fn test_backup_returns_the_error_of_a_failed_data_pack_write() -> Result<()> {
    let source = source()?;
    let opts = BackupOptions::default().as_path(PathBuf::from("data"));

    let (backend, snap) =
        backup_with_failed_pack_writes(source.path(), &opts, FailingPacks::EachData)?;

    expect_only_data_packs_refused(&backend);
    expect_storage_error(&backend, snap)
}

#[test]
fn test_strict_backup_returns_the_error_of_a_failed_data_pack_write() -> Result<()> {
    let source = source()?;
    let opts = BackupOptions::default()
        .as_path(PathBuf::from("data"))
        .fail_on_read_error(true);

    let (backend, snap) =
        backup_with_failed_pack_writes(source.path(), &opts, FailingPacks::EachData)?;

    expect_only_data_packs_refused(&backend);
    expect_storage_error(&backend, snap)
}

#[test]
fn test_backup_returns_the_error_of_a_failed_tree_pack_write() -> Result<()> {
    let source = source()?;
    let opts = BackupOptions::default().as_path(PathBuf::from("data"));

    let (backend, snap) =
        backup_with_failed_pack_writes(source.path(), &opts, FailingPacks::FirstTree)?;

    expect_storage_error(&backend, snap)
}

#[test]
fn test_strict_backup_returns_the_error_of_a_failed_tree_pack_write() -> Result<()> {
    let source = source()?;
    let opts = BackupOptions::default()
        .as_path(PathBuf::from("data"))
        .fail_on_read_error(true);

    let (backend, snap) =
        backup_with_failed_pack_writes(source.path(), &opts, FailingPacks::FirstTree)?;

    expect_storage_error(&backend, snap)
}
