//! Repositories, source trees and checks for the fault scenarios.

use std::{
    ffi::OsStr,
    fs,
    io::{self, Cursor, Read},
    iter,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use rustic_core::{
    BackupOptions, ConfigOptions, Credentials, FileType, IndexedFullStatus, IndexedIdsStatus,
    KeyOptions, LocalDestination, LsOptions, NoProgressBars, Open, OpenStatus, PathList, Progress,
    ProgressBars, ProgressType, ReadBackend, ReadSource, ReadSourceEntry, ReadSourceOpen,
    Repository, RepositoryBackends, RepositoryOptions, RestoreOptions, RestorePlan, RusticProgress,
    RusticResult, WriteBackend,
    repofile::{MasterKey, Metadata, Node, NodeType, SnapshotFile},
};
use rustic_testing::{
    TestResult,
    backend::{fault_injection_backend::FaultInjectionBackend, in_memory_backend::InMemoryBackend},
};
use tempfile::{TempDir, tempdir};

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
    init_repo_with_key(backend, &MasterKey::new())
}

/// Creates a repository in `backend` that `key` opens.
///
/// The repository uses no cache.
///
/// # Errors
///
/// * If the function cannot create the repository.
pub fn init_repo_with_key(
    backend: &Arc<FaultInjectionBackend>,
    key: &MasterKey,
) -> TestResult<Repository<OpenStatus>> {
    init_repo_with_progress(backend, key, NoProgressBars)
}

/// Creates a repository in `backend` that `key` opens, and that gives its progress to `progress`.
///
/// The repository uses no cache.
///
/// # Errors
///
/// * If the function cannot create the repository.
pub fn init_repo_with_progress(
    backend: &Arc<FaultInjectionBackend>,
    key: &MasterKey,
    progress: impl ProgressBars,
) -> TestResult<Repository<OpenStatus>> {
    let backends = RepositoryBackends::new(backend.clone(), None);
    let repo = Repository::new_with_progress(
        &RepositoryOptions::default().no_cache(true),
        &backends,
        progress,
    )?;
    Ok(repo.init(
        &Credentials::Masterkey(key.clone()),
        &KeyOptions::default(),
        &ConfigOptions::default(),
    )?)
}

/// Opens the repository in `backend` with a cache below the directory `cache_dir`.
///
/// # Arguments
///
/// * `backend` - The backend that holds the repository
/// * `key` - The master key of the repository
/// * `cache_dir` - The directory for the cache
///
/// # Errors
///
/// * If the function cannot open the repository.
pub fn open_repo_with_cache(
    backend: &Arc<FaultInjectionBackend>,
    key: &MasterKey,
    cache_dir: &Path,
) -> TestResult<Repository<OpenStatus>> {
    let backends = RepositoryBackends::new(backend.clone(), None);
    let opts = RepositoryOptions::default().cache_dir(cache_dir.to_path_buf());
    let repo = Repository::new(&opts, &backends)?;
    Ok(repo.open(&Credentials::Masterkey(key.clone()))?)
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
    let opts = BackupOptions::default().as_path(PathBuf::from(as_path));
    let (repo, snap) = backup_with_options(repo, source, &opts)?;
    Ok((repo, snap?))
}

/// Backs up the directory or file `source` into `repo` with the options `opts`.
///
/// # Returns
///
/// The repository with an index that contains the new data, and the result of the backup.
///
/// # Errors
///
/// * If the function cannot read the index.
pub fn backup_with_options<S: Open>(
    repo: Repository<S>,
    source: &Path,
    opts: &BackupOptions,
) -> TestResult<(Repository<IndexedIdsStatus>, RusticResult<SnapshotFile>)> {
    let repo = repo.to_indexed_ids()?;
    let snap = repo.backup(
        opts,
        &PathList::from_iter(Some(source.to_path_buf())),
        SnapshotFile::default(),
    );
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

/// Saves `count` files in a new repository, each file in its own pack.
///
/// The function backs up each file in its own backup, so each file is in its own pack.
/// A last backup of all files, with the path `data`, adds no data, so its snapshot uses all packs.
///
/// # Arguments
///
/// * `count` - The number of files
/// * `seed` - The seed of the content of the first file. Each next file uses the next seed.
///
/// # Errors
///
/// * If the function cannot write the files.
/// * If the function cannot create the repository, or a backup fails.
pub fn save_files_in_own_packs(count: u64, seed: u64) -> TestResult<SavedRepo> {
    let backend = fault_injection_backend();
    let source = tempdir()?;
    let files: Box<[_]> = (0..count)
        .map(|index| {
            (
                format!("f{index:02}").into_boxed_str(),
                content(seed + index, 4_000),
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
    Ok(SavedRepo {
        backend,
        repo: repo.to_indexed()?,
        snap,
    })
}

/// A repository that uses a cache, with a snapshot, and the backend of the repository.
///
/// The cache is below a temporary directory that lives as long as this value.
#[derive(Debug)]
pub struct CachedRepo {
    /// The backend of the repository.
    pub backend: Arc<FaultInjectionBackend>,
    /// The repository with the full index.
    pub repo: Repository<IndexedFullStatus>,
    /// The snapshot of the saved files.
    pub snap: SnapshotFile,
    /// The directory that holds the cache.
    pub cache_dir: TempDir,
}

/// Saves files in a new repository, and opens the repository again with a cache.
///
/// The function writes `files` into a new directory, and backs up the directory with the path `data`.
/// The backup uses no cache, and the cache of the open repository holds no pack file.
/// Thus each pack read of the open repository reaches the backend.
///
/// # Arguments
///
/// * `files` - The path of each file relative to the directory, and the content of the file
///
/// # Errors
///
/// * If the function cannot write the files.
/// * If the function cannot create the repository, or the backup fails.
/// * If the function cannot open the repository with a cache.
pub fn save_files_with_cache(files: &[(&str, &[u8])]) -> TestResult<CachedRepo> {
    let backend = fault_injection_backend();
    let key = MasterKey::new();
    let source = tempdir()?;
    write_files(source.path(), files)?;
    let (_, snap) = backup(init_repo_with_key(&backend, &key)?, source.path(), "data")?;
    let cache_dir = tempdir()?;
    let repo = open_repo_with_cache(&backend, &key, cache_dir.path())?;
    Ok(CachedRepo {
        backend,
        repo: repo.to_indexed()?,
        snap,
        cache_dir,
    })
}

/// Replaces each file of the type `tpe` in `backend` with the first `len` bytes of that file.
///
/// A file that has `len` bytes or less does not change.
///
/// # Arguments
///
/// * `backend` - The backend that holds the files
/// * `tpe` - The type of the files to shorten
/// * `len` - The maximum number of bytes that a file keeps
///
/// # Errors
///
/// * If the function cannot list, read, remove or write a file.
pub fn shorten_files(
    backend: &Arc<FaultInjectionBackend>,
    tpe: FileType,
    len: usize,
) -> TestResult<()> {
    backend.list(tpe)?.into_iter().try_for_each(|id| {
        let data = backend.read_full(tpe, &id)?;
        let short = data.slice(..len.min(data.len()));
        backend.remove(tpe, &id, false)?;
        backend.write_bytes(tpe, &id, false, short.into())?;
        Ok(())
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

/// Restores only the metadata of `nodes` into the directory `dir`.
///
/// The restore uses an empty repository and an empty plan, so it writes no file contents.
/// Thus a scenario can give nodes with values that a backup does not make.
/// `nodes` holds `PathBuf` values, because the restore takes nodes with a `PathBuf`.
///
/// # Arguments
///
/// * `dir` - The destination directory
/// * `opts` - The restore options
/// * `nodes` - The path of each node relative to `dir`, and the node
///
/// # Returns
///
/// The result of the restore.
///
/// # Errors
///
/// * If the function cannot create the empty repository or the destination.
pub fn restore_metadata(
    dir: &Path,
    opts: &RestoreOptions,
    nodes: Box<[(PathBuf, Node)]>,
) -> TestResult<RusticResult<()>> {
    let repo = init_repo(&fault_injection_backend())?.to_indexed()?;
    let dest = LocalDestination::new(
        dir.to_str().ok_or("the directory path is not UTF-8")?,
        true,
        false,
    )?;
    Ok(repo.restore(
        RestorePlan::default(),
        opts,
        nodes.into_iter().map(Ok),
        &dest,
    ))
}

/// Progress bars that count the bytes of each progress of the type [`ProgressType::Bytes`].
///
/// Each progress is hidden, so a backup does not scan the size of its source.
#[derive(Clone, Debug, Default)]
pub struct ByteCounter(Arc<AtomicU64>);

impl ByteCounter {
    /// Gives the number of bytes that the progress bars counted.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

impl ProgressBars for ByteCounter {
    fn progress(&self, progress_type: ProgressType, _prefix: &str) -> Progress {
        match progress_type {
            ProgressType::Bytes => Progress::new(CountingProgress(Arc::clone(&self.0))),
            ProgressType::Spinner | ProgressType::Counter => Progress::hidden(),
        }
    }
}

/// A hidden progress that adds each increment to a shared counter.
#[derive(Debug)]
struct CountingProgress(Arc<AtomicU64>);

impl RusticProgress for CountingProgress {
    fn is_hidden(&self) -> bool {
        true
    }

    fn set_length(&self, _len: u64) {}

    fn set_title(&self, _title: &str) {}

    fn inc(&self, inc: u64) {
        _ = self.0.fetch_add(inc, Ordering::SeqCst);
    }

    fn finish(&self) {}
}

/// Sets the permission bits of the file or directory `path` to `mode`.
///
/// # Errors
///
/// * If the function cannot set the permission bits.
#[cfg(unix)]
pub fn set_mode(path: &Path, mode: u32) -> TestResult<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    Ok(())
}

/// Sets the extended attributes with the names `names` of `node`, each with the value `x`.
///
/// The type of the extended attributes of a node is not public, so the function builds them from their
/// serialized form. `eA==` is the value `x` in base 64.
///
/// # Arguments
///
/// * `node` - The node to set the extended attributes of
/// * `names` - The name of each extended attribute
///
/// # Errors
///
/// * If the function cannot build the extended attributes.
#[cfg(target_os = "linux")]
pub fn set_extended_attributes(node: &mut Node, names: &[&str]) -> TestResult<()> {
    let attributes: Box<[Box<str>]> = names
        .iter()
        .map(|name| format!(r#"{{"name":"{name}","value":"eA=="}}"#).into_boxed_str())
        .collect();
    node.meta.extended_attributes = serde_json::from_str(&format!("[{}]", attributes.join(",")))?;
    Ok(())
}

/// The value that [`set_extended_attributes`] gives to each extended attribute.
#[cfg(target_os = "linux")]
pub const EXTENDED_ATTRIBUTE_VALUE: &[u8] = b"x";

/// Checks that the extended attribute `name` of the file `path` has the value `value`.
///
/// # Arguments
///
/// * `path` - The file to read the extended attribute of
/// * `name` - The name of the extended attribute
/// * `value` - The expected value, or `None` if the file must not have the attribute
///
/// # Errors
///
/// * If the function cannot read the extended attribute.
/// * If the value of the extended attribute is not `value`.
#[cfg(target_os = "linux")]
pub fn expect_xattr(path: &Path, name: &str, value: Option<&[u8]>) -> TestResult<()> {
    let found = xattr::get(path, name)?;
    if found.as_deref() == value {
        Ok(())
    } else {
        Err(format!(
            "The extended attribute `{name}` of `{}` is {found:?}, but {value:?} was expected.",
            path.display()
        )
        .into())
    }
}

/// Checks that the kernel refuses a set of the extended attribute `name` on the file `path` to this process.
///
/// A scenario of a failed set needs a set that the kernel refuses. A kernel that permits the set would make
/// the scenario give `Ok` without showing anything, so the scenario stops instead.
///
/// The function does not remove the attribute when the set succeeds, because the scenario then stops.
///
/// # Arguments
///
/// * `path` - The file to set the extended attribute on
/// * `name` - The name of the extended attribute
///
/// # Errors
///
/// * If the kernel permits the set.
#[cfg(target_os = "linux")]
pub fn require_xattr_refused(path: &Path, name: &str) -> TestResult<()> {
    if xattr::set(path, name, b"refused").is_err() {
        Ok(())
    } else {
        Err(format!(
            "This kernel lets this process set the extended attribute `{name}` on `{}`. The scenario cannot show the rule for an attribute that the kernel refuses.",
            path.display()
        )
        .into())
    }
}

/// Checks that this process does not run as root.
///
/// Some scenarios need a file operation that fails for a user other than root.
/// Root can do these operations, so such a scenario cannot show the error under root.
///
/// # Errors
///
/// * If this process runs as root.
#[cfg(target_os = "linux")]
pub fn require_non_root() -> TestResult<()> {
    if nix::unistd::geteuid().is_root() {
        Err("This scenario needs a user other than root. Root can do the file operation that the scenario makes fail.".into())
    } else {
        Ok(())
    }
}

/// Counts the files that a backup reads at the same time.
///
/// A read of a file is live from `open` until the drop of the reader.
/// The archiver drops the reader when it has read and chunked the file, in its parallel stage.
#[derive(Debug, Default)]
pub struct ReadCounter {
    /// The number of files that are open now.
    live: AtomicUsize,
    /// The maximum number of files that were open at the same time.
    peak: AtomicUsize,
    /// The number of files that were opened.
    opened: AtomicUsize,
}

impl ReadCounter {
    /// Gives the number of files that were opened.
    #[must_use]
    pub fn opened(&self) -> usize {
        self.opened.load(Ordering::SeqCst)
    }

    /// Gives the maximum number of files that were open at the same time.
    #[must_use]
    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    /// Gives the number of files that are open now.
    #[must_use]
    pub fn live(&self) -> usize {
        self.live.load(Ordering::SeqCst)
    }
}

/// The time that the first read of each file of [`CountingSource`] waits, so that the reads of different threads overlap.
pub const READ_PAUSE: Duration = Duration::from_millis(10);

/// Opens a file of [`CountingSource`].
#[derive(Debug)]
pub struct CountingOpen {
    content: Box<[u8]>,
    counter: Arc<ReadCounter>,
}

impl ReadSourceOpen for CountingOpen {
    type Reader = CountingReader;

    fn open(self) -> RusticResult<Self::Reader> {
        _ = self.counter.opened.fetch_add(1, Ordering::SeqCst);
        let now = self.counter.live.fetch_add(1, Ordering::SeqCst) + 1;
        _ = self.counter.peak.fetch_max(now, Ordering::SeqCst);
        Ok(CountingReader {
            content: Cursor::new(self.content),
            counter: self.counter,
            paused: false,
        })
    }
}

/// Reads a file of [`CountingSource`]. The drop of the reader ends the read of the file.
#[derive(Debug)]
pub struct CountingReader {
    content: Cursor<Box<[u8]>>,
    counter: Arc<ReadCounter>,
    paused: bool,
}

impl Read for CountingReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if !self.paused {
            self.paused = true;
            thread::sleep(READ_PAUSE);
        }
        self.content.read(buf)
    }
}

impl Drop for CountingReader {
    fn drop(&mut self) {
        _ = self.counter.live.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A source of `count` files below the path `source`, with other content in each file.
///
/// Each file has a fixed size and no times, so two backups of the source give the same tree.
#[derive(Debug)]
pub struct CountingSource {
    count: u64,
    counter: Arc<ReadCounter>,
}

impl CountingSource {
    /// Creates a source of `count` files.
    #[must_use]
    pub fn new(count: u64) -> Self {
        Self {
            count,
            counter: Arc::default(),
        }
    }

    /// Gives the counter of the reads of this source.
    #[must_use]
    pub fn counter(&self) -> &ReadCounter {
        &self.counter
    }
}

impl ReadSource for CountingSource {
    type Open = CountingOpen;
    type Iter = std::vec::IntoIter<RusticResult<ReadSourceEntry<CountingOpen>>>;

    fn size(&self) -> RusticResult<Option<u64>> {
        Ok(None)
    }

    fn entries(&self) -> Self::Iter {
        (0..self.count)
            .map(|index| {
                let name = format!("f{index:03}");
                let content = content(index, 1_000);
                let meta = Metadata {
                    size: content.len() as u64,
                    ..Metadata::default()
                };
                Ok(ReadSourceEntry {
                    path: Path::new("source").join(&name),
                    node: Node::new_node(OsStr::new(&name), NodeType::File, meta),
                    open: Some(CountingOpen {
                        content,
                        counter: Arc::clone(&self.counter),
                    }),
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
    }
}

/// Backs up `source` into a new repository with the options `opts`, through `Repository::archive`.
///
/// # Errors
///
/// * If the function cannot create the repository, or the backup fails.
pub fn archive_counting_source(
    source: &CountingSource,
    opts: &BackupOptions,
) -> TestResult<SnapshotFile> {
    let repo = init_repo(&fault_injection_backend())?.to_indexed_ids()?;
    Ok(repo.archive(
        opts,
        source,
        SnapshotFile::default(),
        &[PathBuf::from("source")],
    )?)
}
