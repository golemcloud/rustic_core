pub(crate) mod file_archiver;
pub(crate) mod parent;
pub(crate) mod tree;
pub(crate) mod tree_archiver;

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::thread::scope;

use jiff::Zoned;
use log::warn;
use pariter::IteratorExt;

use crate::{
    Progress,
    archiver::{
        file_archiver::FileArchiver, parent::Parent, tree::TreeIterator,
        tree_archiver::TreeArchiver,
    },
    backend::{ReadSource, ReadSourceEntry, decrypt::DecryptFullBackend},
    blob::BlobType,
    error::{ErrorKind, FirstError, RusticError, RusticResult},
    index::{
        ReadGlobalIndex,
        indexer::{Indexer, SharedIndexer},
    },
    repofile::{configfile::ConfigFile, snapshotfile::SnapshotFile},
};

#[derive(thiserror::Error, Debug, displaydoc::Display)]
/// Tree stack empty
pub struct TreeStackEmptyError;

/// The `Archiver` is responsible for archiving files and trees.
/// It will read the file, chunk it, and write the chunks to the backend.
///
/// # Type Parameters
///
/// * `BE` - The backend type.
/// * `I` - The index to read from.
#[allow(missing_debug_implementations)]
#[allow(clippy::struct_field_names)]
pub struct Archiver<'a, BE: DecryptFullBackend, I: ReadGlobalIndex> {
    /// The `FileArchiver` is responsible for archiving files.
    file_archiver: FileArchiver<'a, BE, I>,

    /// The `TreeArchiver` is responsible for archiving trees.
    tree_archiver: TreeArchiver<'a, BE, I>,

    /// The parent snapshot to use.
    parent: Parent,

    /// The `SharedIndexer` is used to index the data.
    indexer: SharedIndexer<BE>,

    /// The backend to write to.
    be: BE,

    /// The backend to write to.
    index: &'a I,

    /// The `SnapshotFile` to write to.
    snap: SnapshotFile,

    /// Fail the backup on each error that makes it skip an entry, for example if it cannot read an entry.
    fail_on_read_error: bool,

    /// The number of threads that read and chunk files. If it is `None`, pariter uses its default.
    threads: Option<NonZeroUsize>,
}

impl<'a, BE: DecryptFullBackend, I: ReadGlobalIndex> Archiver<'a, BE, I> {
    /// Creates a new `Archiver`.
    ///
    /// # Arguments
    ///
    /// * `be` - The backend to write to.
    /// * `index` - The index to read from.
    /// * `config` - The config file.
    /// * `parent` - The parent snapshot to use.
    /// * `snap` - The `SnapshotFile` to write to.
    /// * `fail_on_read_error` - Fail the backup on each error that makes it skip an entry, for example if it cannot read an entry.
    /// * `threads` - The number of threads of each parallel stage. If it is `None`, each stage uses the default of pariter.
    ///
    /// # Errors
    ///
    /// * If sending the message to the raw packer fails.
    /// * If converting the data length to u64 fails
    pub fn new(
        be: BE,
        index: &'a I,
        config: &ConfigFile,
        parent: Parent,
        mut snap: SnapshotFile,
        fail_on_read_error: bool,
        threads: Option<NonZeroUsize>,
    ) -> RusticResult<Self> {
        let indexer = Indexer::new(be.clone()).into_shared();
        let mut summary = snap.summary.take().unwrap_or_default();
        summary.backup_start = Zoned::now();

        let file_archiver = FileArchiver::new(be.clone(), index, indexer.clone(), config, threads)?;
        let tree_archiver =
            TreeArchiver::new(be.clone(), index, indexer.clone(), config, summary, threads)?;

        Ok(Self {
            file_archiver,
            tree_archiver,
            parent,
            indexer,
            be,
            index,
            snap,
            fail_on_read_error,
            threads,
        })
    }

    /// Archives the given source.
    ///
    /// This will archive all files and trees in the given source.
    ///
    /// # Type Parameters
    ///
    /// * `R` - The type of the source.
    ///
    /// # Arguments
    ///
    /// * `index` - The index to read from.
    /// * `src` - The source to archive.
    /// * `backup_path` - The path to the backup.
    /// * `as_path` - The path to archive the backup as.
    /// * `skip_identical_parent` - skip saving of snapshot if tree is identical to parent tree.
    /// * `p` - The progress bar.
    ///
    /// # Errors
    ///
    /// * If sending the message to the raw packer fails.
    /// * If the index file could not be serialized.
    /// * If the time is not in the range of `Local::now()`.
    /// * If `fail_on_read_error` is set and this function cannot read an entry. Then this function writes no snapshot file.
    #[allow(clippy::too_many_lines)]
    pub fn archive<R>(
        mut self,
        src: &R,
        backup_path: &Path,
        as_path: Option<&PathBuf>,
        skip_identical_parent: bool,
        no_scan: bool,
        p: &Progress,
    ) -> RusticResult<SnapshotFile>
    where
        R: ReadSource + 'static,
        <R as ReadSource>::Open: Send,
        <R as ReadSource>::Iter: Send,
    {
        let fail_on_read_error = self.fail_on_read_error;
        let threads = self.threads;
        // Keeps the first error of an entry if `fail_on_read_error` is set.
        let first_error = FirstError::default();

        scope(|s| -> RusticResult<_> {
            // determine backup size in parallel to running backup
            let src_size_handle = s.spawn(|| {
                if !no_scan && !p.is_hidden() {
                    match src.size() {
                        Ok(Some(size)) => p.set_length(size),
                        Ok(None) => {}
                        Err(err) => warn!("error determining backup size: {}", err.display_log()),
                    }
                }
            });

            // stop reading the source after the first error of an entry
            let entries = src.entries().take_while(|_| !first_error.is_set());

            // filter out errors and handle as_path
            let iter = entries.filter_map(|item| match item {
                Err(err) => {
                    if fail_on_read_error {
                        first_error.store(err);
                    } else {
                        warn!("ignoring error: {}", err.display_log());
                    }
                    None
                }
                Ok(ReadSourceEntry { path, node, open }) => {
                    let snapshot_path = if let Some(as_path) = as_path {
                        as_path
                            .clone()
                            .join(path.strip_prefix(backup_path).unwrap())
                    } else {
                        path
                    };
                    Some(if node.is_dir() {
                        (snapshot_path, node, open)
                    } else {
                        (
                            snapshot_path
                                .parent()
                                .expect("file path should have a parent!")
                                .to_path_buf(),
                            node,
                            open,
                        )
                    })
                }
            });
            // handle beginning and ending of trees
            let iter = TreeIterator::new(iter);

            // use parent snapshot
            iter.filter_map(
                |item| match self.parent.process(&self.be, self.index, item) {
                    Ok(item) => Some(item),
                    Err(err) => {
                        if fail_on_read_error {
                            first_error.store(RusticError::with_source(
                                ErrorKind::Internal,
                                "The tree stack of the parent snapshot is empty.",
                                err,
                            ));
                        } else {
                            warn!("ignoring error reading parent snapshot: {err:?}");
                        }
                        None
                    }
                },
            )
            // archive files in parallel
            .parallel_map_scoped_custom(
                s,
                |builder| match threads {
                    Some(threads) => builder.threads(threads.get()),
                    None => builder,
                },
                |item| self.file_archiver.process(item, p),
            )
            .readahead_scoped(s)
            .filter_map(|item| match item {
                Ok(item) => Some(item),
                Err(err) => {
                    if fail_on_read_error {
                        first_error.store(err);
                    } else {
                        warn!("ignoring error: {}", err.display_log());
                    }
                    None
                }
            })
            .try_for_each(|item| self.tree_archiver.add(item))?;

            src_size_handle
                .join()
                .expect("Scoped Size Handler thread should not panic!");

            Ok(())
        })?;

        // If the backup cannot read an entry, return the error here, before the backup writes the snapshot file.
        first_error.into_result()?;

        let stats = self.file_archiver.finalize()?;
        let (id, mut summary) = self.tree_archiver.finalize(self.parent.tree_id())?;
        stats.apply(&mut summary, BlobType::Data);
        self.snap.tree = id;

        self.indexer.write().unwrap().finalize()?;

        summary.finalize(&self.snap.time);
        self.snap.summary = Some(summary);

        if !skip_identical_parent || Some(self.snap.tree) != self.parent.tree_id() {
            let id = self.be.save_file(&self.snap)?;
            self.snap.id = id.into();
        }

        p.finish();
        Ok(self.snap)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsStr,
        io::{Cursor, Read},
        num::NonZeroUsize,
        path::Path,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        thread,
        time::Duration,
    };

    use bytes::Bytes;
    use rstest::rstest;

    use super::Archiver;
    use crate::{
        Id, Progress,
        archiver::parent::Parent,
        backend::{
            BytesList, FileType, ReadBackend, ReadSource, ReadSourceEntry, ReadSourceOpen,
            WriteBackend,
            decrypt::DecryptBackend,
            node::{Metadata, Node, NodeType},
        },
        blob::tree::TreeId,
        chunker::rabin::random_poly,
        crypto::{CryptoKey, aespoly1305::Key},
        error::{ErrorKind, RusticError, RusticResult},
        index::{
            GlobalIndex,
            binarysorted::{IndexCollector, IndexType},
        },
        repofile::{ConfigFile, SnapshotFile, configfile::RepositoryId},
    };

    /// The number of directories of the source. Each directory gives one tree blob.
    const DIRS: usize = 32;

    /// The number of files in each directory. Each file gives one data blob.
    const FILES_PER_DIR: usize = 2;

    /// The first bytes of each file. The key uses them to find the data blobs of the files.
    const FILE_MARKER: &[u8] = b"gol-651 file ";

    /// The first bytes of each tree blob. The key uses them to find the tree blobs.
    const TREE_MARKER: &[u8] = b"{\"nodes\":";

    /// The last bytes of each file, so that a file is larger than its marker and its name.
    const PADDING: [u8; 1024] = [b'.'; 1024];

    /// The time that each call of the slow stage waits, so that the calls of different threads overlap.
    const PAUSE: Duration = Duration::from_millis(10);

    /// The number of threads of the controls.
    ///
    /// The tests expect more than [`LIMIT_THREADS`] calls at the same time with it.
    const CONTROL_THREADS: NonZeroUsize = NonZeroUsize::new(6).unwrap();

    /// The number of threads of the limit tests.
    const LIMIT_THREADS: NonZeroUsize = NonZeroUsize::new(2).unwrap();

    /// A parallel stage of a backup.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Stage {
        /// Reads and chunks files.
        Files,
        /// Compresses and encrypts data blobs.
        Data,
        /// Compresses and encrypts tree blobs.
        Trees,
    }

    /// Counts the live calls of one stage.
    #[derive(Debug, Default)]
    struct Probe {
        /// The number of calls that run now.
        live: AtomicUsize,
        /// The maximum number of calls that ran at the same time.
        peak: AtomicUsize,
        /// The number of calls.
        calls: AtomicUsize,
        /// If this is set, each call waits [`PAUSE`].
        slow: AtomicBool,
    }

    impl Probe {
        /// Starts a call.
        fn enter(&self) {
            _ = self.calls.fetch_add(1, Ordering::SeqCst);
            let now = self.live.fetch_add(1, Ordering::SeqCst) + 1;
            _ = self.peak.fetch_max(now, Ordering::SeqCst);
        }

        /// Waits [`PAUSE`] if the stage is slow.
        fn pause(&self) {
            if self.slow.load(Ordering::SeqCst) {
                thread::sleep(PAUSE);
            }
        }

        /// Ends a call.
        fn leave(&self) {
            _ = self.live.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// The probes of the three stages of one backup.
    #[derive(Debug, Default)]
    struct Probes {
        files: Probe,
        data: Probe,
        trees: Probe,
    }

    impl Probes {
        /// Gives the probe of a stage.
        const fn of(&self, stage: Stage) -> &Probe {
            match stage {
                Stage::Files => &self.files,
                Stage::Data => &self.data,
                Stage::Trees => &self.trees,
            }
        }
    }

    /// A key that counts the live encryptions of data blobs and of tree blobs.
    ///
    /// The packer encrypts a blob in `process_data`, inside its parallel stage.
    /// Thus a live encryption of a blob is a live call of the stage of its packer.
    /// Other encryptions, for example of index files and pack headers, are not counted.
    #[derive(Clone, Copy)]
    struct CountingKey {
        key: Key,
        probes: &'static Probes,
    }

    impl CryptoKey for CountingKey {
        fn decrypt_data(&self, data: &[u8]) -> RusticResult<Vec<u8>> {
            self.key.decrypt_data(data)
        }

        fn encrypt_data(&self, data: &[u8]) -> RusticResult<Vec<u8>> {
            let probe = if data.starts_with(FILE_MARKER) {
                Some(&self.probes.data)
            } else if data.starts_with(TREE_MARKER) {
                Some(&self.probes.trees)
            } else {
                None
            };
            if let Some(probe) = probe {
                probe.enter();
                probe.pause();
            }
            let result = self.key.encrypt_data(data);
            if let Some(probe) = probe {
                probe.leave();
            }
            result
        }
    }

    /// A backend that accepts each write and keeps nothing.
    #[derive(Debug)]
    struct DiscardBackend;

    impl ReadBackend for DiscardBackend {
        fn location(&self) -> String {
            "discard".to_string()
        }

        fn list_with_size(&self, _tpe: FileType) -> RusticResult<Vec<(Id, u32)>> {
            Ok(Vec::new())
        }

        fn read_full(&self, _tpe: FileType, id: &Id) -> RusticResult<Bytes> {
            Err(
                RusticError::new(ErrorKind::Backend, "The backend holds no file `{id}`.")
                    .attach_context("id", id.to_string()),
            )
        }

        fn read_partial(
            &self,
            tpe: FileType,
            id: &Id,
            _cacheable: bool,
            _offset: u32,
            _length: u32,
        ) -> RusticResult<Bytes> {
            self.read_full(tpe, id)
        }

        fn warmup_path(&self, _tpe: FileType, id: &Id) -> String {
            id.to_string()
        }
    }

    impl WriteBackend for DiscardBackend {
        fn create(&self) -> RusticResult<()> {
            Ok(())
        }

        fn write_bytes(
            &self,
            _tpe: FileType,
            _id: &Id,
            _cacheable: bool,
            _content: BytesList,
        ) -> RusticResult<()> {
            Ok(())
        }

        fn remove(&self, _tpe: FileType, _id: &Id, _cacheable: bool) -> RusticResult<()> {
            Ok(())
        }
    }

    /// Opens a file of [`CountingSource`]. A call of the files stage is live from `open` until the drop of the reader.
    #[derive(Debug)]
    struct CountingOpen {
        content: Box<[u8]>,
        probe: &'static Probe,
    }

    impl ReadSourceOpen for CountingOpen {
        type Reader = CountingReader;

        fn open(self) -> RusticResult<Self::Reader> {
            self.probe.enter();
            Ok(CountingReader {
                content: Cursor::new(self.content),
                probe: self.probe,
                paused: false,
            })
        }
    }

    /// Reads a file of [`CountingSource`], and ends the call of the files stage when it is dropped.
    #[derive(Debug)]
    struct CountingReader {
        content: Cursor<Box<[u8]>>,
        probe: &'static Probe,
        paused: bool,
    }

    impl Read for CountingReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if !self.paused {
                self.paused = true;
                self.probe.pause();
            }
            self.content.read(buf)
        }
    }

    impl Drop for CountingReader {
        fn drop(&mut self) {
            self.probe.leave();
        }
    }

    /// A source of [`DIRS`] directories with [`FILES_PER_DIR`] files each. Each file has other content.
    struct CountingSource {
        probe: &'static Probe,
    }

    impl ReadSource for CountingSource {
        type Open = CountingOpen;
        type Iter = std::vec::IntoIter<RusticResult<ReadSourceEntry<CountingOpen>>>;

        fn size(&self) -> RusticResult<Option<u64>> {
            Ok(None)
        }

        fn entries(&self) -> Self::Iter {
            (0..DIRS)
                .flat_map(|dir| (0..FILES_PER_DIR).map(move |file| (dir, file)))
                .map(|(dir, file)| {
                    let name = format!("f{file}");
                    let content = [FILE_MARKER, format!("{dir}/{file}").as_bytes(), &PADDING]
                        .concat()
                        .into_boxed_slice();
                    let meta = Metadata {
                        size: content.len() as u64,
                        ..Metadata::default()
                    };
                    Ok(ReadSourceEntry {
                        path: Path::new(&format!("d{dir:02}")).join(&name),
                        node: Node::new_node(OsStr::new(&name), NodeType::File, meta),
                        open: Some(CountingOpen {
                            content,
                            probe: self.probe,
                        }),
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
        }
    }

    /// The probes and the tree of one backup.
    struct Backup {
        probes: &'static Probes,
        tree: TreeId,
    }

    /// Backs up [`CountingSource`] with the given number of threads, and makes the given stage slow.
    fn backup(threads: Option<NonZeroUsize>, slow: Option<Stage>) -> Backup {
        let probes: &'static Probes = Box::leak(Box::default());
        if let Some(stage) = slow {
            probes.of(stage).slow.store(true, Ordering::SeqCst);
        }
        let be = DecryptBackend::new(
            Arc::new(DiscardBackend),
            CountingKey {
                key: Key::new(),
                probes,
            },
        );
        let index = GlobalIndex::new_from_index(IndexCollector::new(IndexType::Full).into_index());
        let config = ConfigFile::new(2, RepositoryId::default(), random_poly().unwrap());
        let parent = Parent::new(&be, &index, Vec::new(), false, false);
        let archiver = Archiver::new(
            be,
            &index,
            &config,
            parent,
            SnapshotFile::default(),
            false,
            threads,
        )
        .unwrap();
        let snap = archiver
            .archive(
                &CountingSource {
                    probe: &probes.files,
                },
                Path::new(""),
                None,
                false,
                true,
                &Progress::hidden(),
            )
            .unwrap();
        Backup {
            probes,
            tree: snap.tree,
        }
    }

    /// Gives the peak of a stage, and checks that the stage had at least [`CONTROL_THREADS`] + 1 calls.
    ///
    /// With fewer calls, a peak could not show that the backup ignores the number of threads.
    fn peak(backup: &Backup, stage: Stage) -> usize {
        let probe = backup.probes.of(stage);
        let calls = probe.calls.load(Ordering::SeqCst);
        assert!(
            calls > CONTROL_THREADS.get(),
            "The {stage:?} stage had {calls} calls. The test needs more than {CONTROL_THREADS}."
        );
        assert_eq!(probe.live.load(Ordering::SeqCst), 0, "{stage:?}");
        let peak = probe.peak.load(Ordering::SeqCst);
        println!("The {stage:?} stage had {calls} calls. At most {peak} ran at the same time.");
        peak
    }

    #[rstest]
    fn a_number_of_threads_limits_each_stage(
        #[values(Stage::Files, Stage::Data, Stage::Trees)] stage: Stage,
    ) {
        let backup = backup(Some(LIMIT_THREADS), Some(stage));
        let peak = peak(&backup, stage);
        assert!(
            (1..=LIMIT_THREADS.get()).contains(&peak),
            "The {stage:?} stage ran {peak} calls at the same time. The limit is {LIMIT_THREADS} threads."
        );
    }

    #[rstest]
    fn more_threads_run_more_calls_of_each_stage(
        #[values(Stage::Files, Stage::Data, Stage::Trees)] stage: Stage,
    ) {
        let backup = backup(Some(CONTROL_THREADS), Some(stage));
        let peak = peak(&backup, stage);
        assert!(
            (LIMIT_THREADS.get() + 1..=CONTROL_THREADS.get()).contains(&peak),
            "The {stage:?} stage ran {peak} calls at the same time with {CONTROL_THREADS} threads."
        );
    }

    #[rstest]
    fn without_the_option_each_stage_follows_the_host(
        #[values(Stage::Files, Stage::Data, Stage::Trees)] stage: Stage,
    ) {
        let cores = thread::available_parallelism().unwrap().get();
        let backup = backup(None, Some(stage));
        let peak = peak(&backup, stage);
        assert!(
            (cores.min(3)..=cores).contains(&peak),
            "The {stage:?} stage ran {peak} calls at the same time on a host with {cores} cores."
        );
    }

    #[rstest]
    fn one_thread_gives_the_tree_of_the_default(
        #[values(Stage::Files, Stage::Data, Stage::Trees)] stage: Stage,
    ) {
        let backup_one = backup(Some(NonZeroUsize::MIN), Some(stage));
        assert_eq!(peak(&backup_one, stage), 1, "{stage:?}");
        assert_eq!(backup_one.tree, backup(None, None).tree);
    }
}
