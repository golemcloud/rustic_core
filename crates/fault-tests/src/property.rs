//! A property test that injects random faults into a sequence of repository operations.
//!
//! Each case creates a repository with a master key, and runs a random sequence of steps: backups,
//! restores, snapshot deletes and prunes. Each step can inject random faults into the backend
//! reads, writes and listings. A restore can also get a destination that fails writes.
//! After each step, the case checks these properties:
//!
//! * The step gives a result or an error, and no thread panics.
//! * A failed backup writes no snapshot file, and a backup of a tree with an unreadable entry fails.
//! * A successful restore gives the saved tree: the same files, contents, symlinks, permissions,
//!   modification times and hard links.
//!
//! At the end, the case clears the faults, opens the repository again, and restores each snapshot
//! that the backend lists. Each restore must give the saved tree. A case with a cache opens the
//! repository with a new, empty cache for this check.
//!
//! The backups use `fail_on_read_error`, and the restores use `fail_on_metadata_error`.
//! The generator is a seeded pseudo-random generator. Each case prints its plan and its seed
//! before it runs, and the seed reproduces the plan. The order of the backend calls of different
//! threads can change between runs, so a fault can hit a different call in a new run.

use std::{
    array,
    collections::{BTreeMap, BTreeSet},
    env,
    fmt::Write as _,
    fs::{self, File, FileTimes},
    iter,
    num::NonZeroUsize,
    os::unix::fs::{MetadataExt, PermissionsExt, symlink},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use rustic_core::{
    BackupOptions, ConfigOptions, Credentials, ErrorKind, FileType, KeyOptions, LimitOption,
    LocalDestination, LsOptions, OpenStatus, PathList, PruneOptions, ReadBackend, Repository,
    RepositoryBackends, RepositoryOptions, RestoreOptions, RusticError, RusticResult,
    jiff::Span,
    repofile::{MasterKey, SnapshotFile, SnapshotId},
};
use rustic_testing::{
    TestResult,
    backend::fault_injection_backend::{BackendCall, BackendOp, Fault, FaultInjectionBackend},
};
use tempfile::{TempDir, tempdir};

use crate::{
    fixtures::{content, fault_injection_backend, require_non_root, set_mode},
    panics::{count_panics, wait_until_released},
    volume::{Tmpfs, enter_user_mount_namespace},
};

/// The number of cases of a property scenario in the binary of this crate.
const CASES: usize = 256;

/// The number of cases for each fixture of the scenario with small volumes.
const VOLUME_CASES: usize = 128;

/// The seed of the first case, if the environment variable [`SEED_VARIABLE`] does not set it.
const DEFAULT_SEED: u64 = 0x0601;

/// The environment variable that sets the seed of the first case.
const SEED_VARIABLE: &str = "RUSTIC_FAULT_SEED";

/// The maximum time for the threads of a step to stop after the step returns.
const RELEASE_TIMEOUT: Duration = Duration::from_secs(30);

/// The directories of each source tree, below the root of the tree.
const DIRS: [&str; 3] = ["d0", "d0/d1", "d2"];

/// The permission bits of the files of a tree.
const FILE_MODES: [u32; 4] = [0o644, 0o600, 0o755, 0o444];

/// The permission bits of the directories of a tree.
const DIR_MODES: [u32; 3] = [0o755, 0o700, 0o555];

/// The targets of the symlinks of a tree. Some targets do not exist.
const LINK_TARGETS: [&str; 4] = ["f0", "../f0", "missing", "d0"];

/// The directories of a restore that a destination fault can make read-only.
const READ_ONLY_TARGETS: [&str; 4] = ["data", "data/d0", "data/d0/d1", "data/d2"];

/// The backend calls of the creation of a repository with a master key.
const INIT_CALLS: [(BackendOp, FileType); 2] = [
    (BackendOp::List, FileType::Config),
    (BackendOp::Write, FileType::Config),
];

/// The backend calls of a backup, with the open of the repository and the load of the index.
const BACKUP_CALLS: [(BackendOp, FileType); 10] = [
    (BackendOp::List, FileType::Config),
    (BackendOp::List, FileType::Index),
    (BackendOp::List, FileType::Snapshot),
    (BackendOp::ReadFull, FileType::Config),
    (BackendOp::ReadFull, FileType::Index),
    (BackendOp::ReadFull, FileType::Snapshot),
    (BackendOp::ReadPartial, FileType::Pack),
    (BackendOp::Write, FileType::Pack),
    (BackendOp::Write, FileType::Index),
    (BackendOp::Write, FileType::Snapshot),
];

/// The backend calls of a restore, with the open of the repository and the load of the index.
const RESTORE_CALLS: [(BackendOp, FileType); 7] = [
    (BackendOp::List, FileType::Config),
    (BackendOp::List, FileType::Index),
    (BackendOp::ReadFull, FileType::Config),
    (BackendOp::ReadFull, FileType::Index),
    (BackendOp::ReadFull, FileType::Snapshot),
    (BackendOp::ReadFull, FileType::Pack),
    (BackendOp::ReadPartial, FileType::Pack),
];

/// The backend calls of a snapshot delete, with the open of the repository.
const DELETE_CALLS: [(BackendOp, FileType); 3] = [
    (BackendOp::List, FileType::Config),
    (BackendOp::ReadFull, FileType::Config),
    (BackendOp::Remove, FileType::Snapshot),
];

/// The backend calls of a prune, with the open of the repository.
const PRUNE_CALLS: [(BackendOp, FileType); 13] = [
    (BackendOp::List, FileType::Config),
    (BackendOp::List, FileType::Index),
    (BackendOp::List, FileType::Snapshot),
    (BackendOp::List, FileType::Pack),
    (BackendOp::ReadFull, FileType::Config),
    (BackendOp::ReadFull, FileType::Index),
    (BackendOp::ReadFull, FileType::Snapshot),
    (BackendOp::ReadFull, FileType::Pack),
    (BackendOp::ReadPartial, FileType::Pack),
    (BackendOp::Write, FileType::Pack),
    (BackendOp::Write, FileType::Index),
    (BackendOp::Remove, FileType::Index),
    (BackendOp::Remove, FileType::Pack),
];

/// The backend operations, in the order of the counts of injected faults.
const OPS: [BackendOp; 5] = [
    BackendOp::List,
    BackendOp::ReadFull,
    BackendOp::ReadPartial,
    BackendOp::Write,
    BackendOp::Remove,
];

/// The faults that the backend can inject.
const FAULTS: [Fault; 3] = [Fault::Error, Fault::Corrupt, Fault::Truncate];

/// The numbers of reader threads of a restore. `None` is the default.
const THREADS: [Option<NonZeroUsize>; 4] = [
    None,
    NonZeroUsize::new(1),
    NonZeroUsize::new(2),
    NonZeroUsize::new(4),
];

/// A seeded pseudo-random generator (xorshift64*).
///
/// The same seed gives the same values.
#[derive(Debug)]
struct Rng(u64);

impl Rng {
    /// Creates a generator from `seed`.
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    /// Gives the next value.
    fn next_u64(&mut self) -> u64 {
        let state = self.0 ^ (self.0 >> 12);
        let state = state ^ (state << 25);
        let state = state ^ (state >> 27);
        self.0 = state;
        state.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Gives a value from 0 to `n - 1`. `n` must not be 0.
    fn below(&mut self, n: usize) -> usize {
        // The upper 32 bits fit into `usize` on each supported platform.
        usize::try_from(self.next_u64() >> 32).unwrap_or_default() % n
    }

    /// Gives a value of `values`. `values` must not be empty.
    fn pick<T: Copy>(&mut self, values: &[T]) -> T {
        values[self.below(values.len())]
    }

    /// Gives `true` in `numerator` of `denominator` calls on average.
    fn chance(&mut self, numerator: usize, denominator: usize) -> bool {
        self.below(denominator) < numerator
    }

    /// Gives a random time between the years 2020 and 2023.
    fn time(&mut self) -> Duration {
        let nanos = u32::try_from(self.next_u64() % 1_000_000_000).unwrap_or_default();
        Duration::new(1_600_000_000 + self.next_u64() % 100_000_000, nanos)
    }
}

/// A directory of a source tree.
#[derive(Debug, Clone, Copy)]
enum Location {
    /// The root of the tree.
    Root,
    /// The directory with this index in [`DIRS`].
    Dir(usize),
}

impl Location {
    /// Generates a location. Each directory of [`DIRS`] and the root have the same chance.
    fn generate(rng: &mut Rng) -> Self {
        rng.below(DIRS.len() + 1)
            .checked_sub(1)
            .map_or(Self::Root, Self::Dir)
    }

    /// Gives the path of the location relative to the root of the tree.
    fn path(self) -> &'static str {
        match self {
            Self::Root => "",
            Self::Dir(index) => DIRS[index],
        }
    }
}

/// A file of a source tree.
#[derive(Debug)]
struct FileSpec {
    /// The directory of the file.
    dir: Location,
    /// The length of the file.
    len: usize,
    /// The seed of the content of the file.
    seed: u64,
    /// The permission bits of the file.
    mode: u32,
    /// The modification time of the file, after the epoch.
    mtime: Duration,
}

/// A symlink of a source tree.
#[derive(Debug)]
struct LinkSpec {
    /// The directory of the symlink.
    dir: Location,
    /// The target of the symlink.
    target: &'static str,
}

/// A second name of a file of a source tree.
#[derive(Debug)]
struct HardLinkSpec {
    /// The directory of the second name.
    dir: Location,
    /// The index of the file.
    file: usize,
}

/// An entry of a source tree that the backup cannot read.
#[derive(Debug)]
enum Unreadable {
    /// The file with this index has no permissions.
    File(usize),
    /// The directory with this index in [`DIRS`] has no permissions, as in [`Location::Dir`].
    Dir(usize),
}

/// A source tree.
///
/// The tree has the directories of [`DIRS`], the files `f<index>`, the symlinks `l<index>`, and
/// sometimes a second name `h` of a file.
#[derive(Debug)]
struct TreeSpec {
    /// The files.
    files: Box<[FileSpec]>,
    /// The symlinks.
    links: Box<[LinkSpec]>,
    /// The second name of a file.
    hard_link: Option<HardLinkSpec>,
    /// The permission bits of each directory of [`DIRS`].
    dir_modes: [u32; DIRS.len()],
    /// The modification time of each directory of [`DIRS`].
    dir_mtimes: [Duration; DIRS.len()],
    /// The entry that the backup cannot read.
    unreadable: Option<Unreadable>,
}

impl TreeSpec {
    /// Generates a tree.
    ///
    /// # Arguments
    ///
    /// * `rng` - The generator
    /// * `allow_unreadable` - Whether the tree may have an entry that the backup cannot read
    fn generate(rng: &mut Rng, allow_unreadable: bool) -> Self {
        let files: Box<[FileSpec]> = (0..=rng.below(8))
            .map(|_| FileSpec {
                dir: Location::generate(rng),
                len: match rng.below(8) {
                    0 => 0,
                    1..=4 => 1 + rng.below(4_096),
                    5 | 6 => 4_096 + rng.below(20_000),
                    _ => 500_000 + rng.below(200_000),
                },
                seed: rng.next_u64(),
                mode: rng.pick(&FILE_MODES),
                mtime: rng.time(),
            })
            .collect();
        let links = (0..rng.below(3))
            .map(|_| LinkSpec {
                dir: Location::generate(rng),
                target: rng.pick(&LINK_TARGETS),
            })
            .collect();
        let hard_link = rng.chance(1, 4).then(|| HardLinkSpec {
            dir: Location::generate(rng),
            file: rng.below(files.len()),
        });
        let dir_modes = [(); DIRS.len()].map(|()| rng.pick(&DIR_MODES));
        let dir_mtimes = [(); DIRS.len()].map(|()| rng.time());
        let unreadable = (allow_unreadable && rng.chance(1, 6)).then(|| {
            if rng.chance(1, 2) {
                Unreadable::File(rng.below(files.len()))
            } else {
                Unreadable::Dir(rng.below(DIRS.len()))
            }
        });
        Self {
            files,
            links,
            hard_link,
            dir_modes,
            dir_mtimes,
            unreadable,
        }
    }

    /// Gives the path of the file with the index `index`, relative to the root of the tree.
    fn file_path(&self, index: usize) -> PathBuf {
        Path::new(self.files[index].dir.path()).join(format!("f{index}"))
    }

    /// Writes the tree below the directory `root`.
    ///
    /// The function sets the times and the permissions of the directories last, so that the
    /// entries of a directory do not change its time, and a read-only directory can get entries.
    ///
    /// # Errors
    ///
    /// * If the function cannot write an entry, or set its time or its permissions.
    fn write(&self, root: &Path) -> TestResult<()> {
        DIRS.iter()
            .try_for_each(|dir| fs::create_dir_all(root.join(dir)))?;
        self.files
            .iter()
            .enumerate()
            .try_for_each(|(index, file)| -> TestResult<()> {
                let path: Box<Path> = root.join(self.file_path(index)).into_boxed_path();
                fs::write(&path, content(file.seed, file.len))?;
                set_mtime(&path, file.mtime)?;
                set_mode(&path, file.mode)
            })?;
        if let Some(link) = &self.hard_link {
            fs::hard_link(
                root.join(self.file_path(link.file)),
                root.join(link.dir.path()).join("h"),
            )?;
        }
        self.links
            .iter()
            .enumerate()
            .try_for_each(|(index, link)| {
                symlink(
                    link.target,
                    root.join(link.dir.path()).join(format!("l{index}")),
                )
            })?;
        DIRS.iter()
            .zip(self.dir_mtimes)
            .zip(self.dir_modes)
            .rev()
            .try_for_each(|((dir, mtime), mode)| -> TestResult<()> {
                let path: Box<Path> = root.join(dir).into_boxed_path();
                set_mtime(&path, mtime)?;
                set_mode(&path, mode)
            })?;
        match self.unreadable {
            Some(Unreadable::File(index)) => set_mode(&root.join(self.file_path(index)), 0),
            Some(Unreadable::Dir(index)) => set_mode(&root.join(DIRS[index]), 0),
            None => Ok(()),
        }
    }
}

/// Sets the modification time of the file or directory `path` to `mtime` after the epoch.
///
/// # Errors
///
/// * If the function cannot open the entry or set its time.
fn set_mtime(path: &Path, mtime: Duration) -> TestResult<()> {
    File::open(path)?.set_times(FileTimes::new().set_modified(SystemTime::UNIX_EPOCH + mtime))?;
    Ok(())
}

/// Gives the permissions `0o755` to each directory below `dir`, so that the directory can be removed.
///
/// # Errors
///
/// * If the function cannot read a directory or set its permissions.
fn make_removable(dir: &Path) -> TestResult<()> {
    set_mode(dir, 0o755)?;
    fs::read_dir(dir)?.try_for_each(|entry| -> TestResult<()> {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            make_removable(&entry.path())
        } else {
            Ok(())
        }
    })
}

/// When a fault hits a call.
#[derive(Debug, Clone, Copy)]
enum Trigger {
    /// Only the call with this index among the matching calls. The first index is 0.
    Nth(usize),
    /// Each call from this index on among the matching calls.
    From(usize),
}

/// A rule that injects a fault into matching backend calls.
#[derive(Debug, Clone, Copy)]
struct FaultRule {
    /// The operation of the matching calls.
    op: BackendOp,
    /// The file type of the matching calls. `None` matches each file type.
    tpe: Option<FileType>,
    /// The fault to inject.
    fault: Fault,
    /// When the fault hits a matching call.
    trigger: Trigger,
}

impl FaultRule {
    /// Generates 0 to 2 rules for the backend calls of `calls`.
    ///
    /// A rule matches one of the calls, or the operation of one of the calls with each file type.
    fn generate_plan(rng: &mut Rng, calls: &[(BackendOp, FileType)]) -> Box<[Self]> {
        let count = match rng.below(4) {
            0 => 0,
            3 => 2,
            _ => 1,
        };
        (0..count)
            .map(|_| {
                let (op, tpe) = rng.pick(calls);
                Self {
                    op,
                    tpe: rng.chance(3, 4).then_some(tpe),
                    fault: rng.pick(&FAULTS),
                    trigger: if rng.chance(1, 2) {
                        Trigger::Nth(rng.below(3))
                    } else {
                        Trigger::From(rng.below(3))
                    },
                }
            })
            .collect()
    }

    /// Gives the fault for `call`, and counts `call` in `count` if it matches.
    fn fault_for(&self, call: &BackendCall, count: &AtomicUsize) -> Option<Fault> {
        (self.op == call.op && self.tpe.is_none_or(|tpe| tpe == call.tpe))
            .then(|| count.fetch_add(1, Ordering::SeqCst))
            .filter(|index| match self.trigger {
                Trigger::Nth(nth) => *index == nth,
                Trigger::From(from) => *index >= from,
            })
            .map(|_| self.fault)
    }
}

/// A fault of a restore destination.
#[derive(Debug, Clone, Copy)]
enum DestFault {
    /// The destination has no fault.
    None,
    /// The directory with this index in [`READ_ONLY_TARGETS`] becomes read-only after the plan.
    ReadOnlyDir(usize),
    /// The destination is a tmpfs with this size in bytes.
    Volume(u64),
}

/// A step of a case.
#[derive(Debug)]
enum Step {
    /// Backs up a new tree.
    Backup {
        /// The tree.
        tree: TreeSpec,
        /// The faults.
        faults: Box<[FaultRule]>,
    },
    /// Restores a saved snapshot.
    Restore {
        /// Selects the snapshot among the saved snapshots.
        pick: usize,
        /// The number of reader threads.
        threads: Option<NonZeroUsize>,
        /// The fault of the destination.
        dest: DestFault,
        /// The faults.
        faults: Box<[FaultRule]>,
    },
    /// Deletes a saved snapshot.
    Delete {
        /// Selects the snapshot among the saved snapshots.
        pick: usize,
        /// The faults.
        faults: Box<[FaultRule]>,
    },
    /// Prunes the repository.
    Prune {
        /// The faults.
        faults: Box<[FaultRule]>,
    },
}

/// The kinds of faults that a run can inject, apart from backend faults.
#[derive(Debug, Clone, Copy)]
struct Faults {
    /// Source trees can have an entry that the backup cannot read.
    unreadable: bool,
    /// A restore can get a read-only directory in its destination.
    read_only: bool,
    /// A restore can get a small tmpfs as its destination.
    volume: bool,
}

/// A case: the faults of the creation of the repository, and the steps.
#[derive(Debug)]
struct Case {
    /// The faults of the creation of the repository.
    init: Box<[FaultRule]>,
    /// The steps. The first step is a backup without faults of a readable tree.
    steps: Box<[Step]>,
}

impl Case {
    /// Generates a case.
    fn generate(rng: &mut Rng, faults: Faults) -> Self {
        let init = FaultRule::generate_plan(rng, &INIT_CALLS);
        let first = Step::Backup {
            tree: TreeSpec::generate(rng, false),
            faults: Box::new([]),
        };
        let others: Box<[Step]> = (0..2 + rng.below(6))
            .map(|_| match rng.below(8) {
                0..=2 => Step::Backup {
                    tree: TreeSpec::generate(rng, faults.unreadable),
                    faults: FaultRule::generate_plan(rng, &BACKUP_CALLS),
                },
                3..=5 => Step::Restore {
                    pick: rng.below(usize::MAX),
                    threads: rng.pick(&THREADS),
                    dest: match rng.below(4) {
                        0 if faults.read_only => {
                            DestFault::ReadOnlyDir(rng.below(READ_ONLY_TARGETS.len()))
                        }
                        1 | 2 if faults.volume => {
                            DestFault::Volume(4_096 * (1 + rng.below(64) as u64))
                        }
                        _ => DestFault::None,
                    },
                    faults: FaultRule::generate_plan(rng, &RESTORE_CALLS),
                },
                6 => Step::Delete {
                    pick: rng.below(usize::MAX),
                    faults: FaultRule::generate_plan(rng, &DELETE_CALLS),
                },
                _ => Step::Prune {
                    faults: FaultRule::generate_plan(rng, &PRUNE_CALLS),
                },
            })
            .collect();
        let steps = iter::once(first).chain(others).collect();
        Self { init, steps }
    }
}

/// The kind and the content of an entry of a tree.
#[derive(Debug, PartialEq, Eq)]
enum Kind {
    /// A directory.
    Dir,
    /// A file with its content.
    File(Box<[u8]>),
    /// A symlink with its target.
    Symlink(Box<Path>),
}

/// An entry of a tree, with the metadata that a restore must give.
#[derive(Debug, PartialEq, Eq)]
struct Entry {
    /// The kind and the content.
    kind: Kind,
    /// The permission bits. A symlink has none.
    mode: Option<u32>,
    /// The modification time.
    mtime: SystemTime,
}

impl Entry {
    /// Describes the entry without its content.
    fn describe(&self) -> Box<str> {
        let kind = match &self.kind {
            Kind::Dir => "directory".to_string(),
            Kind::File(data) => format!("file of {} bytes", data.len()),
            Kind::Symlink(target) => format!("symlink to `{}`", target.display()),
        };
        format!("{kind}, mode {:?}, mtime {:?}", self.mode, self.mtime).into_boxed_str()
    }
}

/// The entries of a tree below its root, and its hard links.
#[derive(Debug)]
struct Model {
    /// The entries, by path relative to the root.
    entries: BTreeMap<Box<Path>, Entry>,
    /// The groups of paths that are names of one file, for each file that has more than one name.
    /// Each group and the list of groups are sorted.
    links: Box<[Box<[Box<Path>]>]>,
}

/// The entries and the file identities that a scan of a tree collects.
#[derive(Debug, Default)]
struct Scan {
    /// The entries, by path relative to the root.
    entries: BTreeMap<Box<Path>, Entry>,
    /// The paths of each regular file that has more than one name, by device ID and inode number.
    names: BTreeMap<(u64, u64), BTreeSet<Box<Path>>>,
}

impl Model {
    /// Reads the tree below the directory `root`.
    ///
    /// # Errors
    ///
    /// * If the function cannot read an entry.
    fn read(root: &Path) -> TestResult<Self> {
        let scan = Self::read_dir(root, Path::new(""), Scan::default())?;
        let mut links: Box<[Box<[Box<Path>]>]> = scan
            .names
            .into_values()
            .map(|names| names.into_iter().collect())
            .collect();
        links.sort();
        Ok(Self {
            entries: scan.entries,
            links,
        })
    }

    /// Adds the entries below the directory `relative` of the tree `root` to `scan`.
    fn read_dir(root: &Path, relative: &Path, scan: Scan) -> TestResult<Scan> {
        fs::read_dir(root.join(relative))?.try_fold(scan, |mut scan, item| {
            let path: Box<Path> = relative.join(item?.file_name()).into_boxed_path();
            let full: Box<Path> = root.join(&path).into_boxed_path();
            let meta = fs::symlink_metadata(&full)?;
            let kind = if meta.is_dir() {
                Kind::Dir
            } else if meta.is_symlink() {
                Kind::Symlink(fs::read_link(&full)?.into_boxed_path())
            } else {
                Kind::File(fs::read(&full)?.into_boxed_slice())
            };
            if meta.is_file() && meta.nlink() > 1 {
                _ = scan
                    .names
                    .entry((meta.dev(), meta.ino()))
                    .or_default()
                    .insert(path.clone());
            }
            let entry = Entry {
                mode: (!meta.is_symlink()).then(|| meta.permissions().mode() & 0o7777),
                mtime: meta.modified()?,
                kind,
            };
            let is_dir = entry.kind == Kind::Dir;
            _ = scan.entries.insert(path.clone(), entry);
            if is_dir {
                Self::read_dir(root, &path, scan)
            } else {
                Ok(scan)
            }
        })
    }

    /// Checks that the tree below the directory `restored` equals this model.
    ///
    /// The function checks the paths first, then each entry, and then the hard links.
    ///
    /// # Errors
    ///
    /// * If the function cannot read the restored tree.
    /// * If the restored tree has other paths, or an entry that differs.
    /// * If the restored tree has other groups of names of one file.
    fn check(&self, restored: &Path) -> TestResult<()> {
        let found = Self::read(restored)?;
        let expected: BTreeSet<&Box<Path>> = self.entries.keys().collect();
        let paths: BTreeSet<&Box<Path>> = found.entries.keys().collect();
        if expected != paths {
            return Err(format!(
                "The restored tree has other paths. Missing: {:?}. Extra: {:?}.",
                expected.difference(&paths).collect::<Box<[_]>>(),
                paths.difference(&expected).collect::<Box<[_]>>()
            )
            .into());
        }
        self.entries.iter().try_for_each(|(path, entry)| {
            let other = found
                .entries
                .get(path)
                .ok_or("The restored tree has other paths.")?;
            if entry == other {
                Ok(())
            } else {
                Err(format!(
                    "The restored entry `{}` differs. Saved: {}. Restored: {}.",
                    path.display(),
                    entry.describe(),
                    other.describe()
                ))
            }
        })?;
        if self.links != found.links {
            return Err(format!(
                "The paths and entries of the restored tree are equal to the saved tree, but the hard links differ. Saved groups of names of one file: {:?}. Restored groups: {:?}.",
                self.links, found.links
            )
            .into());
        }
        Ok(())
    }
}

/// The numbers of injected faults, for each operation of [`OPS`] and each fault of [`FAULTS`].
#[derive(Debug, Default)]
struct Injected {
    /// The number of injected faults for each operation of [`OPS`].
    ops: [AtomicUsize; OPS.len()],
    /// The number of injected faults for each fault of [`FAULTS`].
    faults: [AtomicUsize; FAULTS.len()],
}

impl Injected {
    /// Counts one injected `fault` in the call `call`.
    fn count(&self, call: &BackendCall, fault: Fault) {
        [
            OPS.iter()
                .position(|op| *op == call.op)
                .map(|index| &self.ops[index]),
            FAULTS
                .iter()
                .position(|other| *other == fault)
                .map(|index| &self.faults[index]),
        ]
        .into_iter()
        .flatten()
        .for_each(|count| _ = count.fetch_add(1, Ordering::SeqCst));
    }

    /// Gives the counts for each operation and each fault.
    fn load(&self) -> ([usize; OPS.len()], [usize; FAULTS.len()]) {
        (
            self.ops
                .each_ref()
                .map(|count| count.load(Ordering::SeqCst)),
            self.faults
                .each_ref()
                .map(|count| count.load(Ordering::SeqCst)),
        )
    }
}

/// The name of the outcome of a restore that had a fault plan or a destination fault, succeeded,
/// and gave the saved tree.
const RESTORE_WITH_FAULTS_OK: &str = "ok with faults";

/// The number of results of each kind of step.
#[derive(Debug, Default)]
struct Stats {
    /// The number of results for each step name and outcome.
    results: BTreeMap<(&'static str, &'static str), usize>,
}

impl Stats {
    /// Counts one result of the step `step`.
    fn count(&mut self, step: &'static str, ok: bool) {
        self.add(step, if ok { "ok" } else { "error" });
    }

    /// Counts one outcome `outcome` of the step `step`.
    fn add(&mut self, step: &'static str, outcome: &'static str) {
        *self.results.entry((step, outcome)).or_default() += 1;
    }

    /// Describes the counts.
    fn describe(&self) -> Box<str> {
        self.results
            .iter()
            .fold(String::new(), |mut text, ((step, outcome), count)| {
                _ = write!(text, "{step} {outcome}: {count}; ");
                text
            })
            .into_boxed_str()
    }
}

/// The state of a case: the repository, the saved snapshots, and the counts.
struct State {
    /// The backend of the repository.
    backend: Arc<FaultInjectionBackend>,
    /// The master key of the repository.
    key: MasterKey,
    /// The directory of the cache, or `None` for a repository without a cache.
    cache: Option<TempDir>,
    /// The snapshots that the backend holds, with the saved trees.
    saved: Vec<(SnapshotId, Model)>,
    /// The numbers of injected faults.
    injected: Arc<Injected>,
    /// The number of panics in this process when the case started.
    panics: usize,
    /// The counts of the results.
    stats: Stats,
}

impl State {
    /// Creates the state of a case with a new backend.
    ///
    /// # Errors
    ///
    /// * If the function cannot create the directory of the cache.
    fn new(cache: bool) -> TestResult<Self> {
        Ok(Self {
            backend: fault_injection_backend(),
            key: MasterKey::new(),
            cache: cache.then(tempdir).transpose()?,
            saved: Vec::new(),
            injected: Arc::new(Injected::default()),
            panics: count_panics(),
            stats: Stats::default(),
        })
    }

    /// Gives the repository options: with the cache of the case, or without a cache.
    fn options(&self) -> RepositoryOptions {
        self.cache.as_ref().map_or_else(
            || RepositoryOptions::default().no_cache(true),
            |cache| RepositoryOptions::default().cache_dir(cache.path().to_path_buf()),
        )
    }

    /// Opens the repository.
    fn open(&self) -> RusticResult<Repository<OpenStatus>> {
        let backends = RepositoryBackends::new(self.backend.clone(), None);
        Repository::new(&self.options(), &backends)?.open(&Credentials::Masterkey(self.key.clone()))
    }

    /// Creates the repository.
    fn init(&self) -> RusticResult<()> {
        let backends = RepositoryBackends::new(self.backend.clone(), None);
        _ = Repository::new(&self.options(), &backends)?.init(
            &Credentials::Masterkey(self.key.clone()),
            &KeyOptions::default(),
            &ConfigOptions::default(),
        )?;
        Ok(())
    }

    /// Injects the faults of `rules` into the backend calls that come after.
    fn inject(&self, rules: &[FaultRule]) {
        let rules: Box<[FaultRule]> = rules.into();
        let counts: Box<[AtomicUsize]> = rules.iter().map(|_| AtomicUsize::new(0)).collect();
        let injected = Arc::clone(&self.injected);
        self.backend.inject(move |call| {
            let fault = rules
                .iter()
                .zip(counts.iter())
                .find_map(|(rule, count)| rule.fault_for(call, count));
            if let Some(fault) = fault {
                injected.count(call, fault);
            }
            fault
        });
    }

    /// Waits for the threads of the step, removes the faults, and checks that no thread panicked.
    ///
    /// The faults stay active until the threads of the step stop, so a thread that calls the backend
    /// after the step returns also gets the faults of the step.
    ///
    /// # Errors
    ///
    /// * If a thread of the step does not stop in [`RELEASE_TIMEOUT`].
    /// * If a thread panicked.
    fn settle(&self) -> TestResult<()> {
        let released = wait_until_released(&self.backend, RELEASE_TIMEOUT);
        self.backend.clear();
        released?;
        let panics = count_panics() - self.panics;
        if panics > 0 {
            return Err(format!("{panics} threads panicked.").into());
        }
        Ok(())
    }

    /// Gives the snapshot files that the backend holds.
    ///
    /// # Errors
    ///
    /// * If the backend cannot list the snapshot files.
    fn snapshots(&self) -> TestResult<BTreeSet<SnapshotId>> {
        Ok(self
            .backend
            .list(FileType::Snapshot)?
            .into_iter()
            .map(SnapshotId::from)
            .collect())
    }

    /// Creates the repository with the faults of `rules`.
    ///
    /// If the creation fails, the function creates the repository again without faults.
    ///
    /// # Errors
    ///
    /// * If a thread panics.
    /// * If the creation without faults fails.
    fn create(&mut self, rules: &[FaultRule]) -> TestResult<()> {
        self.inject(rules);
        let result = self.init();
        self.settle()?;
        self.stats.count("init", result.is_ok());
        if result.is_err() {
            self.init()?;
            self.settle()?;
        }
        Ok(())
    }

    /// Runs `step`, and checks its properties.
    ///
    /// # Errors
    ///
    /// * If the step does not have one of its properties.
    fn run(&mut self, step: &Step) -> TestResult<()> {
        match step {
            Step::Backup { tree, faults } => self.backup(tree, faults),
            Step::Restore {
                pick,
                threads,
                dest,
                faults,
            } => self.restore(*pick, *threads, *dest, faults),
            Step::Delete { pick, faults } => self.delete(*pick, faults),
            Step::Prune { faults } => self.prune(faults),
        }
    }

    /// Backs up `tree` with the faults of `rules`.
    ///
    /// # Errors
    ///
    /// * If a thread panics.
    /// * If the backup of a tree with an unreadable entry succeeds.
    /// * If a failed backup leaves a snapshot file, or a successful backup does not leave its snapshot file.
    fn backup(&mut self, tree: &TreeSpec, rules: &[FaultRule]) -> TestResult<()> {
        let source = tempdir()?;
        tree.write(source.path())?;
        let model = tree
            .unreadable
            .is_none()
            .then(|| Model::read(source.path()))
            .transpose()?;
        let before = self.snapshots()?;
        let opts = BackupOptions::default()
            .as_path(PathBuf::from("data"))
            .fail_on_read_error(true);

        self.inject(rules);
        let result = self.open().and_then(|repo| {
            repo.to_indexed_ids()?.backup(
                &opts,
                &PathList::from_iter(Some(source.path().to_path_buf())),
                SnapshotFile::default(),
            )
        });
        self.settle()?;
        make_removable(source.path())?;
        self.stats.count("backup", result.is_ok());

        let after = self.snapshots()?;
        match (result, model) {
            (Ok(snap), Some(model)) => {
                let expected: BTreeSet<SnapshotId> =
                    before.iter().copied().chain(Some(snap.id)).collect();
                if after != expected {
                    return Err(
                        "The snapshot files after the backup are not the files before and the new snapshot."
                            .into(),
                    );
                }
                self.saved.push((snap.id, model));
                Ok(())
            }
            (Ok(_), None) => Err("The backup of a tree with an unreadable entry succeeded.".into()),
            (Err(_), _) if after == before => Ok(()),
            (Err(err), _) => Err(format!(
                "The failed backup changed the snapshot files. The error was:\n{err}"
            )
            .into()),
        }
    }

    /// Restores the snapshot `id` into the directory `dir`.
    ///
    /// # Arguments
    ///
    /// * `id` - The snapshot
    /// * `dir` - The destination directory
    /// * `threads` - The number of reader threads
    /// * `dest` - The fault of the destination. This function applies only [`DestFault::ReadOnlyDir`].
    ///
    /// # Returns
    ///
    /// The result of the restore.
    ///
    /// # Errors
    ///
    /// * If the function cannot make a directory read-only.
    fn restore_into(
        &self,
        id: SnapshotId,
        dir: &Path,
        threads: Option<NonZeroUsize>,
        dest: DestFault,
    ) -> TestResult<RusticResult<()>> {
        let opts = RestoreOptions::default()
            .fail_on_metadata_error(true)
            .numeric_id(true)
            .reader_threads(threads);
        let repo = match self.open().and_then(Repository::to_indexed) {
            Ok(repo) => repo,
            Err(err) => return Ok(Err(err)),
        };
        let node = repo
            .get_snapshots(&[id.to_hex().as_str()])
            .and_then(|snaps| {
                let snap = snaps.into_iter().next().ok_or_else(|| {
                    RusticError::new(ErrorKind::Other, "The snapshot is missing.")
                })?;
                repo.node_from_snapshot_and_path(&snap, "")
            });
        let node = match node {
            Ok(node) => node,
            Err(err) => return Ok(Err(err)),
        };
        let ls_opts = LsOptions::default();
        let prepared = repo.ls(&node, &ls_opts).and_then(|ls| {
            let dest = LocalDestination::new(&dir.to_string_lossy(), true, false)?;
            let plan = repo.prepare_restore(&opts, ls.clone(), &dest, false)?;
            Ok((ls, dest, plan))
        });
        let (ls, dest_dir, plan) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return Ok(Err(err)),
        };
        if let DestFault::ReadOnlyDir(index) = dest {
            let target: Box<Path> = dir.join(READ_ONLY_TARGETS[index]).into_boxed_path();
            if target.is_dir() {
                set_mode(&target, 0o555)?;
            }
        }
        Ok(repo.restore(plan, &opts, ls, &dest_dir))
    }

    /// Restores a saved snapshot with the faults of `rules` and `dest`.
    ///
    /// # Errors
    ///
    /// * If a thread panics.
    /// * If a successful restore does not give the saved tree.
    fn restore(
        &mut self,
        pick: usize,
        threads: Option<NonZeroUsize>,
        dest: DestFault,
        rules: &[FaultRule],
    ) -> TestResult<()> {
        if self.saved.is_empty() {
            self.stats.add("restore", "skipped");
            return Ok(());
        }
        let index = pick % self.saved.len();
        let id = self.saved[index].0;
        let dir = tempdir()?;
        let volume = match dest {
            DestFault::Volume(size) => Some(Tmpfs::mount(dir.path(), size)?),
            DestFault::None | DestFault::ReadOnlyDir(_) => None,
        };

        self.inject(rules);
        let result = self.restore_into(id, dir.path(), threads, dest);
        self.settle()?;
        let result = result?;
        self.stats.count("restore", result.is_ok());

        let checked = match result {
            Ok(()) => self.saved[index].1.check(&dir.path().join("data")),
            Err(_) => Ok(()),
        };
        make_removable(dir.path())?;
        drop(volume);
        checked?;
        if result.is_ok() && (!rules.is_empty() || !matches!(dest, DestFault::None)) {
            self.stats.add("restore", RESTORE_WITH_FAULTS_OK);
        }
        Ok(())
    }

    /// Deletes a saved snapshot with the faults of `rules`.
    ///
    /// # Errors
    ///
    /// * If a thread panics.
    /// * If a successful delete leaves the snapshot file.
    fn delete(&mut self, pick: usize, rules: &[FaultRule]) -> TestResult<()> {
        if self.saved.is_empty() {
            self.stats.add("delete", "skipped");
            return Ok(());
        }
        let index = pick % self.saved.len();
        let id = self.saved[index].0;

        self.inject(rules);
        let result = self.open().and_then(|repo| repo.delete_snapshots(&[id]));
        self.settle()?;
        self.stats.count("delete", result.is_ok());

        let present = self.snapshots()?.contains(&id);
        if result.is_ok() && present {
            return Err("The delete succeeded, but the snapshot file is still there.".into());
        }
        if !present {
            _ = self.saved.remove(index);
        }
        Ok(())
    }

    /// Prunes the repository with the faults of `rules`.
    ///
    /// The prune repacks each pack with unused data, and removes packs that an earlier prune marked.
    ///
    /// # Errors
    ///
    /// * If a thread panics.
    fn prune(&mut self, rules: &[FaultRule]) -> TestResult<()> {
        let opts = PruneOptions::default()
            .keep_delete(Span::new())
            .max_unused(LimitOption::Percentage(0));

        self.inject(rules);
        let result = self.open().and_then(|repo| {
            let plan = repo.prune_plan(&opts)?;
            repo.prune(&opts, plan)
        });
        self.settle()?;
        self.stats.count("prune", result.is_ok());
        Ok(())
    }

    /// Restores each snapshot without faults, and checks that it gives the saved tree.
    ///
    /// A case with a cache uses a new, empty cache for this check.
    ///
    /// # Errors
    ///
    /// * If the backend does not hold exactly the saved snapshots.
    /// * If a restore fails, or does not give the saved tree.
    fn check_repository(&mut self) -> TestResult<()> {
        if self.cache.is_some() {
            self.cache = Some(tempdir()?);
        }
        let saved: BTreeSet<SnapshotId> = self.saved.iter().map(|(id, _)| *id).collect();
        if self.snapshots()? != saved {
            return Err("The backend does not hold exactly the saved snapshots.".into());
        }
        self.saved
            .iter()
            .try_for_each(|(id, model)| -> TestResult<()> {
                let dir = tempdir()?;
                let result = self.restore_into(*id, dir.path(), None, DestFault::None);
                self.settle()?;
                let checked = result?
                    .map_err(|err| format!("The restore without faults failed:\n{err}").into())
                    .and_then(|()| model.check(&dir.path().join("data")));
                make_removable(dir.path())?;
                checked
            })
    }
}

/// What a run of cases covered: the counts of the results, and the numbers of injected faults.
#[derive(Debug, Default)]
struct Coverage {
    /// The counts of the results of all cases.
    stats: Stats,
    /// The number of injected faults for each operation of [`OPS`].
    ops: [usize; OPS.len()],
    /// The number of injected faults for each fault of [`FAULTS`].
    faults: [usize; FAULTS.len()],
}

impl Coverage {
    /// Adds the counts of the case `state` to the counts of the run.
    fn add(mut self, state: State) -> Self {
        state.stats.results.into_iter().for_each(|(key, count)| {
            *self.stats.results.entry(key).or_default() += count;
        });
        let (ops, faults) = state.injected.load();
        self.ops = array::from_fn(|index| self.ops[index] + ops[index]);
        self.faults = array::from_fn(|index| self.faults[index] + faults[index]);
        self
    }

    /// Describes the counts.
    fn describe(&self) -> Box<str> {
        let ops = OPS
            .iter()
            .zip(self.ops)
            .fold(String::new(), |mut text, (op, count)| {
                _ = write!(text, " {op:?} {count}");
                text
            });
        let faults =
            FAULTS
                .iter()
                .zip(self.faults)
                .fold(String::new(), |mut text, (fault, count)| {
                    _ = write!(text, " {fault:?} {count}");
                    text
                });
        format!(
            "{}injected faults by operation:{ops}; injected faults by kind:{faults}",
            self.stats.describe()
        )
        .into_boxed_str()
    }

    /// Checks that the run injected each operation and each fault, and checked a restore with faults.
    ///
    /// # Errors
    ///
    /// * If the run injected no fault into an operation of [`OPS`], or never injected a fault of [`FAULTS`].
    /// * If no restore that had a fault plan or a destination fault succeeded and gave the saved tree.
    fn check(&self) -> TestResult<()> {
        let missing_ops: Box<[BackendOp]> = OPS
            .iter()
            .zip(self.ops)
            .filter(|(_, count)| *count == 0)
            .map(|(op, _)| *op)
            .collect();
        let missing_faults: Box<[Fault]> = FAULTS
            .iter()
            .zip(self.faults)
            .filter(|(_, count)| *count == 0)
            .map(|(fault, _)| *fault)
            .collect();
        let restores = self
            .stats
            .results
            .get(&("restore", RESTORE_WITH_FAULTS_OK))
            .copied()
            .unwrap_or_default();
        if !missing_ops.is_empty() || !missing_faults.is_empty() || restores == 0 {
            return Err(format!(
                "The run does not cover all faults. Operations without a fault: {missing_ops:?}. Faults never injected: {missing_faults:?}. Checked restores with faults: {restores}."
            )
            .into());
        }
        Ok(())
    }
}

/// Runs `cases` cases, each with a new repository.
///
/// The first case uses the seed from [`SEED_VARIABLE`], or [`DEFAULT_SEED`]. Each next case uses
/// the next seed.
///
/// # Arguments
///
/// * `cache` - Whether the repositories use a cache
/// * `cases` - The number of cases
/// * `faults` - The kinds of faults apart from backend faults
///
/// # Returns
///
/// What the cases covered.
///
/// # Errors
///
/// * If a case does not have one of the properties. The error names the case and its seed.
fn run(cache: bool, cases: usize, faults: Faults) -> TestResult<Coverage> {
    let first_seed = env::var(SEED_VARIABLE)
        .ok()
        .map(|seed| seed.parse::<u64>())
        .transpose()?
        .unwrap_or(DEFAULT_SEED);
    let coverage =
        (0..cases).try_fold(Coverage::default(), |coverage, index| -> TestResult<_> {
            let seed = first_seed + index as u64;
            let case = Case::generate(&mut Rng::new(seed), faults);
            eprintln!("case {index}, seed {seed}, cache {cache}: {case:?}");
            let mut state = State::new(cache)?;
            state
                .create(&case.init)
                .and_then(|()| case.steps.iter().try_for_each(|step| state.run(step)))
                .and_then(|()| state.check_repository())
                .map_err(|err| format!("Case {index} with the seed {seed} failed: {err}"))?;
            Ok(coverage.add(state))
        })?;
    println!("{cases} cases, cache {cache}: {}", coverage.describe());
    Ok(coverage)
}

/// The faults of the property scenarios without small volumes.
const SOURCE_AND_DIRECTORY_FAULTS: Faults = Faults {
    unreadable: true,
    read_only: true,
    volume: false,
};

/// Runs `cases` cases with backend faults, unreadable source entries and read-only destinations, and checks their coverage.
///
/// # Arguments
///
/// * `cache` - Whether the repositories use a cache
/// * `cases` - The number of cases
///
/// # Errors
///
/// * If this process runs as root.
/// * If a case does not have one of the properties.
/// * If the cases do not cover all faults, as [`Coverage::check`] describes.
pub fn run_cases(cache: bool, cases: usize) -> TestResult<()> {
    require_non_root()?;
    run(cache, cases, SOURCE_AND_DIRECTORY_FAULTS)?.check()
}

/// The property test without a cache, with [`CASES`] cases.
///
/// # Errors
///
/// * If [`run_cases`] fails.
pub fn property_no_cache() -> TestResult<()> {
    run_cases(false, CASES)
}

/// The property test with a cache, with [`CASES`] cases.
///
/// # Errors
///
/// * If [`run_cases`] fails.
pub fn property_cache() -> TestResult<()> {
    run_cases(true, CASES)
}

/// The property test with small tmpfs volumes as restore destinations, without and with a cache.
///
/// The scenario moves this process into new namespaces, so run it only in a process that has one
/// thread. In the new user namespace, the process can read each file and write each directory
/// that it owns. Thus this scenario injects no unreadable source entries and no read-only
/// destinations. The scenario checks the coverage of each run.
///
/// # Errors
///
/// * If the process cannot enter the namespaces.
/// * If a case does not have one of the properties.
/// * If a run does not cover all faults, as [`Coverage::check`] describes.
pub fn property_full_volume() -> TestResult<()> {
    enter_user_mount_namespace()?;
    let faults = Faults {
        unreadable: false,
        read_only: false,
        volume: true,
    };
    [false, true]
        .into_iter()
        .try_for_each(|cache| run(cache, VOLUME_CASES, faults)?.check())
}
