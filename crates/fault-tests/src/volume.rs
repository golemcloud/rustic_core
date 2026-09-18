//! A small tmpfs, to test a restore destination that has no free space.
//!
//! The process mounts the tmpfs in its own user namespace and mount namespace.
//! Thus the process needs no privileges, and other processes do not see the mount.

use std::{fs, io, path::Path};

use nix::{
    mount::{MntFlags, MsFlags, mount, umount2},
    sched::{CloneFlags, unshare},
    unistd::{getgid, getuid},
};

/// Moves this process into a new user namespace and a new mount namespace.
///
/// After this call, the process can mount a tmpfs without privileges.
/// Call this function before the process starts a second thread.
/// The kernel refuses a new user namespace to a process that has more than one thread.
///
/// # Errors
///
/// * If the kernel refuses the new namespaces.
/// * If the function cannot write the user ID map or the group ID map.
/// * If the function cannot make the mounts private to the new mount namespace.
pub fn enter_user_mount_namespace() -> io::Result<()> {
    let (uid, gid) = (getuid(), getgid());
    unshare(CloneFlags::CLONE_NEWUSER | CloneFlags::CLONE_NEWNS)?;
    fs::write("/proc/self/uid_map", format!("{uid} {uid} 1"))?;
    fs::write("/proc/self/setgroups", "deny")?;
    fs::write("/proc/self/gid_map", format!("{gid} {gid} 1"))?;
    mount(
        None::<&str>,
        "/",
        None::<&str>,
        MsFlags::MS_REC | MsFlags::MS_PRIVATE,
        None::<&str>,
    )?;
    Ok(())
}

/// A tmpfs on a directory.
///
/// The drop of the value unmounts the tmpfs.
#[derive(Debug)]
pub struct Tmpfs {
    /// The directory that holds the tmpfs.
    path: Box<Path>,
}

impl Tmpfs {
    /// Mounts a tmpfs of `size` bytes on the directory `path`.
    ///
    /// A write that needs more space than the tmpfs has fails with [`io::ErrorKind::StorageFull`], as on a full volume.
    /// A change of the length of a file does not use space, as on a full volume.
    ///
    /// # Arguments
    ///
    /// * `path` - The directory for the tmpfs
    /// * `size` - The size of the tmpfs in bytes
    ///
    /// # Errors
    ///
    /// * If the mount fails. Call [`enter_user_mount_namespace`] first, or use a process that has the privileges to mount.
    pub fn mount(path: &Path, size: u64) -> io::Result<Self> {
        mount(
            Some("tmpfs"),
            path,
            Some("tmpfs"),
            MsFlags::empty(),
            Some(format!("size={size}").as_str()),
        )?;
        Ok(Self { path: path.into() })
    }

    /// Gives the directory that holds the tmpfs.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for Tmpfs {
    fn drop(&mut self) {
        // The kernel also removes the mount when the mount namespace stops, so an error has no effect.
        _ = umount2(self.path.as_ref(), MntFlags::MNT_DETACH);
    }
}
