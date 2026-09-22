//! A small tmpfs, to test a restore destination that has no free space.
//!
//! The process mounts the tmpfs in its own user namespace and mount namespace.
//! Thus the process needs no privileges, and other processes do not see the mount.

use std::{fs, io, path::Path};

use nix::{
    mount::{MntFlags, MsFlags, mount, umount2},
    sched::{CloneFlags, unshare},
    unistd::{Gid, Uid, getgid, getuid},
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
    let ids = (getuid(), getgid());
    unshare(CloneFlags::CLONE_NEWUSER | CloneFlags::CLONE_NEWNS)?;
    write_id_maps(ids)?;
    mount(
        None::<&str>,
        "/",
        None::<&str>,
        MsFlags::MS_REC | MsFlags::MS_PRIVATE,
        None::<&str>,
    )?;
    Ok(())
}

/// Moves this process into a new user namespace, and thus removes its privileges over the mounts of the
/// current user namespace.
///
/// The process keeps its user ID and the access that the user ID gives, for example the permission to write a
/// file that it owns, to change the permissions of the file and to set an extended attribute of the namespace
/// `user.`. It loses the capabilities over a filesystem that the current user namespace owns, for example a
/// tmpfs of [`Tmpfs::mount`]. So it can no longer set an extended attribute that the kernel owns on a file of
/// such a filesystem, and the kernel refuses the call with `EPERM`, as it does for a process of a container
/// that runs without privileges. It can also no longer unmount that tmpfs, so the drop of [`Tmpfs`] leaves
/// the mount, and the mount stops only with the mount namespace of the process.
///
/// Call this function after the mount, and before the process starts a second thread.
/// The kernel refuses a new user namespace to a process that has more than one thread.
///
/// # Errors
///
/// * If the kernel refuses the new namespace.
/// * If the function cannot write the user ID map or the group ID map.
pub fn enter_nested_user_namespace() -> io::Result<()> {
    let ids = (getuid(), getgid());
    unshare(CloneFlags::CLONE_NEWUSER)?;
    write_id_maps(ids)
}

/// Maps the IDs `ids` of this process to the same IDs in its new user namespace.
///
/// A process in a new user namespace that has no map has the overflow IDs, and the kernel lets it map only
/// the IDs that it had before the namespace. So the caller reads the IDs before it makes the namespace, and
/// gives them to this function.
///
/// # Arguments
///
/// * `ids` - The user ID and the group ID that this process had before the new user namespace
///
/// # Errors
///
/// * If the function cannot write the user ID map or the group ID map.
fn write_id_maps((uid, gid): (Uid, Gid)) -> io::Result<()> {
    fs::write("/proc/self/uid_map", format!("{uid} {uid} 1"))?;
    fs::write("/proc/self/setgroups", "deny")?;
    fs::write("/proc/self/gid_map", format!("{gid} {gid} 1"))?;
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
