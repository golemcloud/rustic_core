use std::{sync::Arc, thread::sleep};

use bytes::Bytes;
use derive_more::Constructor;

use crate::{
    backend::{FileType, decrypt::DecryptReadBackend},
    blob::{BlobId, BlobLocation, BlobType, DataId, tree::TreeId},
    error::{ErrorKind, RusticError, RusticResult},
    index::binarysorted::{Index, IndexCollector, IndexType},
    progress::Progress,
    repofile::{
        indexfile::{IndexBlob, IndexFile},
        packfile::PackId,
    },
};

pub(crate) mod binarysorted;
pub(crate) mod indexer;

pub(super) mod constants {
    use std::time::Duration;

    /// The number of times that the conversion of a global index into an index tries to take the index.
    pub(super) const MAX_INDEX_TRIES: usize = 10;
    /// The time that the conversion of a global index into an index waits between two tries.
    pub(super) const INDEX_TRY_WAIT: Duration = Duration::from_millis(100);
}

/// An entry in the index
#[derive(Debug, Clone, Copy, PartialEq, Eq, Constructor)]
pub struct IndexEntry {
    /// The type of the blob
    blob_type: BlobType,
    /// The pack the blob is in
    pub pack: PackId,
    /// The location of the blob in the pack
    pub location: BlobLocation,
}

impl IndexEntry {
    /// Create an [`IndexEntry`] from an [`IndexBlob`]
    ///
    /// # Arguments
    ///
    /// * `blob` - The [`IndexBlob`] to create the [`IndexEntry`] from
    /// * `pack` - The pack the blob is in
    #[must_use]
    pub const fn from_index_blob(blob: &IndexBlob, pack: PackId) -> Self {
        Self {
            blob_type: blob.tpe,
            pack,
            location: blob.location,
        }
    }

    /// Get a blob described by [`IndexEntry`] from the backend
    ///
    /// # Arguments
    ///
    /// * `be` - The backend to read from
    ///
    /// # Errors
    ///
    // TODO:  add error! This function will return an error if the blob is not found in the backend.
    pub fn read_data<B: DecryptReadBackend>(&self, be: &B) -> RusticResult<Bytes> {
        let data = be.read_encrypted_partial(
            FileType::Pack,
            &self.pack,
            self.blob_type.is_cacheable(),
            self.location,
        )?;

        Ok(data)
    }

    /// Get the length of the data described by the [`IndexEntry`]
    #[must_use]
    pub const fn data_length(&self) -> u32 {
        self.location.data_length()
    }
}

/// The index of the repository
///
/// The index is a list of [`IndexEntry`]s
pub trait ReadIndex {
    /// Get an [`IndexEntry`] from the index
    ///
    /// # Arguments
    ///
    /// * `tpe` - The type of the blob
    /// * `id` - The id of the blob
    ///
    /// # Returns
    ///
    /// The [`IndexEntry`] - If it exists otherwise `None`
    fn get_id(&self, tpe: BlobType, id: &BlobId) -> Option<IndexEntry>;

    /// Get the total size of all blobs of the given type
    ///
    /// # Arguments
    ///
    /// * `tpe` - The type of the blobs
    fn total_size(&self, tpe: BlobType) -> u64;

    /// Check if the index contains the given blob
    ///
    /// # Arguments
    ///
    /// * `tpe` - The type of the blob
    /// * `id` - The id of the blob
    fn has(&self, tpe: BlobType, id: &BlobId) -> bool;

    /// Get a tree from the index
    ///
    /// # Arguments
    ///
    /// * `id` - The id of the tree
    ///
    /// # Returns
    ///
    /// The [`IndexEntry`] of the tree if it exists otherwise `None`
    fn get_tree(&self, id: &TreeId) -> Option<IndexEntry> {
        self.get_id(BlobType::Tree, &BlobId::from(**id))
    }

    /// Get a data blob from the index
    ///
    /// # Arguments
    ///
    /// * `id` - The id of the data blob
    ///
    /// # Returns
    ///
    /// The [`IndexEntry`] of the data blob if it exists otherwise `None`
    fn get_data(&self, id: &DataId) -> Option<IndexEntry> {
        self.get_id(BlobType::Data, &BlobId::from(**id))
    }

    /// Check if the index contains the given tree
    ///
    /// # Arguments
    ///
    /// * `id` - The id of the tree
    ///
    /// # Returns
    ///
    /// `true` if the index contains the tree otherwise `false`
    fn has_tree(&self, id: &TreeId) -> bool {
        self.has(BlobType::Tree, &BlobId::from(**id))
    }

    /// Check if the index contains the given data blob
    ///
    /// # Arguments
    ///
    /// * `id` - The id of the data blob
    ///
    /// # Returns
    ///
    /// `true` if the index contains the data blob otherwise `false`
    fn has_data(&self, id: &DataId) -> bool {
        self.has(BlobType::Data, &BlobId::from(**id))
    }

    /// Get a blob from the backend
    ///
    /// # Arguments
    ///
    /// * `tpe` - The type of the blob
    /// * `id` - The id of the blob
    ///
    /// # Errors
    ///
    /// * If the blob could not be found in the index
    fn blob_from_backend(
        &self,
        be: &impl DecryptReadBackend,
        tpe: BlobType,
        id: &BlobId,
    ) -> RusticResult<Bytes> {
        self.get_id(tpe, id).map_or_else(
            || {
                Err(RusticError::new(
                    ErrorKind::Internal,
                    "Blob `{id}` with type `{type}` not found in index",
                )
                .attach_context("id", id.to_string())
                .attach_context("type", tpe.to_string()))
            },
            |ie| ie.read_data(be),
        )
    }
}

/// A trait for a global index
pub trait ReadGlobalIndex: ReadIndex + Clone + Sync + Send + 'static {}

/// A global index
#[derive(Clone, Debug)]
pub struct GlobalIndex {
    /// The atomic reference counted, sharable index.
    index: Arc<Index>,
}

impl ReadIndex for GlobalIndex {
    /// Get an [`IndexEntry`] from the index
    ///
    /// # Arguments
    ///
    /// * `tpe` - The type of the blob
    /// * `id` - The id of the blob
    ///
    /// # Returns
    ///
    /// The [`IndexEntry`] - If it exists otherwise `None`
    fn get_id(&self, tpe: BlobType, id: &BlobId) -> Option<IndexEntry> {
        self.index.get_id(tpe, id)
    }

    /// Get the total size of all blobs of the given type
    ///
    /// # Arguments
    ///
    /// * `tpe` - The type of the blobs
    fn total_size(&self, tpe: BlobType) -> u64 {
        self.index.total_size(tpe)
    }

    /// Check if the index contains the given blob
    ///
    /// # Arguments
    ///
    /// * `tpe` - The type of the blob
    /// * `id` - The id of the blob
    ///
    /// # Returns
    ///
    /// `true` if the index contains the blob otherwise `false`
    fn has(&self, tpe: BlobType, id: &BlobId) -> bool {
        self.index.has(tpe, id)
    }
}

impl GlobalIndex {
    /// Create a new [`GlobalIndex`] from an [`Index`]
    ///
    /// # Type Parameters
    ///
    /// * `BE` - The backend type
    ///
    /// # Arguments
    ///
    /// * `be` - The backend to read from
    /// * `index` - The index to use
    pub fn new_from_index(index: Index) -> Self {
        Self {
            index: Arc::new(index),
        }
    }

    /// Create a new [`GlobalIndex`] from an [`IndexCollector`]
    ///
    /// # Arguments
    ///
    /// * `be` - The backend to read from
    /// * `p` - The progress tracker
    /// * `collector` - The [`IndexCollector`] to use
    ///
    /// # Errors
    ///
    /// * If the index could not be read
    fn new_from_collector(
        be: &impl DecryptReadBackend,
        p: &Progress,
        mut collector: IndexCollector,
    ) -> RusticResult<Self> {
        p.set_title("reading index...");
        for index in be.stream_all::<IndexFile>(p)? {
            collector.extend(index?.1.packs);
        }

        p.finish();

        Ok(Self::new_from_index(collector.into_index()))
    }

    /// Create a new [`GlobalIndex`]
    ///
    /// # Arguments
    ///
    /// * `be` - The backend to read from
    /// * `p` - The progress tracker
    pub fn new(be: &impl DecryptReadBackend, p: &Progress) -> RusticResult<Self> {
        Self::new_from_collector(be, p, IndexCollector::new(IndexType::Full))
    }

    /// Create a new [`GlobalIndex`] with only full trees
    ///
    /// # Arguments
    ///
    /// * `be` - The backend to read from
    /// * `p` - The progress tracker
    ///
    /// # Errors
    ///
    /// * If the index could not be read
    pub fn only_full_trees(be: &impl DecryptReadBackend, p: &Progress) -> RusticResult<Self> {
        Self::new_from_collector(be, p, IndexCollector::new(IndexType::DataIds))
    }

    /// Convert the `Arc<Index>` to an Index
    ///
    /// Other threads can still hold the index for a short time after they shut down. Thus this
    /// function waits and tries again, up to a fixed number of tries.
    ///
    /// # Errors
    ///
    /// * If another user of the index still holds it after the last try.
    pub fn into_index(self) -> RusticResult<Index> {
        let mut index = self.index;

        for _ in 0..constants::MAX_INDEX_TRIES {
            index = match Arc::try_unwrap(index) {
                Ok(index) => return Ok(index),
                // Seems index is still in use; this could be due to some threads using it which didn't yet completely shut down.
                // sleep a bit to let threads using the index shut down, after this index should be available to unwrap
                Err(index) => index,
            };
            sleep(constants::INDEX_TRY_WAIT);
        }

        Arc::try_unwrap(index).map_err(|_| {
            RusticError::new(
                ErrorKind::Internal,
                "The index is still in use after `{tries}` tries in `{wait}` milliseconds each. Please try again.",
            )
            .attach_context("tries", constants::MAX_INDEX_TRIES.to_string())
            .attach_context(
                "wait",
                constants::INDEX_TRY_WAIT.as_millis().to_string(),
            )
        })
    }

    /// Drop the data pack information from the index
    ///
    /// # Errors
    ///
    /// * If another user of the index still holds it.
    pub(crate) fn drop_data(self) -> RusticResult<Self> {
        Ok(Self {
            index: Arc::new(self.into_index()?.drop_data()),
        })
    }
}

impl ReadGlobalIndex for GlobalIndex {}

#[cfg(test)]
mod tests {
    use super::{GlobalIndex, constants};
    use crate::index::binarysorted::{IndexCollector, IndexType};

    /// Creates a global index that holds no blob.
    fn empty_index() -> GlobalIndex {
        GlobalIndex::new_from_index(IndexCollector::new(IndexType::Full).into_index())
    }

    #[test]
    fn into_index_gives_the_index_of_a_single_user() {
        assert!(empty_index().into_index().is_ok());
    }

    #[test]
    fn into_index_fails_while_another_user_holds_the_index() {
        let index = empty_index();
        let other_user = index.clone();

        let err = index.into_index().unwrap_err();

        assert!(
            err.to_string().contains("still in use"),
            "unexpected error: {err}"
        );
        assert!(
            err.to_string()
                .contains(&constants::MAX_INDEX_TRIES.to_string()),
            "the error does not name the number of tries: {err}"
        );
        drop(other_user);
    }
}
