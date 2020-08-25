use super::{Entry, Leaf};
use crate::Metadata;
use alloc::collections::btree_map::Entry::*;
use alloc::collections::BTreeMap;

pub(super) struct DuplicateName;
pub(super) struct FoundLeaf;

/// Node in a directory tree.
#[derive(Debug)]
pub(super) struct DirBuilder {
    /// Immediate files, symlinks or directories in this directory
    pub nodes: BTreeMap<String, Entry>,
    /// Metadata for this directory
    metadata: Metadata,
    /// Id of the parent; None for the root node
    pub parent_id: Option<u64>,
    /// Internal id, used for propagating Cids back from children during post order visit.
    pub id: u64,
}

impl DirBuilder {
    pub fn new(parent_id: u64, id: u64) -> Self {
        assert_ne!(parent_id, id);
        DirBuilder {
            nodes: Default::default(),
            metadata: Default::default(),
            parent_id: Some(parent_id),
            id,
        }
    }

    pub fn root(id: u64) -> Self {
        DirBuilder {
            nodes: Default::default(),
            metadata: Default::default(),
            parent_id: None,
            id,
        }
    }

    pub fn put_leaf(&mut self, key: String, leaf: Leaf) -> Result<(), DuplicateName> {
        match self.nodes.entry(key) {
            Occupied(_) => Err(DuplicateName),
            Vacant(ve) => {
                ve.insert(Entry::Leaf(leaf));
                Ok(())
            }
        }
    }

    pub fn add_or_get_node(
        &mut self,
        key: String,
        id: &mut Option<u64>,
    ) -> Result<&mut DirBuilder, FoundLeaf> {
        match self.nodes.entry(key) {
            Occupied(oe) => oe.into_mut().as_dir_builder().map_err(|_| FoundLeaf),
            Vacant(ve) => {
                let id = id.take().unwrap();
                let entry = ve.insert(Entry::Directory(Self::new(self.id, id)));
                Ok(entry.as_dir_builder().expect("just inserted"))
            }
        }
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = metadata;
    }
}
