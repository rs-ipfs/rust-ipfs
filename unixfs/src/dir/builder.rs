use cid::Cid;
use core::fmt;

mod dir_builder;
use dir_builder::DirBuilder;

mod iter;
pub use iter::{OwnedTreeNode, PostOrderIterator, TreeNode};

mod buffered;
pub use buffered::BufferingTreeBuilder;

mod custom_pb;
use custom_pb::CustomFlatUnixFs;

enum Entry {
    Leaf(Leaf),
    Directory(DirBuilder),
}

impl fmt::Debug for Entry {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Entry::*;

        match self {
            Leaf(leaf) => write!(fmt, "Leaf {{ {:?} }}", leaf),
            Directory(_) => write!(fmt, "DirBuilder {{ .. }}"),
        }
    }
}

impl Entry {
    fn as_dir_builder(&mut self) -> Result<&mut DirBuilder, ()> {
        use Entry::*;
        match self {
            Directory(ref mut d) => Ok(d),
            _ => Err(()),
        }
    }
}

struct Leaf {
    link: Cid,
    total_size: u64,
}

impl fmt::Debug for Leaf {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "{}, {}", self.link, self.total_size)
    }
}

/// Configuration for customizing how the tree is built.
#[derive(Debug, Clone)]
pub struct TreeOptions {
    block_size_limit: Option<u64>,
    wrap_with_directory: bool,
}

impl Default for TreeOptions {
    fn default() -> Self {
        TreeOptions {
            // this is just a guess; our bitswap message limit is a bit more
            block_size_limit: Some(512 * 1024),
            wrap_with_directory: false,
        }
    }
}

impl TreeOptions {
    /// Overrides the default directory block size limit. If the size limit is set to `None`, no
    /// directory will be too large.
    pub fn block_size_limit(&mut self, limit: Option<u64>) {
        self.block_size_limit = limit;
    }

    /// When true, allow multiple top level entries, otherwise error on the second entry.
    /// Defaults to false.
    pub fn wrap_with_directory(&mut self) {
        self.wrap_with_directory = true;
    }
}

/// Tree building failure cases.
#[derive(Debug)]
pub enum TreeBuildingFailed {
    /// The given full path started with a slash; paths in the `/add` convention are not rooted.
    RootedPath(String),
    /// The given full path contained an empty segment.
    RepeatSlashesInPath(String),
    /// The given full path ends in slash.
    PathEndsInSlash(String),
    /// If the `BufferingTreeBuilder` was created without `TreeOptions` with the option
    /// `wrap_with_directory` enabled, then there can be only a single element at the root.
    TooManyRootLevelEntries,
    /// The given full path had already been added.
    DuplicatePath(String),
    /// The given full path had already been added as a link to an opaque entry.
    LeafAsDirectory(String),
}

impl fmt::Display for TreeBuildingFailed {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TreeBuildingFailed::*;

        match self {
            RootedPath(s) => write!(fmt, "path is rooted: {:?}", s),
            RepeatSlashesInPath(s) => write!(fmt, "path contains repeat slashes: {:?}", s),
            PathEndsInSlash(s) => write!(fmt, "path ends in a slash: {:?}", s),
            TooManyRootLevelEntries => write!(
                fmt,
                "multiple root level entries while configured wrap_with_directory = false"
            ),
            // TODO: perhaps we should allow adding two leafs with the same Cid?
            DuplicatePath(s) => write!(fmt, "path exists already: {:?}", s),
            LeafAsDirectory(s) => write!(
                fmt,
                "attempted to use already added leaf as a subdirectory: {:?}",
                s
            ),
        }
    }
}

impl std::error::Error for TreeBuildingFailed {}

/// Failure cases for `PostOrderIterator` creating the tree dag-pb nodes.
#[derive(Debug)]
pub enum TreeConstructionFailed {
    /// Failed to serialize the protobuf node for the directory
    Protobuf(quick_protobuf::Error),
    /// The resulting directory would be too large and HAMT sharding is yet to be implemented or
    /// denied.
    TooLargeBlock(u64),
}

impl fmt::Display for TreeConstructionFailed {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TreeConstructionFailed::*;

        match self {
            Protobuf(e) => write!(fmt, "serialization failed: {}", e),
            TooLargeBlock(size) => write!(fmt, "attempted to create block of {} bytes", size),
        }
    }
}

impl std::error::Error for TreeConstructionFailed {}

#[derive(Debug)]
struct NamedLeaf(String, Cid, u64);
