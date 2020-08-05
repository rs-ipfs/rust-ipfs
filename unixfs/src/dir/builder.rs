use crate::Metadata;
use cid::Cid;
use std::collections::{BTreeMap, HashMap};
use std::fmt;

mod dir_builder;
use dir_builder::{DirBuilder, DuplicateName, FoundLeaf};

mod iter;
pub use iter::PostOrderIterator;

mod buffered;
pub use buffered::BufferingTreeBuilder;

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
#[derive(Default, Debug)]
pub struct TreeOptions {
    wrap_in_directory: bool,
}

impl TreeOptions {
    /// When true, allow multiple top level entries, otherwise error on the second entry
    pub fn with_wrap_in_directory(mut self) -> TreeOptions {
        self.wrap_in_directory = true;
        self
    }
}

#[derive(Debug)]
pub enum TreeBuildingFailed {
    RootedPath(String),
    RepeatSlashesInPath(String),
    TooManyRootLevelEntries,
    DuplicatePath(String),
    LeafAsDirectory(String),
}

impl fmt::Display for TreeBuildingFailed {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TreeBuildingFailed::*;

        match self {
            RootedPath(s) => write!(fmt, "path is rooted: {:?}", s),
            RepeatSlashesInPath(s) => write!(fmt, "path contains repeat slashes: {:?}", s),
            TooManyRootLevelEntries => write!(
                fmt,
                "multiple root level entries while configured wrap_in_directory = false"
            ),
            // TODO: perhaps we should allow adding two leafs with same Cid?
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

#[derive(Debug)]
enum Visited {
    Descent {
        node: DirBuilder,
        name: Option<String>,
        depth: usize,
    },
    Post {
        parent_id: Option<u64>,
        id: u64,
        name: Option<String>,
        depth: usize,
        leaves: Vec<(String, Leaf)>,
    },
}

/// Failure cases for `PostOrderIterator` creating the tree dag-pb nodes.
#[derive(Debug)]
pub enum TreeConstructionFailed {
    // TODO: at least any quick_protobuf errors here?
}

impl fmt::Display for TreeConstructionFailed {
    fn fmt(&self, _fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl std::error::Error for TreeConstructionFailed {}
