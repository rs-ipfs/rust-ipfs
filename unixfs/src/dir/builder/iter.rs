use super::{Entry, Leaf, TreeConstructionFailed, TreeOptions, Visited};
use cid::Cid;
use std::collections::{BTreeMap, HashMap};

/// Constructs the directory nodes required for a tree.
///
/// Implements the Iterator interface for owned values and the borrowed version `next_borrowed`.
/// Tree is fully constructed once this has been exhausted.
pub struct PostOrderIterator<'a> {
    pub(super) full_path: &'a mut String,
    pub(super) old_depth: usize,
    pub(super) block_buffer: &'a mut Vec<u8>,
    // our stack of pending work
    pub(super) pending: Vec<Visited>,
    // "communication channel" from nested entries back to their parents
    pub(super) persisted_cids: HashMap<Option<u64>, BTreeMap<String, Leaf>>,
    pub(super) reused_children: Vec<Visited>,
    pub(super) cid: Option<Cid>,
    pub(super) total_size: u64,
    // from TreeOptions
    pub(super) wrap_with_directory: bool,
}

impl<'a> PostOrderIterator<'a> {
    pub(super) fn new(
        root: Visited,
        full_path: &'a mut String,
        block_buffer: &'a mut Vec<u8>,
        opts: TreeOptions,
    ) -> Self {
        full_path.clear();
        block_buffer.clear();

        PostOrderIterator {
            full_path,
            old_depth: 0,
            block_buffer,
            pending: vec![root],
            persisted_cids: Default::default(),
            reused_children: Vec::new(),
            cid: None,
            total_size: 0,
            wrap_with_directory: opts.wrap_with_directory,
        }
    }

    fn render_directory(
        links: &BTreeMap<String, Leaf>,
        buffer: &mut Vec<u8>,
    ) -> Result<Leaf, TreeConstructionFailed> {
        use crate::pb::{FlatUnixFs, PBLink, UnixFs, UnixFsType};
        use quick_protobuf::{BytesWriter, MessageWrite, Writer};
        use sha2::{Digest, Sha256};
        use std::borrow::Cow;

        // TODO: this could quite easily be made so that the links are read from the btreemap for
        // calculating the size and rendering
        let mut combined_from_links = 0;

        let flat = FlatUnixFs {
            links: links
                .iter() // .drain() would be the most reasonable
                .inspect(|(_, Leaf { total_size, .. })| combined_from_links += total_size)
                .map(|(name, Leaf { link, total_size })| PBLink {
                    Hash: Some(link.to_bytes().into()),
                    Name: Some(Cow::Borrowed(name.as_str())),
                    Tsize: Some(*total_size),
                })
                .collect::<Vec<_>>(),
            data: UnixFs {
                Type: UnixFsType::Directory,
                Data: None,
                ..Default::default()
            },
        };

        let size = flat.get_size();

        // FIXME: we shouldn't be creating too large structures (bitswap block size limit!)
        // FIXME: changing this to autosharding is going to take some thinking

        buffer.clear();
        let cap = buffer.capacity();

        if let Some(additional) = size.checked_sub(cap) {
            buffer.reserve(additional);
        }

        // TODO: this could be done more integelligently; for example, we could just zero extend
        // on reserving, then just truncate or somehow carry around the real length of the buffer
        // to avoid truncating and zero extending.
        buffer.extend(std::iter::repeat(0).take(size));

        let mut writer = Writer::new(BytesWriter::new(&mut buffer[..]));
        flat.write_message(&mut writer)
            .expect("unsure how this could fail");
        let mh = multihash::wrap(multihash::Code::Sha2_256, &Sha256::digest(&buffer));
        let cid = Cid::new_v0(mh).expect("sha2_256 is the correct multihash for cidv0");

        Ok(Leaf {
            link: cid,
            total_size: buffer.len() as u64 + combined_from_links,
        })
    }

    /// Construct the next dag-pb node, if any.
    ///
    /// Returns a `TreeNode` of the latest constructed tree node.
    pub fn next_borrowed(&mut self) -> Option<Result<TreeNode<'_>, TreeConstructionFailed>> {
        while let Some(visited) = self.pending.pop() {
            let (name, depth) = match &visited {
                Visited::Descent { name, depth, .. } => (name.as_deref(), *depth),
                Visited::Post { name, depth, .. } => (name.as_deref(), *depth),
            };

            update_full_path((self.full_path, &mut self.old_depth), name, depth);

            match visited {
                Visited::Descent { node, name, depth } => {
                    let mut leaves = Vec::new();

                    let children = &mut self.reused_children;

                    for (k, v) in node.nodes {
                        match v {
                            Entry::Directory(node) => children.push(Visited::Descent {
                                node,
                                name: Some(k),
                                depth: depth + 1,
                            }),
                            Entry::Leaf(leaf) => leaves.push((k, leaf)),
                        }
                    }

                    self.pending.push(Visited::Post {
                        parent_id: node.parent_id,
                        id: node.id,
                        name,
                        depth,
                        leaves,
                    });

                    let any_children = !children.is_empty();

                    self.pending.extend(children.drain(..));

                    if any_children {
                        // we could strive to do everything right now but pushing and popping might
                        // turn out easier code wise, or in other words, when there are no child_nodes
                        // we wouldn't need to go through Visited::Post.
                    }
                }
                Visited::Post {
                    parent_id,
                    id,
                    name,
                    leaves,
                    ..
                } => {
                    // all of our children have now been visited; we should be able to find their
                    // Cids in the btreemap
                    let mut collected = self.persisted_cids.remove(&Some(id)).unwrap_or_default();

                    // FIXME: leaves could be drained and reused
                    collected.extend(leaves);

                    if !self.wrap_with_directory && parent_id.is_none() {
                        // we aren't supposed to wrap_with_directory, and we are now looking at the
                        // possibly to be generated root directory.

                        assert_eq!(
                            collected.len(),
                            1,
                            "should not have gone this far with multiple added roots"
                        );

                        return None;
                    }

                    // render unixfs, maybe return it?
                    let buffer = &mut self.block_buffer;
                    buffer.clear();

                    let leaf = match Self::render_directory(&collected, buffer) {
                        Ok(leaf) => leaf,
                        Err(e) => return Some(Err(e)),
                    };

                    self.cid = Some(leaf.link.clone());
                    self.total_size = leaf.total_size;

                    // this reuse strategy is probably good enough
                    collected.clear();

                    if let Some(name) = name {
                        // name is none only for the wrap_with_directory, which cannot really be
                        // propagated up but still the parent_id is allowed to be None
                        let previous = self
                            .persisted_cids
                            .entry(parent_id)
                            .or_insert(collected)
                            .insert(name, leaf);

                        assert!(previous.is_none());
                    }

                    return Some(Ok(TreeNode {
                        path: self.full_path.as_str(),
                        cid: self.cid.as_ref().unwrap(),
                        total_size: self.total_size,
                        block: &self.block_buffer,
                    }));
                }
            }
        }
        None
    }
}

impl<'a> Iterator for PostOrderIterator<'a> {
    type Item = Result<OwnedTreeNode, TreeConstructionFailed>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_borrowed()
            .map(|res| res.map(TreeNode::into_owned))
    }
}

/// Borrowed representation of a node in the tree.
pub struct TreeNode<'a> {
    /// Full path to the node.
    pub path: &'a str,
    /// The Cid of the document.
    pub cid: &'a Cid,
    /// Cumulative total size of the subtree in bytes.
    pub total_size: u64,
    /// Raw dag-pb document.
    pub block: &'a [u8],
}

impl TreeNode<'_> {
    /// Convert to an owned and detached representation.
    pub fn into_owned(self) -> OwnedTreeNode {
        OwnedTreeNode {
            path: self.path.to_owned(),
            cid: self.cid.to_owned(),
            total_size: self.total_size,
            block: self.block.into(),
        }
    }
}

/// Owned representation of a node in the tree.
pub struct OwnedTreeNode {
    /// Full path to the node.
    pub path: String,
    /// The Cid of the document.
    pub cid: Cid,
    /// Cumulative total size of the subtree in bytes.
    pub total_size: u64,
    /// Raw dag-pb document.
    pub block: Box<[u8]>,
}

fn update_full_path(
    (full_path, old_depth): (&mut String, &mut usize),
    name: Option<&str>,
    depth: usize,
) {
    if depth < 2 {
        // initially thought it might be good idea to add slash to all components; removing it made
        // it impossible to get back down to empty string, so fixing this for depths 0 and 1.
        full_path.clear();
        *old_depth = 0;
    } else {
        while *old_depth >= depth && *old_depth > 0 {
            // we now want to pop the last segment
            // this would be easier with pathbuf
            let slash_at = full_path.bytes().rposition(|ch| ch == b'/');
            if let Some(slash_at) = slash_at {
                full_path.truncate(slash_at);
                *old_depth -= 1;
            } else {
                todo!(
                    "no last slash_at in {:?} yet {} >= {}",
                    full_path,
                    old_depth,
                    depth
                );
            }
        }
    }

    debug_assert!(*old_depth <= depth);

    if let Some(name) = name {
        if !full_path.is_empty() {
            full_path.push_str("/");
        }
        full_path.push_str(name);
        *old_depth += 1;
    }

    assert_eq!(*old_depth, depth);
}
