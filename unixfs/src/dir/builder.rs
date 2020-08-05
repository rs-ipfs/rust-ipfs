use cid::Cid;
use std::collections::hash_map::Entry::*;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Write};

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
pub struct BufferingTreeBuilder {
    /// At the root there can be only one element, unless an option was given to create a new
    /// directory surrounding the root elements.
    root_builder: DirBuilder,
    longest_path: usize,
    // used to generate each node an unique id which is used when doing the post order traversal to
    // recover all childrens rendered Cids
    counter: u64,
    opts: TreeOptions,
}

impl Default for BufferingTreeBuilder {
    fn default() -> Self {
        Self::new(TreeOptions::default())
    }
}

impl BufferingTreeBuilder {
    pub fn new(opts: TreeOptions) -> Self {
        BufferingTreeBuilder {
            root_builder: DirBuilder::root(0),
            longest_path: 0,
            counter: 1,
            opts,
        }
    }

    // metadata has no bearing here
    pub fn put_file(
        &mut self,
        full_path: &str,
        target: Cid,
        total_size: u64,
    ) -> Result<(), TreeBuildingFailed> {
        // inserted at the depth
        let leaf = Leaf {
            link: target,
            total_size,
        };

        self.modify_with(full_path, |parent, basename, _| {
            parent
                .put_leaf(basename, leaf)
                .map_err(|_| TreeBuildingFailed::DuplicatePath(full_path.to_string()))
        })
    }

    /// Directories get "put" implicitly through the put files, and directories need to be adjusted
    /// only when wanting them to have metadata.
    pub fn set_metadata(
        &mut self,
        full_path: &str,
        metadata: Metadata,
    ) -> Result<(), TreeBuildingFailed> {
        // create all paths along the way
        //
        // set if not set, error otherwise? FIXME: doesn't error atm
        self.modify_with(full_path, |parent, basename, id| {
            parent
                .add_or_get_node(basename, id)
                .map_err(|_| TreeBuildingFailed::LeafAsDirectory(full_path.to_string()))?
                .set_metadata(metadata);
            Ok(())
        })
    }

    fn modify_with<F>(&mut self, full_path: &str, f: F) -> Result<(), TreeBuildingFailed>
    where
        F: FnOnce(&mut DirBuilder, String, &mut Option<u64>) -> Result<(), TreeBuildingFailed>,
    {
        // create all paths along the way
        //
        // assuming it's ok to split '/' since that cannot be escaped in linux at least

        self.longest_path = full_path.len().max(self.longest_path);
        let mut remaining = full_path.split('/').enumerate().peekable();
        let mut dir_builder = &mut self.root_builder;

        // needed to avoid borrowing into the DirBuilder::new calling closure
        let counter = &mut self.counter;

        while let Some((depth, next)) = remaining.next() {
            let last = remaining.peek().is_none();

            match (depth, next, last) {
                // this might need to be accepted in case there is just a single file
                (0, "", true) => { /* accepted */ }
                (0, "", false) => {
                    return Err(TreeBuildingFailed::RootedPath(full_path.to_string()))
                }
                (_, "", false) => {
                    return Err(TreeBuildingFailed::RepeatSlashesInPath(
                        full_path.to_string(),
                    ))
                }
                (_, "", true) => todo!("path ends in slash"),
                _ => {}
            }

            // our first level can be full given the options
            let full = depth == 0 && !self.opts.wrap_in_directory && dir_builder.is_empty();

            if last {
                let mut next_id = Some(*counter);

                let ret = if full {
                    Err(TreeBuildingFailed::TooManyRootLevelEntries)
                } else {
                    f(dir_builder, next.to_string(), &mut next_id)
                };

                if next_id.is_none() {
                    *counter += 1;
                }

                if ret.is_err() {
                    // FIXME: there might be a case where we have now stale nodes in our tree but
                    // cannot figure out an example for that.
                }

                return ret;
            }

            let parent_id = dir_builder.id;

            dir_builder = match (full, dir_builder.nodes.entry(next.to_string())) {
                (_, Occupied(oe)) => oe
                    .into_mut()
                    .as_dir_builder()
                    .map_err(|_| TreeBuildingFailed::LeafAsDirectory(full_path.to_string()))?,
                (false, Vacant(ve)) => {
                    let next_id = *counter;
                    *counter += 1;
                    ve.insert(Entry::Directory(DirBuilder::new(parent_id, next_id)))
                        .as_dir_builder()
                        .expect("safe: we just inserted a DirBuilder")
                }
                (true, Vacant(_)) => return Err(TreeBuildingFailed::TooManyRootLevelEntries),
            };
        }

        // as the str::split will always return a single element this should not ever be hit
        unreachable!(
            "walked the full_path but failed to add anything: {:?}",
            full_path
        );
    }

    /// Called to build the tree. The built tree will have the added files and their implied
    /// directory structure, along with the any directory entries which were created using
    /// `set_metadata`. To build the whole hierarchy, one must iterate the returned iterator to
    /// completion while storing the created blocks.
    ///
    /// Returned `PostOrderIterator` will use the given `full_path` and `block_buffer` to store
    /// it's data during the walk. `PostOrderIterator` implements `Iterator` while also allowing
    /// borrowed access via `next_borrowed`.
    fn build<'a>(
        self,
        full_path: &'a mut String,
        block_buffer: &'a mut Vec<u8>,
    ) -> PostOrderIterator<'a> {
        full_path.clear();
        block_buffer.clear();

        PostOrderIterator {
            full_path,
            old_depth: 0,
            block_buffer,
            pending: vec![Visited::Descent {
                node: self.root_builder,
                name: None,
                depth: 0,
            }],
            persisted_cids: Default::default(),
            reused_children: Vec::new(),
            cid: None,
            wrap_in_directory: self.opts.wrap_in_directory,
        }
    }
}

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

pub struct PostOrderIterator<'a> {
    full_path: &'a mut String,
    old_depth: usize,
    block_buffer: &'a mut Vec<u8>,
    // our stack of pending work
    pending: Vec<Visited>,
    // "communication channel" from nested entries back to their parents
    persisted_cids: HashMap<Option<u64>, BTreeMap<String, Leaf>>,
    reused_children: Vec<Visited>,
    cid: Option<Cid>,
    // from TreeOptions
    wrap_in_directory: bool,
}

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

impl<'a> PostOrderIterator<'a> {
    fn render_directory(
        links: &mut BTreeMap<String, Leaf>,
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

        // argh
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

    fn next_borrowed<'b>(
        &'b mut self,
    ) -> Option<Result<(&'b str, &'b Cid, &'b [u8]), TreeConstructionFailed>> {
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

                    if !self.wrap_in_directory && parent_id.is_none() {
                        // we aren't supposed to wrap_in_directory, and we are now looking at the
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

                    let leaf = match Self::render_directory(&mut collected, buffer) {
                        Ok(leaf) => leaf,
                        Err(e) => return Some(Err(e)),
                    };

                    self.cid = Some(leaf.link.clone());

                    // this reuse strategy is probably good enough
                    collected.clear();

                    if let Some(name) = name {
                        // name is none only for the wrap_in_directory, which cannot really be
                        // propagated up but still the parent_id is allowed to be None
                        let previous = self
                            .persisted_cids
                            .entry(parent_id)
                            .or_insert(collected)
                            .insert(name, leaf);

                        assert!(previous.is_none());
                    }

                    if parent_id.is_none() {
                        // rewrite the full_path for the wrap_in_directory
                        assert!(
                            self.full_path.is_empty(),
                            "full_path should had been empty but it was not: {:?}",
                            self.full_path
                        );
                        // at the wrap_in_directory level the name should be the root level Cid
                        write!(self.full_path, "{}", self.cid.as_ref().unwrap()).unwrap();
                        self.old_depth += 1;
                    }

                    return Some(Ok((
                        self.full_path.as_str(),
                        self.cid.as_ref().unwrap(),
                        &self.block_buffer,
                    )));
                }
            }
        }
        None
    }
}

impl<'a> Iterator for PostOrderIterator<'a> {
    type Item = Result<(String, Cid, Box<[u8]>), TreeConstructionFailed>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_borrowed().map(|res| {
            res.map(|(full_path, cid, block)| (full_path.to_string(), cid.to_owned(), block.into()))
        })
    }
}

struct DuplicateName;
struct FoundLeaf;

/// Node in a directory tree.
#[derive(Debug)]
struct DirBuilder {
    /// Immediate files, symlinks or directories in this directory
    nodes: HashMap<String, Entry>,
    /// Metadata for this directory
    metadata: Metadata,
    /// Id of the parent; None for the root node
    parent_id: Option<u64>,
    /// Internal id, used for propagating Cids back from children during post order visit.
    id: u64,
}

impl DirBuilder {
    fn new(parent_id: u64, id: u64) -> Self {
        assert_ne!(parent_id, id);
        DirBuilder {
            nodes: HashMap::new(),
            metadata: Default::default(),
            parent_id: Some(parent_id),
            id,
        }
    }

    fn root(id: u64) -> Self {
        DirBuilder {
            nodes: HashMap::new(),
            metadata: Default::default(),
            parent_id: None,
            id,
        }
    }

    fn put_leaf(&mut self, key: String, leaf: Leaf) -> Result<(), DuplicateName> {
        match self.nodes.entry(key) {
            Occupied(_) => Err(DuplicateName),
            Vacant(ve) => {
                ve.insert(Entry::Leaf(leaf));
                Ok(())
            }
        }
    }

    fn add_or_get_node(
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

    fn len(&self) -> usize {
        self.nodes.len()
    }

    fn is_empty(&self) -> bool {
        self.len() != 0
    }

    fn set_metadata(&mut self, metadata: Metadata) {
        self.metadata = metadata;
    }
}

#[derive(Default, Debug)]
pub struct Metadata {
    mtime: Option<(i64, u32)>,
    mode: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::{BufferingTreeBuilder, Metadata, TreeBuildingFailed, TreeOptions};
    use cid::Cid;
    use std::convert::TryFrom;

    #[test]
    fn some_directories() {
        let mut builder = BufferingTreeBuilder::default();

        // foobar\n
        let five_block_foobar =
            Cid::try_from("QmRJHYTNvC3hmd9gJQARxLR1QMEincccBV53bBw524yyq6").unwrap();

        builder
            .put_file("a/b/c/d/e/f/g.txt", five_block_foobar.clone(), 221)
            .unwrap();
        builder
            .put_file("a/b/c/d/e/h.txt", five_block_foobar.clone(), 221)
            .unwrap();
        builder
            .put_file("a/b/c/d/e/i.txt", five_block_foobar.clone(), 221)
            .unwrap();

        let mut full_path = String::new();
        let mut buffer = Vec::new();

        let iter = builder.build(&mut full_path, &mut buffer);
        let mut actual = iter
            .map(|res| res.map(|(p, cid, buf)| (p, cid, buf)))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let mut expected = vec![
            (
                "a/b/c/d/e/f",
                "Qmbgf44ztW9wLcGNRNYGinGQB6SQDQtbHVbkM5MrWms698",
            ),
            (
                "a/b/c/d/e",
                "Qma1hCr3CuPRAq2Gw4DCNMqsi42Bjs4Bt1MGSS57kNh144",
            ),
            ("a/b/c/d", "QmUqaYatcJqiSFdykHXGh4Nog1eMSfDJBeYzcG67KV5Ri4"),
            ("a/b/c", "QmYwaNBaGpDCNN9XpHmjxVPHmEXZMw9KDY3uikE2UU5fVB"),
            ("a/b", "QmeAzCPig4o4gBLh2LvP96Sr8MUBrsu2Scw9MTq1EvTDhY"),
            ("a", "QmSTUFaPwJW8xD4KNRLLQRqVTYtYC29xuhYTJoYPWdzvKp"),
        ];

        // hopefully this way the errors will be easier to hunt down

        actual.reverse();
        expected.reverse();

        while let Some(actual) = actual.pop() {
            let expected = expected.pop().expect("size mismatch");
            assert_eq!(actual.0, expected.0);
            assert_eq!(
                actual.1.to_string(),
                expected.1,
                "{:?}: {:?}",
                actual.0,
                Hex(&actual.2)
            );
        }
    }

    struct Hex<'a>(&'a [u8]);
    use std::fmt;

    impl<'a> fmt::Debug for Hex<'a> {
        fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
            for b in self.0 {
                write!(fmt, "{:02x}", b)?;
            }
            Ok(())
        }
    }

    #[test]
    fn empty_path() {
        let mut builder = BufferingTreeBuilder::default();
        builder.put_file("", some_cid(0), 1).unwrap();

        let mut full_path = String::new();
        let mut buffer = Vec::new();

        let iter = builder.build(&mut full_path, &mut buffer);
        let actual = iter
            .map(|res| res.map(|(p, _, _)| p))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert!(
            actual.is_empty(),
            "wrapping in directory was not asked, single element"
        );
    }

    #[test]
    #[should_panic]
    fn rooted_path() {
        let mut builder = BufferingTreeBuilder::default();
        builder.put_file("/a", some_cid(0), 1).unwrap();
    }

    #[test]
    #[should_panic]
    fn successive_slashes() {
        let mut builder = BufferingTreeBuilder::default();
        builder.put_file("a//b", some_cid(0), 1).unwrap();
    }

    #[test]
    fn multiple_roots() {
        // foobar\n
        let five_block_foobar =
            Cid::try_from("QmRJHYTNvC3hmd9gJQARxLR1QMEincccBV53bBw524yyq6").unwrap();

        let opts = TreeOptions::default().with_wrap_in_directory();
        let mut builder = BufferingTreeBuilder::new(opts);
        builder
            .put_file("a", five_block_foobar.clone(), 221)
            .unwrap();
        builder.put_file("b", five_block_foobar, 221).unwrap();

        let mut full_path = String::new();
        let mut buffer = Vec::new();

        let iter = builder.build(&mut full_path, &mut buffer);
        let actual = iter
            .map(|res| res.map(|(p, _, _)| p))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(actual, &["QmdbWuhpVCX9weVMMqvVTMeGwKMqCNJDbx7ZK1zG36sea7"]);
    }

    #[test]
    #[should_panic]
    fn denied_multiple_root_dirs() {
        let mut builder = BufferingTreeBuilder::default();
        builder.put_file("a/c.txt", some_cid(0), 1).unwrap();
        builder.put_file("b/d.txt", some_cid(1), 1).unwrap();
    }

    #[test]
    #[should_panic]
    fn denied_multiple_root_files() {
        let mut builder = BufferingTreeBuilder::default();
        builder.put_file("a.txt", some_cid(0), 1).unwrap();
        builder.put_file("b.txt", some_cid(1), 1).unwrap();
    }

    #[test]
    #[should_panic]
    fn using_leaf_as_node() {
        let mut builder = BufferingTreeBuilder::default();
        builder.put_file("a.txt", some_cid(0), 1).unwrap();
        builder.put_file("a.txt/b.txt", some_cid(1), 1).unwrap();
    }

    #[test]
    fn set_metadata_before_files() {
        let mut builder = BufferingTreeBuilder::default();
        builder
            .set_metadata("a/b/c/d", Metadata::default())
            .unwrap();
        builder.put_file("a/b/c/d/e.txt", some_cid(1), 1).unwrap();
        builder.put_file("a/b/c/d/f.txt", some_cid(2), 1).unwrap();

        let mut full_path = String::new();
        let mut buffer = Vec::new();

        let iter = builder.build(&mut full_path, &mut buffer);
        let actual = iter
            .map(|res| res.map(|(p, _, _)| p))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(actual, &["a/b/c/d", "a/b/c", "a/b", "a",])
    }

    #[test]
    fn set_metadata_on_file() {
        let mut builder = BufferingTreeBuilder::default();
        builder.put_file("a/a.txt", some_cid(0), 1).unwrap();
        let err = builder
            .set_metadata("a/a.txt", Metadata::default())
            .unwrap_err();

        assert!(
            matches!(err, TreeBuildingFailed::LeafAsDirectory(_)),
            "{:?}",
            err
        );
    }

    /// Returns a quick and dirty sha2-256 of the given number as a Cidv0
    fn some_cid(number: usize) -> Cid {
        use multihash::Sha2_256;
        let mh = Sha2_256::digest(&number.to_le_bytes());
        Cid::new_v0(mh).unwrap()
    }
}
