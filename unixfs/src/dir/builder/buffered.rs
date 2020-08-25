use super::{DirBuilder, Entry, Leaf, PostOrderIterator, TreeBuildingFailed, TreeOptions};
use crate::Metadata;
use alloc::collections::btree_map::Entry::*;
use cid::Cid;

/// UnixFs directory tree builder which buffers entries until `build()` is called.
#[derive(Debug)]
pub struct BufferingTreeBuilder {
    /// At the root there can be only one element, unless an option was given to create a new
    /// directory surrounding the root elements.
    root_builder: DirBuilder,
    longest_path: usize,
    // used to generate a unique id for each node; it is used when doing the post order traversal to
    // recover all children's rendered Cids
    counter: u64,
    opts: TreeOptions,
}

impl Default for BufferingTreeBuilder {
    fn default() -> Self {
        Self::new(TreeOptions::default())
    }
}

impl BufferingTreeBuilder {
    /// Construct a new tree builder with the given configuration.
    pub fn new(opts: TreeOptions) -> Self {
        BufferingTreeBuilder {
            root_builder: DirBuilder::root(0),
            longest_path: 0,
            counter: 1,
            opts,
        }
    }

    /// Registers the given path to be a link to the cid that follows. The target leaf should be
    /// either a file, directory or symlink but could of course be anything. It will be treated as
    /// an opaque link.
    pub fn put_link(
        &mut self,
        full_path: &str,
        target: Cid,
        total_size: u64,
    ) -> Result<(), TreeBuildingFailed> {
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
        // assuming it's ok to split at '/' since that cannot be escaped in linux at least

        self.longest_path = full_path.len().max(self.longest_path);
        let mut remaining = full_path.split('/').enumerate().peekable();
        let mut dir_builder = &mut self.root_builder;

        // check these before to avoid creation of bogus nodes in the tree or having to clean up.

        if full_path.ends_with('/') {
            return Err(TreeBuildingFailed::PathEndsInSlash(full_path.to_string()));
        }

        if full_path.contains("//") {
            return Err(TreeBuildingFailed::RepeatSlashesInPath(
                full_path.to_string(),
            ));
        }

        // needed to avoid borrowing into the DirBuilder::new calling closure
        let counter = &mut self.counter;

        while let Some((depth, next)) = remaining.next() {
            let last = remaining.peek().is_none();

            match (depth, next, last) {
                // this might need to be accepted in case there is just a single file
                (0, "", true) => {
                    // accepted: allows unconditional tree building in ipfs-http
                    // but the resulting tree will have at most single node, which doesn't prompt
                    // creation of new directories and should be fine.
                }
                (0, "", false) => {
                    // ok to keep this inside the loop; we are yet to create any nodes.
                    // note the ipfs-http (and for example js-ipfs) normalizes the path by
                    // removing the slash from the start.
                    return Err(TreeBuildingFailed::RootedPath(full_path.to_string()));
                }
                (_, "", false) => unreachable!("already validated: no repeat slashes"),
                (_, "", true) => unreachable!("already validated: path does not end in slash"),
                _ => {}
            }

            // our first level can be full, depending on the options given
            let full = depth == 0 && !self.opts.wrap_with_directory && !dir_builder.is_empty();

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
    /// directory structure, along with the directory entries which were created using
    /// `set_metadata`. To build the whole hierarchy, one must iterate the returned iterator to
    /// completion while storing the created blocks.
    ///
    /// Returned `PostOrderIterator` will use the given `full_path` and `block_buffer` to store
    /// its data during the walk. `PostOrderIterator` implements `Iterator` while also allowing
    /// borrowed access via `next_borrowed`.
    pub fn build(self) -> PostOrderIterator {
        PostOrderIterator::new(self.root_builder, self.opts, self.longest_path)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::OwnedTreeNode, BufferingTreeBuilder, Metadata, TreeBuildingFailed, TreeOptions,
    };
    use cid::Cid;
    use core::convert::TryFrom;

    #[test]
    fn some_directories() {
        let mut builder = BufferingTreeBuilder::default();

        // foobar\n
        let five_block_foobar =
            Cid::try_from("QmRJHYTNvC3hmd9gJQARxLR1QMEincccBV53bBw524yyq6").unwrap();

        builder
            .put_link("a/b/c/d/e/f/g.txt", five_block_foobar.clone(), 221)
            .unwrap();
        builder
            .put_link("a/b/c/d/e/h.txt", five_block_foobar.clone(), 221)
            .unwrap();
        builder
            .put_link("a/b/c/d/e/i.txt", five_block_foobar, 221)
            .unwrap();

        let actual = builder
            .build()
            .map(|res| res.map(|n| (n.path, n.cid, n.block)))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let expected = vec![
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

        verify_results(expected, actual);
    }

    #[test]
    fn empty_path() {
        let mut builder = BufferingTreeBuilder::default();
        builder.put_link("", some_cid(0), 1).unwrap();

        let actual = builder
            .build()
            .map(|res| res.map(|OwnedTreeNode { path, .. }| path))
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
        builder.put_link("/a", some_cid(0), 1).unwrap();
    }

    #[test]
    #[should_panic]
    fn successive_slashes() {
        let mut builder = BufferingTreeBuilder::default();
        builder.put_link("a//b", some_cid(0), 1).unwrap();
    }

    #[test]
    fn multiple_roots() {
        // foobar\n
        let five_block_foobar =
            Cid::try_from("QmRJHYTNvC3hmd9gJQARxLR1QMEincccBV53bBw524yyq6").unwrap();

        let mut opts = TreeOptions::default();
        opts.wrap_with_directory();
        let mut builder = BufferingTreeBuilder::new(opts);
        builder
            .put_link("a", five_block_foobar.clone(), 221)
            .unwrap();
        builder.put_link("b", five_block_foobar, 221).unwrap();

        let actual = builder
            .build()
            .map(|res| res.map(|OwnedTreeNode { path, cid, .. }| (path, cid.to_string())))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(
            actual,
            &[(
                "".to_string(),
                "QmdbWuhpVCX9weVMMqvVTMeGwKMqCNJDbx7ZK1zG36sea7".to_string()
            )]
        );
    }

    #[test]
    fn single_wrapped_root() {
        // foobar\n
        let five_block_foobar =
            Cid::try_from("QmRJHYTNvC3hmd9gJQARxLR1QMEincccBV53bBw524yyq6").unwrap();

        let mut opts = TreeOptions::default();
        opts.wrap_with_directory();
        let mut builder = BufferingTreeBuilder::new(opts);
        builder.put_link("a", five_block_foobar, 221).unwrap();

        let actual = builder
            .build()
            .map(|res| res.map(|OwnedTreeNode { path, cid, .. }| (path, cid.to_string())))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(
            actual,
            &[(
                "".to_string(),
                "QmQBseoi3b2FBrYhjM2E4mCF4Q7C8MgCUbzAbGNfyVwgNk".to_string()
            )]
        );
    }

    #[test]
    #[should_panic]
    fn denied_multiple_root_dirs() {
        let mut builder = BufferingTreeBuilder::default();
        builder.put_link("a/c.txt", some_cid(0), 1).unwrap();
        builder.put_link("b/d.txt", some_cid(1), 1).unwrap();
    }

    #[test]
    #[should_panic]
    fn denied_multiple_root_files() {
        let mut builder = BufferingTreeBuilder::default();
        builder.put_link("a.txt", some_cid(0), 1).unwrap();
        builder.put_link("b.txt", some_cid(1), 1).unwrap();
    }

    #[test]
    #[should_panic]
    fn using_leaf_as_node() {
        let mut builder = BufferingTreeBuilder::default();
        builder.put_link("a.txt", some_cid(0), 1).unwrap();
        builder.put_link("a.txt/b.txt", some_cid(1), 1).unwrap();
    }

    #[test]
    fn set_metadata_before_files() {
        let mut builder = BufferingTreeBuilder::default();
        builder
            .set_metadata("a/b/c/d", Metadata::default())
            .unwrap();
        builder.put_link("a/b/c/d/e.txt", some_cid(1), 1).unwrap();
        builder.put_link("a/b/c/d/f.txt", some_cid(2), 1).unwrap();

        let actual = builder
            .build()
            .map(|res| res.map(|OwnedTreeNode { path, .. }| path))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(actual, &["a/b/c/d", "a/b/c", "a/b", "a",])
    }

    #[test]
    fn set_metadata_on_file() {
        let mut builder = BufferingTreeBuilder::default();
        builder.put_link("a/a.txt", some_cid(0), 1).unwrap();
        let err = builder
            .set_metadata("a/a.txt", Metadata::default())
            .unwrap_err();

        assert!(
            matches!(err, TreeBuildingFailed::LeafAsDirectory(_)),
            "{:?}",
            err
        );
    }

    #[test]
    fn dir_with_cidv1_link() {
        // this is `echo '{ "name": "hello" }` | ./ipfs dag put`
        let target =
            Cid::try_from("bafyreihakpd7te5nbmlhdk5ntvcvhf2hmfgrvcwna2sddq5zz5342mcbli").unwrap();

        let mut builder = BufferingTreeBuilder::default();
        builder.put_link("a/b", target, 12).unwrap();

        let actual = builder
            .build()
            .map(|res| res.map(|n| (n.path, n.cid, n.block)))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let expected = vec![("a", "QmPMDMPG8dbHDC9GuvqWr9pfruLnp4GZCAWrskwCmenVQa")];

        verify_results(expected, actual);
    }

    fn verify_results(
        mut expected: Vec<(
            impl AsRef<str> + core::fmt::Debug,
            impl AsRef<str> + core::fmt::Debug,
        )>,
        mut actual: Vec<(String, Cid, Box<[u8]>)>,
    ) {
        use core::fmt;

        struct Hex<'a>(&'a [u8]);

        impl<'a> fmt::Debug for Hex<'a> {
            fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
                for b in self.0 {
                    write!(fmt, "{:02x}", b)?;
                }
                Ok(())
            }
        }

        // hopefully this way the errors will be easier to hunt down

        actual.reverse();
        expected.reverse();

        while let Some(actual) = actual.pop() {
            let expected = expected.pop().expect("size mismatch");
            assert_eq!(actual.0, expected.0.as_ref());
            assert_eq!(
                actual.1.to_string(),
                expected.1.as_ref(),
                "{:?}: {:?}",
                actual.0,
                Hex(&actual.2)
            );
        }

        assert_eq!(expected.len(), 0, "size mismatch: {:?}", actual);
    }

    /// Returns a quick and dirty sha2-256 of the given number as a Cidv0
    fn some_cid(number: usize) -> Cid {
        use multihash::Sha2_256;
        let mh = Sha2_256::digest(&number.to_le_bytes());
        Cid::new_v0(mh).unwrap()
    }
}
