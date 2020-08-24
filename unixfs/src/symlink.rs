//! UnixFS symlink support. UnixFS symlinks are UnixFS messages similar to single block files, but
//! the link name or target path is encoded in the UnixFS::Data field. This means that the target
//! path could be in any encoding, however it is always treated as an utf8 Unix path. Could be that
//! this is wrong.

use crate::pb::{FlatUnixFs, UnixFs, UnixFsType};
use alloc::borrow::Cow;
use quick_protobuf::{MessageWrite, Writer};

/// Appends a dag-pb block for for a symlink to the given target_path. It is expected that the
/// `target_path` is valid relative unix path relative to the place in which this is used but
/// targets validity cannot really be judged.
pub fn serialize_symlink_block(target_path: &str, block_buffer: &mut Vec<u8>) {
    // should this fail or not? protobuf encoding cannot fail here, however we might create a too
    // large block but what's the limit?
    //
    // why not return a (Cid, Vec<u8>) like usually with cidv0? well...

    let node = FlatUnixFs {
        links: Vec::new(),
        data: UnixFs {
            Type: UnixFsType::Symlink,
            Data: Some(Cow::Borrowed(target_path.as_bytes())),
            ..Default::default()
        },
    };

    let mut writer = Writer::new(block_buffer);
    node.write_message(&mut writer).expect("unexpected failure");
}

#[cfg(test)]
mod tests {
    use super::serialize_symlink_block;
    use cid::Cid;
    use core::convert::TryFrom;
    use sha2::{Digest, Sha256};

    #[test]
    fn simple_symlink() {
        let mut buf = Vec::new();

        // this symlink just points to a "b" at the same level, used in `symlinks_in_trees` to
        // create the `foo_directory/a` which links to sibling `b` or the directory
        // `foo_directory/b`.
        serialize_symlink_block("b", &mut buf);

        let mh = multihash::wrap(multihash::Code::Sha2_256, &Sha256::digest(&buf));
        let cid = Cid::new_v0(mh).expect("sha2_256 is the correct multihash for cidv0");

        assert_eq!(
            cid.to_string(),
            "QmfLJN6HLyREnWr7QQNmgmuNziUhcbwUopkHQ8gD3pMfp6"
        );
    }

    #[test]
    fn symlinks_in_trees_rooted() {
        use crate::dir::builder::BufferingTreeBuilder;

        let mut tree = BufferingTreeBuilder::default();

        tree.put_link(
            "foo_directory/b/car",
            Cid::try_from("QmNYVgoDXh3dqC1jjCuYqQ9w4XfiocehPZjEPiQiCVYv33").unwrap(),
            12,
        )
        .unwrap();

        tree.put_link(
            "foo_directory/a",
            Cid::try_from("QmfLJN6HLyREnWr7QQNmgmuNziUhcbwUopkHQ8gD3pMfp6").unwrap(),
            7,
        )
        .unwrap();

        let otn = tree.build().last().unwrap().unwrap();

        assert_eq!(
            otn.cid.to_string(),
            "QmZDVQHwjHwA4SyzEDtJLNxmZeJVK1W8BWFAHV61x2Rs19"
        );
    }

    #[test]
    fn symlinks_in_trees_wrapped() {
        use crate::dir::builder::{BufferingTreeBuilder, TreeOptions};

        // note regarding the root directory; now we can add the paths without the first component
        // `foo_directory` and still get the same result as in `symlinks_in_trees_rooted`.
        let mut opts = TreeOptions::default();
        opts.wrap_with_directory();

        let mut tree = BufferingTreeBuilder::new(opts);

        tree.put_link(
            "b/car",
            Cid::try_from("QmNYVgoDXh3dqC1jjCuYqQ9w4XfiocehPZjEPiQiCVYv33").unwrap(),
            12,
        )
        .unwrap();

        tree.put_link(
            "a",
            Cid::try_from("QmfLJN6HLyREnWr7QQNmgmuNziUhcbwUopkHQ8gD3pMfp6").unwrap(),
            7,
        )
        .unwrap();

        let otn = tree.build().last().unwrap().unwrap();

        assert_eq!(
            otn.cid.to_string(),
            "QmZDVQHwjHwA4SyzEDtJLNxmZeJVK1W8BWFAHV61x2Rs19"
        );
    }

    #[test]
    fn walking_symlink_containing_tree() {
        use crate::walk::{ContinuedWalk, Walker};
        use hex_literal::hex;
        use std::path::PathBuf;

        // while this case or similar should be repeated in the walker tests, the topic of symlinks
        // and how the target path names are handled (esp. on windows) is curious enough to warrant
        // duplicating these three cases here.

        let mut fake = crate::test_support::FakeBlockstore::default();

        // if `simple_symlink` and `symlinks_in_trees_*` passed, they would had created these
        // blocks, which we now take for granted.

        let tree_blocks: &[(&'static str, &'static [u8])] = &[
            ("QmZDVQHwjHwA4SyzEDtJLNxmZeJVK1W8BWFAHV61x2Rs19", &hex!("12290a221220fc7fac69ddb44e39686ecfd1ecc6c52ab653f4227e533ee74a2e238f8b2143d3120161180712290a221220b924ddb19181d159c29eec7c98ec506976a76d40241ccd203b226849ce6e0b72120162183d0a020801")),
            ("QmfLJN6HLyREnWr7QQNmgmuNziUhcbwUopkHQ8gD3pMfp6", &hex!("0a050804120162")),
            ("QmaoNjmCQ9774sR6H4DzgGPafXyuVVTCyBeXLaxueKYRLm", &hex!("122b0a2212200308c49252eb61966f802baf45074e074f3b3b90619766e0589c1445261a1a221203636172180c0a020801")),
            ("QmNYVgoDXh3dqC1jjCuYqQ9w4XfiocehPZjEPiQiCVYv33", &hex!("0a0a080212046361720a1804")),
        ];

        for (expected, bytes) in tree_blocks {
            assert_eq!(*expected, fake.insert_v0(bytes).to_string());
        }

        let mut walker = Walker::new(
            // note: this matches the `symlinks_in_trees` root cid (the last cid produced)
            Cid::try_from(tree_blocks[0].0).unwrap(),
            String::default(),
        );

        #[derive(Debug, PartialEq, Eq)]
        enum Entry {
            Dir(PathBuf),
            Symlink(PathBuf, String),
            File(PathBuf),
        }

        let mut actual = Vec::new();

        while walker.should_continue() {
            let (next, _) = walker.pending_links();
            let next = fake.get_by_cid(next);

            match walker.next(next, &mut None).unwrap() {
                ContinuedWalk::File(_fs, _cid, path, _metadata, _total_size) => {
                    actual.push(Entry::File(path.into()));
                }
                ContinuedWalk::RootDirectory(_cid, path, _metadata)
                | ContinuedWalk::Directory(_cid, path, _metadata) => {
                    actual.push(Entry::Dir(path.into()));
                }
                ContinuedWalk::Bucket(..) => { /* ignore */ }
                ContinuedWalk::Symlink(link_name, _cid, path, _metadata) => {
                    actual.push(Entry::Symlink(
                        path.into(),
                        core::str::from_utf8(link_name).unwrap().to_owned(),
                    ));
                }
            };
        }

        // possibly noteworthy: compare these paths to the ones used when creating; there was
        // non-empty root component `foo_directory`, refer to `symlinks_in_trees_*` variants for
        // more.
        let expected = &[
            Entry::Dir(PathBuf::from("")),
            Entry::Symlink(PathBuf::from("a"), String::from("b")),
            Entry::Dir(PathBuf::from("b")),
            Entry::File({
                let mut p = PathBuf::from("b");
                p.push("car");
                p
            }),
        ];

        assert_eq!(expected, actual.as_slice());
    }
}
