//! Ipfs path handling following https://github.com/ipfs/go-path/. This rests under ipfs-http for
//! now until it's ready to be moved to `rust-ipfs`. There is a competing implementation under
//! `libipld` which might do almost the same things, but with different dependencies. This should
//! be moving over to `ipfs` once we have seen that this works for `api/v0/dag/get` as well.
//!
//! Does not allow the root to be anything else than `/ipfs/` or missing at the moment.

use libipld::cid::{self, Cid};
use libipld::Ipld;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt;

#[derive(Debug)]
pub enum PathError {
    InvalidCid(cid::Error),
    InvalidPath,
}

impl fmt::Display for PathError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PathError::InvalidCid(e) => write!(fmt, "{}", e),
            PathError::InvalidPath => write!(fmt, "invalid path"),
        }
    }
}

impl std::error::Error for PathError {}

/// Ipfs path following https://github.com/ipfs/go-path/
#[derive(Debug)]
pub struct IpfsPath {
    /// Option to support moving the cid
    root: Option<Cid>,
    path: std::vec::IntoIter<String>,
}

impl From<Cid> for IpfsPath {
    /// Creates a new IpfsPath from just the Cid, which is the same as parsing from a string
    /// representation of a Cid but cannot fail.
    fn from(root: Cid) -> IpfsPath {
        IpfsPath {
            root: Some(root),
            path: Vec::new().into_iter(),
        }
    }
}

impl TryFrom<&str> for IpfsPath {
    type Error = PathError;

    fn try_from(path: &str) -> Result<Self, Self::Error> {
        let mut split = path.splitn(2, "/ipfs/");
        let first = split.next();
        let (_root, path) = match first {
            Some("") => {
                /* started with /ipfs/ */
                if let Some(x) = split.next() {
                    // was /ipfs/x
                    ("ipfs", x)
                } else {
                    // just the /ipfs/
                    return Err(PathError::InvalidPath);
                }
            }
            Some(x) => {
                /* maybe didn't start with /ipfs/, need to check second */
                if split.next().is_some() {
                    // x/ipfs/_
                    return Err(PathError::InvalidPath);
                }

                ("", x)
            }
            None => return Err(PathError::InvalidPath),
        };

        let mut split = path.splitn(2, '/');
        let root = split
            .next()
            .expect("first value from splitn(2, _) must exist");

        let path = split
            .next()
            .iter()
            .flat_map(|s| s.split('/').filter(|s| !s.is_empty()).map(String::from))
            .collect::<Vec<_>>()
            .into_iter();

        let root = Some(Cid::try_from(root).map_err(PathError::InvalidCid)?);

        Ok(IpfsPath { root, path })
    }
}

impl IpfsPath {
    pub fn take_root(&mut self) -> Option<Cid> {
        self.root.take()
    }

    /// Walks the path depicted by self until either the path runs out or a new link needs to be
    /// traversed to continue the walk. With !dag-pb documents this can result in subtree of an
    /// Ipld be represented.
    ///
    /// # Panics
    ///
    /// If the current Ipld is from a dag-pb and the libipld has changed it's dag-pb tree structure.
    pub fn walk(&mut self, current: &Cid, mut ipld: Ipld) -> Result<WalkSuccess, WalkFailed> {
        if self.len() == 0 {
            return Ok(WalkSuccess::EmptyPath(ipld));
        }
        for key in self {
            if current.codec() == cid::Codec::DagProtobuf {
                return walk_dagpb(ipld, key);
            }

            ipld = match ipld {
                Ipld::Link(cid) if key == "." => {
                    // go-ipfs: allows this to be skipped. lets require the dot for now.
                    // FIXME: this would require the iterator to be peekable in addition.
                    return Ok(WalkSuccess::Link(key, cid));
                }
                Ipld::Map(mut m) if m.contains_key(&key) => {
                    if let Some(ipld) = m.remove(&key) {
                        ipld
                    } else {
                        return Err(WalkFailed::UnmatchedMapProperty(m, key));
                    }
                }
                Ipld::List(mut l) => {
                    if let Ok(index) = key.parse::<usize>() {
                        if index < l.len() {
                            l.swap_remove(index)
                        } else {
                            return Err(WalkFailed::ListIndexOutOfRange(l, index));
                        }
                    } else {
                        return Err(WalkFailed::UnparseableListIndex(l, key));
                    }
                }
                x => return Err(WalkFailed::UnmatchableSegment(x, key)),
            };

            if let Ipld::Link(next_cid) = ipld {
                return Ok(WalkSuccess::Link(key, next_cid));
            }
        }

        Ok(WalkSuccess::AtDestination(ipld))
    }

    // Currently unused by commited code, but might become handy or easily removed later on.
    #[allow(dead_code)]
    pub fn debug<'a>(&'a self, current: &'a Cid) -> impl fmt::Debug + 'a {
        struct DebuggableIpfsPath<'a> {
            current: &'a Cid,
            segments: &'a [String],
        }

        impl<'a> fmt::Debug for DebuggableIpfsPath<'a> {
            fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
                write!(fmt, "{}", self.current)?;
                if !self.segments.is_empty() {
                    write!(fmt, "/...")?;
                }

                for seg in self.segments {
                    write!(fmt, "/{}", seg)?;
                }
                Ok(())
            }
        }

        DebuggableIpfsPath {
            current,
            segments: self.path.as_slice(),
        }
    }
}

/// "Specialized" walk over dag-pb ipld tree.
///
/// # Panics
///
/// When the dag-pb ipld structure doesn't conform to expectations. This is to on purpose signal
/// that we have gone out of sync with the libipld implementation.
fn walk_dagpb(ipld: Ipld, key: String) -> Result<WalkSuccess, WalkFailed> {
    // the current dag-pb represents the links of dag-pb nodes under "/Links"
    // panicking instead of errors since we want to change this is dag-pb2ipld
    // structure changes.

    let (map, links) = match ipld {
        Ipld::Map(mut m) => {
            let links = m.remove("Links");
            (m, links)
        }
        x => panic!(
            "Expected dag-pb2ipld have top-level \"Links\", was: {:?}",
            x
        ),
    };

    let mut links = match links {
        Some(Ipld::List(vec)) => vec,
        Some(x) => panic!(
            "Expected dag-pb2ipld top-level \"Links\" to be List, was: {:?}",
            x
        ),
        // assume this means that the list was empty, and as such wasn't created
        None => return Err(WalkFailed::UnmatchableSegment(Ipld::Map(map), key)),
    };

    let index = if let Ok(index) = key.parse::<usize>() {
        if index >= links.len() {
            return Err(WalkFailed::ListIndexOutOfRange(links, index));
        }
        index
    } else {
        let index = links
            .iter()
            .enumerate()
            .position(|(i, link)| match link.get("Name") {
                Some(Ipld::String(s)) => s == &key,
                Some(x) => panic!(
                    "Expected dag-pb2ipld \"Links[{}]/Name\" to be an optional String, was: {:?}",
                    i, x
                ),
                None => false,
            });

        match index {
            Some(index) => index,
            None => return Err(WalkFailed::UnmatchableSegment(Ipld::List(links), key)),
        }
    };

    let link = links.swap_remove(index);

    match link {
        Ipld::Map(mut m) => {
            let link = match m.remove("Hash") {
                Some(Ipld::Link(link)) => link,
                Some(x) => panic!(
                    "Expected dag-pb2ipld \"Links[{}]/Hash\" to be a link, was: {:?}",
                    index, x
                ),
                None => panic!("Expected dag-pb2ipld \"Links[{}]/Hash\" to exist", index),
            };

            Ok(WalkSuccess::Link(key, link))
        }
        x => panic!(
            "Expected dag-pb2ipld \"Links[{}]\" to be a Map, was: {:?}",
            index, x
        ),
    }
}

/// The success values walking an `IpfsPath` can result to.
#[derive(Debug, PartialEq)]
pub enum WalkSuccess {
    /// IpfsPath was already empty, or became empty during previous walk
    EmptyPath(Ipld),
    /// IpfsPath arrived at destination, following walk attempts will return EmptyPath
    AtDestination(Ipld),
    /// Path segment lead to a link which needs to be loaded to continue the walk
    Link(String, Cid),
}

/// These errors correspond to ones given out by go-ipfs 0.4.23 if the walk cannot be completed.
/// go-ipfs reports these as 500 Internal Errors.
#[derive(Debug, PartialEq)]
pub enum WalkFailed {
    /// Map key was not found
    UnmatchedMapProperty(BTreeMap<String, Ipld>, String),
    /// Segment could not be parsed as index
    UnparseableListIndex(Vec<Ipld>, String),
    /// Segment was out of range for the list
    ListIndexOutOfRange(Vec<Ipld>, usize),
    /// Catch-all failure for example when walking a segment on integer
    UnmatchableSegment(Ipld, String),
}

impl fmt::Display for WalkFailed {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            // go-ipfs: no such link found
            WalkFailed::UnmatchedMapProperty(_, ref key) => {
                write!(fmt, "No such link found: {:?}", key)
            }
            // go-ipfs: strconv.Atoi: parsing {:?}: invalid syntax
            WalkFailed::UnparseableListIndex(_, ref segment) => {
                write!(fmt, "Invalid list index: {:?}", segment)
            }
            // go-ipfs: array index out of range
            WalkFailed::ListIndexOutOfRange(ref list, index) => write!(
                fmt,
                "List index out of range: the length is {} but the index is {}",
                list.len(),
                index
            ),
            // go-ipfs: tried to resolve through object that had no links
            WalkFailed::UnmatchableSegment(_, _) => {
                write!(fmt, "Tried to resolve through object that had no links")
            }
        }
    }
}

impl std::error::Error for WalkFailed {}

impl Iterator for IpfsPath {
    type Item = String;

    fn next(&mut self) -> Option<String> {
        self.path.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.path.size_hint()
    }
}

impl ExactSizeIterator for IpfsPath {
    fn len(&self) -> usize {
        self.path.len()
    }
}

#[cfg(test)]
mod tests {
    use super::{IpfsPath, WalkSuccess};
    use libipld::{cid::Cid, ipld, Ipld};
    use std::convert::TryFrom;

    // good_paths, good_but_unsupported, bad_paths from https://github.com/ipfs/go-path/blob/master/path_test.go

    #[test]
    fn good_paths() {
        let good = [
            ("/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", 0),
            ("/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a", 1),
            (
                "/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b/c/d/e/f",
                6,
            ),
            (
                "QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b/c/d/e/f",
                6,
            ),
            ("QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", 0),
        ];

        for &(good, len) in &good {
            let p = IpfsPath::try_from(good).unwrap();
            assert_eq!(p.len(), len);
        }
    }

    #[test]
    fn good_but_unsupported() {
        let unsupported = [
            "/ipld/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n",
            "/ipld/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a",
            "/ipld/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b/c/d/e/f",
            "/ipns/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b/c/d/e/f",
            "/ipns/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n",
        ];

        for &unsupported in &unsupported {
            // these fail from failing to parse "ipld" or "ipns" as cid
            IpfsPath::try_from(unsupported).unwrap_err();
        }
    }

    #[test]
    fn bad_paths() {
        let bad = [
            "/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n",
            "/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a",
            "/ipfs/foo",
            "/ipfs/",
            "ipfs/",
            "ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n",
            "/ipld/foo",
            "/ipld/",
            "ipld/",
            "ipld/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n",
        ];

        for &bad in &bad {
            IpfsPath::try_from(bad).unwrap_err();
        }
    }

    #[test]
    fn trailing_slash_is_ignored() {
        let paths = [
            "/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/",
            "QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/",
        ];
        for &path in &paths {
            let p = IpfsPath::try_from(path).unwrap();
            assert_eq!(p.len(), 0);
        }
    }

    #[test]
    fn multiple_slashes_are_deduplicated() {
        // this is similar to behaviour in js-ipfs, as of
        // https://github.com/ipfs-rust/rust-ipfs/pull/147/files#r408939850
        let p =
            IpfsPath::try_from("/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n///a").unwrap();
        assert_eq!(p.len(), 1);
    }

    fn example_doc_and_a_cid() -> (Ipld, Cid) {
        let cid = Cid::try_from("QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n").unwrap();
        let doc = ipld!({
            "nested": {
                "even": [
                    {
                        "more": 5
                    },
                    {
                        "or": "this",
                    },
                    {
                        "or": cid.clone(),
                    },
                    {
                        "5": "or",
                    }
                ],
            }
        });
        (doc, cid)
    }

    #[test]
    fn good_walks_on_ipld() {
        let (example_doc, _) = example_doc_and_a_cid();

        let good_examples = [
            (
                "bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/nested/even/0/more",
                Ipld::Integer(5),
            ),
            (
                "bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/nested/even/1/or",
                Ipld::from("this"),
            ),
            (
                "bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/nested/even/3/5",
                Ipld::from("or"),
            ),
        ];

        for (path, expected) in &good_examples {
            let mut p = IpfsPath::try_from(*path).unwrap();

            // not really the document cid but it doesn't matter; it just needs to be !dag-pb
            let doc_cid = p.take_root().unwrap();

            // projection
            assert_eq!(
                p.walk(&doc_cid, example_doc.clone()),
                Ok(WalkSuccess::AtDestination(expected.clone()))
            );

            // after walk the iterator has been exhausted and the path is always empty and returns
            // the given value
            assert_eq!(
                p.walk(&doc_cid, example_doc.clone()),
                Ok(WalkSuccess::EmptyPath(example_doc.clone()))
            );
        }
    }

    #[test]
    fn walk_link_with_dot() {
        let cid = Cid::try_from("QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n").unwrap();
        let doc = ipld!(cid.clone());
        let path = "bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/./foobar";

        let mut p = IpfsPath::try_from(path).unwrap();
        let doc_cid = p.take_root().unwrap();

        assert_eq!(
            p.walk(&doc_cid, doc),
            Ok(WalkSuccess::Link(".".into(), cid))
        );
    }

    #[test]
    fn walk_link_without_dot_is_unsupported() {
        let cid = Cid::try_from("QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n").unwrap();
        let doc = ipld!(cid);
        let path = "bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/foobar";

        let mut p = IpfsPath::try_from(path).unwrap();
        let doc_cid = p.take_root().unwrap();

        // go-ipfs would walk over the link even without a dot, this will probably come up with
        // dag/get
        p.walk(&doc_cid, doc).unwrap_err();
    }

    #[test]
    fn good_walk_to_link() {
        let (example_doc, cid) = example_doc_and_a_cid();

        let path = "bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/nested/even/2/or/something_on_the_next_block";
        let mut p = IpfsPath::try_from(path).unwrap();
        let doc_cid = p.take_root().unwrap();

        assert_eq!(
            p.walk(&doc_cid, example_doc),
            Ok(WalkSuccess::Link("or".into(), cid))
        );
        assert_eq!(p.next(), Some("something_on_the_next_block".into()));
    }

    #[test]
    fn walk_mismatches() {
        let (example_doc, _) = example_doc_and_a_cid();

        let mismatches = [
            "bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/nested/0/more",
            "bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/nested/even/-1",
            "bafyreielwgy762ox5ndmhx6kpi6go6il3gzahz3ngagb7xw3bj3aazeita/nested/even/1000",
        ];

        for path in &mismatches {
            let mut p = IpfsPath::try_from(*path).unwrap();
            let doc_cid = p.take_root().unwrap();
            // using just unwrap_err as the context would be quite troublesome to write
            p.walk(&doc_cid, example_doc.clone()).unwrap_err();
        }
    }

    #[test]
    fn walk_dagpb_links() {
        // this is one of the links used by conformance tests
        let payload = hex::decode(
            "12330a2212206aad27d7e2fc815cd15bf679535062565dc927a831547281\
            fc0af9e5d7e67c74120b6166726963616e2e747874180812340a221220fd\
            36ac5279964db0cba8f7fa45f8c4c44ef5e2ff55da85936a378c96c9c632\
            04120c616d6572696361732e747874180812360a2212207564c20415869d\
            77a8a40ca68a9158e397dd48bdff1325cdb23c5bcd181acd17120e617573\
            7472616c69616e2e7478741808",
        )
        .unwrap();
        let cid = Cid::try_from("QmbrFTo4s6H23W6wmoZKQC2vSogGeQ4dYiceSqJddzrKVa").unwrap();

        let decoded = libipld::block::decode_ipld(&cid, &payload).unwrap();

        let paths = [
            (
                "QmbrFTo4s6H23W6wmoZKQC2vSogGeQ4dYiceSqJddzrKVa/african.txt",
                "QmVX54jfjB8eRxLVxyQSod6b1FyDh7mR4mQie9j97i2Qk3",
            ),
            (
                "QmbrFTo4s6H23W6wmoZKQC2vSogGeQ4dYiceSqJddzrKVa/americas.txt",
                "QmfP6D9bRV4FEYDL4EHZtZG58kDwDfnzmyjuyK5d1pvzbM",
            ),
            (
                "QmbrFTo4s6H23W6wmoZKQC2vSogGeQ4dYiceSqJddzrKVa/australian.txt",
                "QmWEuXAjUGyndgr4MKqMBgzMW36XgPgvitt2jsXgtuc7JE",
            ),
        ];

        for (path, link) in &paths {
            let mut path = IpfsPath::try_from(*path).unwrap();
            let target = Cid::try_from(*link).unwrap();

            let doc_cid = path.take_root().unwrap();

            // the dag-pb are walked by taking the names from the links, which makes for a bit
            // ackward map reads and type hassle. the current walking code panicks if the structure
            // isn't compatible so hopefully this test will allow us to catch issues with upgrading
            // libipld early.
            //
            // there are still questionable parts, for example like matching links without a name.

            match path.walk(&doc_cid, decoded.clone()).unwrap() {
                WalkSuccess::Link(_, cid) => assert_eq!(cid, target),
                x => panic!("unexpected result from walk: {:?}", x),
            }
        }
    }
}
