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
pub enum WalkSuccess {
    /// IpfsPath was already empty, or became empty during previous walk
    EmptyPath(Ipld),
    /// IpfsPath arrived at destination, following walk attempts will return EmptyPath
    AtDestination(Ipld),
    /// Path segment lead to a link which needs to be loaded to continue the walk
    Link(String, Cid),
}

// FIXME: this probably needs to result in http 40x error? Currently converted to a stringerror
// which is 500.
#[derive(Debug)]
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
