//! [`IpfsPath`] related functionality for content addressed paths with links.

use crate::error::{Error, TryError};
use cid::Cid;
use core::convert::{TryFrom, TryInto};
use libp2p::PeerId;
use std::fmt;
use std::str::FromStr;

/// Abstraction over Ipfs paths, which are used to target sub-trees or sub-documents on top of
/// content addressable ([`Cid`]) trees. The most common use case is to specify a file under an
/// unixfs tree from underneath a [`Cid`] forest.
///
/// In addition to being based on content addressing, IpfsPaths provide adaptation from other Ipfs
/// (related) functionality which can be resolved to a [`Cid`] such as IPNS. IpfsPaths have similar
/// structure to and can start with a "protocol" as [Multiaddr], except the protocols are
/// different, and at the moment there can be at most one protocol.
///
/// This implementation supports:
///
/// - synonymous `/ipfs` and `/ipld` prefixes to point to a [`Cid`]
/// - `/ipns` to point to either:
///    - [`PeerId`] to signify an [IPNS] DHT record
///    - domain name to signify an [DNSLINK] reachable record
///
/// See [`crate::Ipfs::resolve_ipns`] for the current IPNS resolving capabilities.
///
/// `IpfsPath` is usually created through the [`FromStr`] or [`From`] conversions.
///
/// [Multiaddr]: https://github.com/multiformats/multiaddr
/// [IPNS]: https://github.com/ipfs/specs/blob/master/IPNS.md
/// [DNSLINK]: https://dnslink.io/
// TODO: it might be useful to split this into CidPath and IpnsPath, then have Ipns resolve through
// latter into CidPath (recursively) and have dag.rs support only CidPath. Keep IpfsPath as a
// common abstraction which can be either.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct IpfsPath {
    root: PathRoot,
    pub(crate) path: SlashedPath,
}

impl FromStr for IpfsPath {
    type Err = Error;

    fn from_str(string: &str) -> Result<Self, Error> {
        let mut subpath = string.split('/');
        let empty = subpath.next().expect("there's always the first split");

        let root = if !empty.is_empty() {
            // by default if there is no prefix it's an ipfs or ipld path
            PathRoot::Ipld(Cid::try_from(empty)?)
        } else {
            let root_type = subpath.next();
            let key = subpath.next();

            match (empty, root_type, key) {
                ("", Some("ipfs"), Some(key)) => PathRoot::Ipld(Cid::try_from(key)?),
                ("", Some("ipld"), Some(key)) => PathRoot::Ipld(Cid::try_from(key)?),
                ("", Some("ipns"), Some(key)) => match PeerId::from_str(key).ok() {
                    Some(peer_id) => PathRoot::Ipns(peer_id),
                    None => PathRoot::Dns(key.to_string()),
                },
                _ => {
                    return Err(IpfsPathError::InvalidPath(string.to_owned()).into());
                }
            }
        };

        let mut path = IpfsPath::new(root);
        path.path
            .push_split(subpath)
            .map_err(|_| IpfsPathError::InvalidPath(string.to_owned()))?;
        Ok(path)
    }
}

impl IpfsPath {
    /// Creates a new [`IpfsPath`] from a [`PathRoot`].
    pub fn new(root: PathRoot) -> Self {
        IpfsPath {
            root,
            path: Default::default(),
        }
    }

    /// Returns the [`PathRoot`] "protocol" configured for the [`IpfsPath`].
    pub fn root(&self) -> &PathRoot {
        &self.root
    }

    pub(crate) fn push_str(&mut self, string: &str) -> Result<(), Error> {
        self.path.push_path(string)?;
        Ok(())
    }

    /// Returns a new [`IpfsPath`] with the given path segments appended, or an error, if a segment is
    /// invalid.
    pub fn sub_path(&self, segments: &str) -> Result<Self, Error> {
        let mut path = self.to_owned();
        path.push_str(segments)?;
        Ok(path)
    }

    /// Returns an iterator over the path segments following the root.
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.path.iter().map(|s| s.as_str())
    }

    pub(crate) fn into_shifted(self, shifted: usize) -> SlashedPath {
        assert!(shifted <= self.path.len());

        let mut p = self.path;
        p.shift(shifted);
        p
    }

    pub(crate) fn into_truncated(self, len: usize) -> SlashedPath {
        assert!(len <= self.path.len());

        let mut p = self.path;
        p.truncate(len);
        p
    }
}

impl fmt::Display for IpfsPath {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.root)?;
        if !self.path.is_empty() {
            // slash is not included in the <SlashedPath as fmt::Display>::fmt impl as we need to,
            // serialize it later in json *without* one
            write!(fmt, "/{}", self.path)?;
        }
        Ok(())
    }
}

impl TryFrom<&str> for IpfsPath {
    type Error = Error;

    fn try_from(string: &str) -> Result<Self, Self::Error> {
        IpfsPath::from_str(string)
    }
}

impl<T: Into<PathRoot>> From<T> for IpfsPath {
    fn from(root: T) -> Self {
        IpfsPath::new(root.into())
    }
}

/// SlashedPath is internal to IpfsPath variants, and basically holds a unixfs-compatible path
/// where segments do not contain slashes but can pretty much contain all other valid UTF-8.
///
/// UTF-8 originates likely from UnixFS related protobuf descriptions, where dag-pb links have
/// UTF-8 names, which equal to SlashedPath segments.
#[derive(Debug, PartialEq, Eq, Clone, Default, Hash)]
pub struct SlashedPath {
    path: Vec<String>,
}

impl SlashedPath {
    fn push_path(&mut self, path: &str) -> Result<(), IpfsPathError> {
        if path.is_empty() {
            Ok(())
        } else {
            self.push_split(path.split('/'))
                .map_err(|_| IpfsPathError::SegmentContainsSlash(path.to_owned()))
        }
    }

    pub(crate) fn push_split<'a>(
        &mut self,
        split: impl Iterator<Item = &'a str>,
    ) -> Result<(), ()> {
        let mut split = split.peekable();
        while let Some(sub_path) = split.next() {
            if sub_path.is_empty() {
                return if split.peek().is_none() {
                    // trim trailing
                    Ok(())
                } else {
                    // no empty segments in the middle
                    Err(())
                };
            }
            self.path.push(sub_path.to_owned());
        }
        Ok(())
    }

    /// Returns an iterator over the path segments
    pub fn iter(&self) -> impl Iterator<Item = &String> {
        self.path.iter()
    }

    /// Returns the number of segments
    pub fn len(&self) -> usize {
        // intentionally try to hide the fact that this is based on Vec<String> right now
        self.path.len()
    }

    /// Returns true if len is zero
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn shift(&mut self, n: usize) {
        self.path.drain(0..n);
    }

    fn truncate(&mut self, len: usize) {
        self.path.truncate(len);
    }
}

impl fmt::Display for SlashedPath {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        self.path.iter().try_for_each(move |s| {
            if first {
                first = false;
            } else {
                write!(fmt, "/")?;
            }

            write!(fmt, "{}", s)
        })
    }
}

impl<'a> PartialEq<[&'a str]> for SlashedPath {
    fn eq(&self, other: &[&'a str]) -> bool {
        // FIXME: failed at writing a blanket partialeq over anything which would PartialEq<str> or
        // String
        self.path.iter().zip(other.iter()).all(|(a, b)| a == b)
    }
}

/// The "protocol" of [`IpfsPath`].
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum PathRoot {
    /// [`Cid`] based path is the simplest path, and is stable.
    Ipld(Cid),
    /// IPNS record based path which can point to different [`Cid`] based paths at different times.
    Ipns(PeerId),
    /// DNSLINK based path which can point to different [`Cid`] based paths at different times.
    Dns(String),
}

impl fmt::Debug for PathRoot {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use PathRoot::*;

        match self {
            Ipld(cid) => write!(fmt, "{}", cid),
            Ipns(pid) => write!(fmt, "{}", pid),
            Dns(name) => write!(fmt, "{:?}", name),
        }
    }
}

impl PathRoot {
    /// Returns the `Some(Cid)` if the [`Cid`] based path is present or `None`.
    pub fn cid(&self) -> Option<&Cid> {
        match self {
            PathRoot::Ipld(cid) => Some(cid),
            _ => None,
        }
    }
}

impl fmt::Display for PathRoot {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let (prefix, key) = match self {
            PathRoot::Ipld(cid) => ("/ipfs/", cid.to_string()),
            PathRoot::Ipns(peer_id) => ("/ipns/", peer_id.to_base58()),
            PathRoot::Dns(domain) => ("/ipns/", domain.to_owned()),
        };
        write!(fmt, "{}{}", prefix, key)
    }
}

impl From<Cid> for PathRoot {
    fn from(cid: Cid) -> Self {
        PathRoot::Ipld(cid)
    }
}

impl From<PeerId> for PathRoot {
    fn from(peer_id: PeerId) -> Self {
        PathRoot::Ipns(peer_id)
    }
}

impl TryInto<Cid> for PathRoot {
    type Error = TryError;

    fn try_into(self) -> Result<Cid, Self::Error> {
        match self {
            PathRoot::Ipld(cid) => Ok(cid),
            _ => Err(TryError),
        }
    }
}

impl TryInto<PeerId> for PathRoot {
    type Error = TryError;

    fn try_into(self) -> Result<PeerId, Self::Error> {
        match self {
            PathRoot::Ipns(peer_id) => Ok(peer_id),
            _ => Err(TryError),
        }
    }
}

/// The path mutation or parsing errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum IpfsPathError {
    /// The given path cannot be parsed as IpfsPath.
    #[error("Invalid path {0:?}")]
    InvalidPath(String),

    /// Path segment contains a slash, which is not allowed.
    #[error("Invalid segment {0:?}")]
    SegmentContainsSlash(String),
}

#[cfg(test)]
mod tests {
    use super::IpfsPath;
    use std::convert::TryFrom;

    #[test]
    fn display() {
        let input = [
            (
                "/ipld/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n",
                Some("/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n"),
            ),
            ("/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", None),
            (
                "/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a",
                None,
            ),
            (
                "/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/",
                Some("/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a"),
            ),
            (
                "QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n",
                Some("/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n"),
            ),
            ("/ipns/foobar.com", None),
            ("/ipns/foobar.com/a", None),
            ("/ipns/foobar.com/a/", Some("/ipns/foobar.com/a")),
        ];

        for (input, maybe_actual) in &input {
            assert_eq!(
                IpfsPath::try_from(*input).unwrap().to_string(),
                maybe_actual.unwrap_or(input)
            );
        }
    }

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
            ("/ipld/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n", 0),
            ("/ipld/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a", 1),
            (
                "/ipld/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/a/b/c/d/e/f",
                6,
            ),
            ("/ipns/QmSrPmbaUKA3ZodhzPWZnpFgcPMFWF4QsxXbkWfEptTBJd", 0),
            (
                "/ipns/QmSrPmbaUKA3ZodhzPWZnpFgcPMFWF4QsxXbkWfEptTBJd/a/b/c/d/e/f",
                6,
            ),
        ];

        for &(good, len) in &good {
            let p = IpfsPath::try_from(good).unwrap();
            assert_eq!(p.iter().count(), len);
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
            assert_eq!(p.iter().count(), 0, "{:?} from {:?}", p, path);
        }
    }

    #[test]
    fn multiple_slashes_are_not_deduplicated() {
        // this used to be the behaviour in ipfs-http
        IpfsPath::try_from("/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n///a").unwrap_err();
    }

    #[test]
    fn shifting() {
        let mut p = super::SlashedPath::default();
        p.push_split(vec!["a", "b", "c"].into_iter()).unwrap();
        p.shift(2);

        assert_eq!(p.to_string(), "c");
    }
}
