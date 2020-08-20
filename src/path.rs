use crate::error::{Error, TryError};
use crate::ipld::Ipld;
use cid::Cid;
use core::convert::{TryFrom, TryInto};
use libp2p::PeerId;
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct IpfsPath {
    root: PathRoot,
    path: SlashedPath,
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
                    //todo!("empty: {:?}, root: {:?}, key: {:?}", empty, root_type, key);
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
    pub fn new(root: PathRoot) -> Self {
        IpfsPath {
            root,
            path: Default::default(),
        }
    }

    pub fn root(&self) -> &PathRoot {
        &self.root
    }

    pub fn set_root(&mut self, root: PathRoot) {
        self.root = root;
    }

    pub fn push_str(&mut self, string: &str) -> Result<(), Error> {
        self.path.push_path(string)?;
        Ok(())
    }

    pub fn sub_path(&self, string: &str) -> Result<Self, Error> {
        let mut path = self.to_owned();
        path.push_str(string)?;
        Ok(path)
    }

    pub fn into_sub_path(mut self, string: &str) -> Result<Self, Error> {
        self.push_str(string)?;
        Ok(self)
    }

    /// Returns an iterator over the path segments following the root
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.path.iter().map(|s| s.as_str())
    }

    pub fn is_path_empty(&self) -> bool {
        self.iter().next().is_none()
    }
}

impl fmt::Display for IpfsPath {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}{}", self.root, self.path)
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

// FIXME: get rid of this; it would mean that there must be a clone to retain the rest of the path.
impl TryInto<Cid> for IpfsPath {
    type Error = Error;

    fn try_into(self) -> Result<Cid, Self::Error> {
        match self.root().cid() {
            Some(cid) => Ok(cid.to_owned()),
            None => Err(anyhow::anyhow!("expected cid")),
        }
    }
}

// FIXME: get rid of this; it would mean that there must be a clone to retain the rest of the path.
impl TryInto<PeerId> for IpfsPath {
    type Error = Error;

    fn try_into(self) -> Result<PeerId, Self::Error> {
        match self.root().peer_id() {
            Some(peer_id) => Ok(peer_id.to_owned()),
            None => Err(anyhow::anyhow!("expected peer id")),
        }
    }
}

/// SlashedPath is internal to IpfsPath variants, and basically holds an unixfs compatible path
/// where segments do not contain slashes but can pretty much contain all other valid UTF-8.
///
/// UTF-8 originates likely from UnixFS related protobuf descriptions, where dag-pb links have
/// UTF-8 names, which equal to SlashedPath segments.
#[doc(hidden)]
#[derive(Debug, PartialEq, Eq, Clone, Default)]
struct SlashedPath {
    path: Vec<String>,
}

impl SlashedPath {
    fn push_path(&mut self, path: &str) -> Result<(), IpfsPathError> {
        if path.is_empty() {
            Ok(())
        } else {
            self.push_split(path.split('/'))
                .map_err(|_| IpfsPathError::InvalidPath(path.to_owned()))
        }
    }

    fn push_split<'a>(&mut self, split: impl Iterator<Item = &'a str>) -> Result<(), ()> {
        let mut split = split.peekable();
        while let Some(sub_path) = split.next() {
            if sub_path == "" {
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

    pub fn iter(&self) -> impl Iterator<Item = &String> {
        self.path.iter()
    }

    pub fn path(&self) -> &[String] {
        &self.path
    }
}

impl fmt::Display for SlashedPath {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.path.iter().try_for_each(|s| write!(fmt, "/{}", s))
    }
}

#[derive(Clone, PartialEq)]
pub enum PathRoot {
    Ipld(Cid),
    Ipns(PeerId),
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
    pub fn is_ipld(&self) -> bool {
        matches!(self, PathRoot::Ipld(_))
    }

    pub fn is_ipns(&self) -> bool {
        matches!(self, PathRoot::Ipns(_))
    }

    pub fn cid(&self) -> Option<&Cid> {
        match self {
            PathRoot::Ipld(cid) => Some(cid),
            _ => None,
        }
    }

    pub fn peer_id(&self) -> Option<&PeerId> {
        match self {
            PathRoot::Ipns(peer_id) => Some(peer_id),
            _ => None,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.into()
    }
}

impl Into<Vec<u8>> for &PathRoot {
    fn into(self) -> Vec<u8> {
        match self {
            PathRoot::Ipld(cid) => cid.to_bytes(),
            PathRoot::Ipns(peer_id) => peer_id.as_bytes().to_vec(),
            PathRoot::Dns(domain) => domain.as_bytes().to_vec(),
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

#[derive(Debug, Error)]
pub enum IpfsPathError {
    #[error("Invalid path {0:?}")]
    InvalidPath(String),
    #[error("Can't resolve {path:?}")]
    ResolveError { ipld: Ipld, path: String },
    #[error("Expected ipld path but found ipns path.")]
    ExpectedIpldPath,
}

#[cfg(test)]
mod tests {
    use super::IpfsPath;
    use std::convert::TryFrom;
    /*use super::*;
    use bitswap::Block;

    #[test]
    fn test_from() {
        let res = Block::from("hello").path("key/3").unwrap();

        let cid = Cid::new_v1(Codec::Raw, b"hello");
        let mut path = IpfsPath::new(PathRoot::Ipld(cid));
        path.push("key");
        path.push(3);

        assert_eq!(path, res);
    }

    #[test]
    fn test_from_errors() {
        let block = Block::from("hello");
        assert!(block.path("").is_ok());
        assert!(block.path("/").is_err());
        assert!(block.path("/abc").is_err());
        assert!(block.path("abc/").is_err());
        assert!(block.path("abc//de").is_err());
    }

    #[test]
    fn test_from_str() {
        let string = "/ipld/QmRN6wdp1S2A5EtjW9A3M1vKSBuQQGcgvuhoMUoEz4iiT5/key/3";
        let res = IpfsPath::from_str(string).unwrap();

        let cid = Block::from("hello").cid().to_owned();
        let mut path = IpfsPath::new(PathRoot::Ipld(cid));
        path.push("key");
        path.push(3);

        assert_eq!(path, res);
    }

    #[test]
    fn test_from_str_errors() {
        assert!(IpfsPath::from_str("").is_err());
        assert!(IpfsPath::from_str("/").is_err());
        assert!(IpfsPath::from_str("/QmRN").is_err());
    }

    #[test]
    fn test_to_string() {
        let path = Block::from("hello").path("key/3").unwrap();
        let res = "/ipfs/QmRN6wdp1S2A5EtjW9A3M1vKSBuQQGcgvuhoMUoEz4iiT5/key/3";
        assert_eq!(path.to_string(), res);
    }*/

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
}
