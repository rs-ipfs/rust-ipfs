use crate::error::{Error, TryError};
use core::convert::{TryFrom, TryInto};
use libipld::cid::Cid;
use libipld::ipld::Ipld;
use libp2p::PeerId;
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct IpfsPath {
    root: PathRoot,
    path: Vec<SubPath>,
}

impl FromStr for IpfsPath {
    type Err = Error;

    fn from_str(string: &str) -> Result<Self, Error> {
        let mut subpath = string.split('/');
        let empty = subpath.next();
        let root_type = subpath.next();
        let key = subpath.next();

        let root = match (empty, root_type, key) {
            (Some(""), Some("ipfs"), Some(key)) => PathRoot::Ipld(Cid::try_from(key)?),
            (Some(""), Some("ipld"), Some(key)) => PathRoot::Ipld(Cid::try_from(key)?),
            (Some(""), Some("ipns"), Some(key)) => match PeerId::from_str(key).ok() {
                Some(peer_id) => PathRoot::Ipns(peer_id),
                None => PathRoot::Dns(key.to_string()),
            },
            _ => return Err(IpfsPathError::InvalidPath(string.to_owned()).into()),
        };
        let mut path = IpfsPath::new(root);
        path.push_str(&subpath.collect::<Vec<&str>>().join("/"))?;
        Ok(path)
    }
}

impl IpfsPath {
    pub fn new(root: PathRoot) -> Self {
        IpfsPath {
            root,
            path: Vec::new(),
        }
    }

    pub fn root(&self) -> &PathRoot {
        &self.root
    }

    pub fn set_root(&mut self, root: PathRoot) {
        self.root = root;
    }

    pub fn push<T: Into<SubPath>>(&mut self, sub_path: T) {
        self.path.push(sub_path.into());
    }

    pub fn push_str(&mut self, string: &str) -> Result<(), Error> {
        if string.is_empty() {
            return Ok(());
        }
        for sub_path in string.split('/') {
            if sub_path == "" {
                return Err(IpfsPathError::InvalidPath(string.to_owned()).into());
            }
            let index = sub_path.parse::<usize>();
            if let Ok(index) = index {
                self.push(index);
            } else {
                self.push(sub_path);
            }
        }
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

    pub fn iter(&self) -> impl Iterator<Item = &SubPath> {
        self.path.iter()
    }
}

impl fmt::Display for IpfsPath {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.root)?;
        for sub_path in &self.path {
            write!(fmt, "/{}", sub_path)?;
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

impl TryInto<Cid> for IpfsPath {
    type Error = Error;

    fn try_into(self) -> Result<Cid, Self::Error> {
        match self.root().cid() {
            Some(cid) => Ok(cid.to_owned()),
            None => Err(anyhow::anyhow!("expected cid")),
        }
    }
}

impl TryInto<PeerId> for IpfsPath {
    type Error = Error;

    fn try_into(self) -> Result<PeerId, Self::Error> {
        match self.root().peer_id() {
            Some(peer_id) => Ok(peer_id.to_owned()),
            None => Err(anyhow::anyhow!("expected peer id")),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum PathRoot {
    Ipld(Cid),
    Ipns(PeerId),
    Dns(String),
}

impl PathRoot {
    pub fn is_ipld(&self) -> bool {
        match self {
            PathRoot::Ipld(_) => true,
            _ => false,
        }
    }

    pub fn is_ipns(&self) -> bool {
        match self {
            PathRoot::Ipns(_) => true,
            _ => false,
        }
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

#[derive(Clone, Debug, PartialEq)]
pub enum SubPath {
    Key(String),
    Index(usize),
}

impl From<String> for SubPath {
    fn from(key: String) -> Self {
        SubPath::Key(key)
    }
}

impl From<&str> for SubPath {
    fn from(key: &str) -> Self {
        SubPath::from(key.to_string())
    }
}

impl From<usize> for SubPath {
    fn from(index: usize) -> Self {
        SubPath::Index(index)
    }
}

impl SubPath {
    pub fn is_key(&self) -> bool {
        match *self {
            SubPath::Key(_) => true,
            _ => false,
        }
    }

    pub fn to_key(&self) -> Option<&String> {
        match self {
            SubPath::Key(ref key) => Some(key),
            _ => None,
        }
    }

    pub fn is_index(&self) -> bool {
        match self {
            SubPath::Index(_) => true,
            _ => false,
        }
    }

    pub fn to_index(&self) -> Option<usize> {
        match self {
            SubPath::Index(index) => Some(*index),
            _ => None,
        }
    }
}

impl fmt::Display for SubPath {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SubPath::Key(ref key) => write!(fmt, "{}", key),
            SubPath::Index(index) => write!(fmt, "{}", index),
        }
    }
}

#[derive(Debug, Error)]
pub enum IpfsPathError {
    #[error("Invalid path {0:?}")]
    InvalidPath(String),
    #[error("Can't resolve {path:?}")]
    ResolveError { ipld: Ipld, path: SubPath },
    #[error("Expected ipld path but found ipns path.")]
    ExpectedIpldPath,
}

#[cfg(test)]
mod tests {
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
}
