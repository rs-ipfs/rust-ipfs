use crate::block::Cid;
use crate::error::Error;
use std::convert::TryFrom;

pub mod error;
pub use self::error::IpfsPathError;

#[derive(Clone, Debug, PartialEq)]
pub struct IpfsPath {
    root: PathRoot,
    path: Vec<SubPath>,
}

impl IpfsPath {
    pub fn new(root: PathRoot) -> Self {
        IpfsPath {
            root,
            path: Vec::new(),
        }
    }

    pub fn from_str(string: &str) -> Result<Self, Error> {
        let mut subpath = string.split("/");
        let empty = subpath.next();
        let root_type = subpath.next();
        let cid = subpath.next().map(|cid_string| {
            Cid::from(cid_string)
        });
        let root = match (empty, root_type, cid) {
            (Some(""), Some("ipfs"), Some(Ok(cid))) => PathRoot::Ipld(cid),
            (Some(""), Some("ipld"), Some(Ok(cid))) => PathRoot::Ipld(cid),
            (Some(""), Some("ipns"), Some(Ok(cid))) => PathRoot::Ipns(cid),
            _ => return Err(IpfsPathError::InvalidPath(string.to_owned()).into()),
        };
        let mut path = IpfsPath::new(root);
        path.push_str(&subpath.collect::<Vec<&str>>().join("/"))?;
        Ok(path)
    }

    pub fn ipld_root(&self) -> Result<&Cid, Error> {
        if self.root.is_ipld() {
            Ok(self.root.cid())
        } else {
            Err(IpfsPathError::ExpectedIpldPath.into())
        }
    }

    pub fn cid(&self) -> Cid {
        self.root.cid().to_owned()
    }

    pub fn push<T: Into<SubPath>>(&mut self, sub_path: T) {
        self.path.push(sub_path.into());
    }

    pub fn push_str(&mut self, string: &str) -> Result<(), Error> {
        if string.is_empty() {
            return Ok(());
        }
        for sub_path in string.split("/") {
            if sub_path == "" {
                return Err(IpfsPathError::InvalidPath(string.to_owned()).into());
            }
            let index = sub_path.parse::<usize>();
            if index.is_ok() {
                self.push(index.unwrap());
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

    pub fn iter(&self) -> impl Iterator<Item=&SubPath> {
        self.path.iter()
    }

    pub fn to_string(&self) -> String {
        let mut path = self.root.to_string();
        for sub_path in &self.path {
            path.push_str("/");
            path.push_str(&sub_path.to_string());
        }
        path
    }
}

impl TryFrom<&str> for IpfsPath {
    type Error = Error;

    fn try_from(string: &str) -> Result<Self, Self::Error> {
        IpfsPath::from_str(string)
    }
}

impl From<PathRoot> for IpfsPath {
    fn from(root: PathRoot) -> Self {
        IpfsPath::new(root)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum PathRoot {
    Ipld(Cid),
    Ipns(Cid),
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

    pub fn cid(&self) -> &Cid {
        match self {
            PathRoot::Ipld(cid) => cid,
            PathRoot::Ipns(cid) => cid,
        }
    }

    pub fn to_string(&self) -> String {
        let mut string = match self {
            PathRoot::Ipld(_) => "/ipfs/",
            PathRoot::Ipns(_) => "/ipns/",
        }.to_string();
        string.push_str(&self.cid().to_string());
        string
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

    pub fn to_key<'a>(&'a self) -> Option<&'a String> {
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

    pub fn to_string(&self) -> String {
        match self {
            SubPath::Key(ref key) => key.to_owned(),
            SubPath::Index(index) => index.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::Block;

    #[test]
    fn test_from() {
        let res = Block::from("hello").path("key/3").unwrap();

        let cid = Block::from("hello").cid().to_owned();
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
    }
}
