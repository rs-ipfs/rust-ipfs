use crate::block::Cid;
use crate::error::Error;
use crate::ipld::IpldError;

#[derive(Clone, Debug, PartialEq)]
pub struct IpldPath {
    root: Cid,
    path: Vec<SubPath>,
}

impl IpldPath {
    pub fn new(cid: Cid) -> Self {
        IpldPath {
            root: cid,
            path: Vec::new(),
        }
    }

    pub fn from(cid: Cid, string: &str) -> Result<Self, Error> {
        let mut path = IpldPath::new(cid);
        for sub_path in string.split("/") {
            if sub_path == "" {
                return Err(IpldError::InvalidPath(string.to_owned()).into());
            }
            let index = sub_path.parse::<usize>();
            if index.is_ok() {
                path.push(index.unwrap());
            } else {
                path.push(sub_path);
            }
        }
        Ok(path)
    }

    pub fn from_str(string: &str) -> Result<Self, Error> {
        let mut subpath = string.split("/");
        if subpath.next() != Some("") {
            return Err(IpldError::InvalidPath(string.to_owned()).into());
        }
        let cid_string = subpath.next();
        if cid_string.is_none() {
            return Err(IpldError::InvalidPath(string.to_owned()).into());
        }
        let cid = Cid::from(cid_string.unwrap())?;
        IpldPath::from(cid, &subpath.collect::<Vec<&str>>().join("/"))
    }

    pub fn root(&self) -> &Cid {
        &self.root
    }

    pub fn push<T: Into<SubPath>>(&mut self, sub_path: T) {
        self.path.push(sub_path.into());
    }

    pub fn iter(&self) -> impl Iterator<Item=&SubPath> {
        self.path.iter()
    }

    pub fn to_string(&self) -> String {
        let mut path = String::new();
        path.push_str("/");
        path.push_str(&self.root.to_string());
        for sub_path in &self.path {
            path.push_str("/");
            path.push_str(&sub_path.to_string());
        }
        path
    }
}

impl From<Cid> for IpldPath {
    fn from(cid: Cid) -> Self {
        IpldPath::new(cid)
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
        let cid = Block::from("hello").cid().to_owned();
        let res = IpldPath::from(cid, "key/3").unwrap();

        let cid = Block::from("hello").cid().to_owned();
        let mut path = IpldPath::new(cid);
        path.push("key");
        path.push(3);

        assert_eq!(path, res);
    }

    #[test]
    fn test_from_errors() {
        let cid = Block::from("hello").cid().to_owned();
        assert!(IpldPath::from(cid.clone(), "").is_err());
        assert!(IpldPath::from(cid.clone(), "/").is_err());
        assert!(IpldPath::from(cid.clone(), "/abc").is_err());
        assert!(IpldPath::from(cid.clone(), "abc/").is_err());
        assert!(IpldPath::from(cid, "abc//de").is_err());
    }

    #[test]
    fn test_from_str() {
        let string = "/QmRN6wdp1S2A5EtjW9A3M1vKSBuQQGcgvuhoMUoEz4iiT5/key/3";
        let res = IpldPath::from_str(string).unwrap();

        let cid = Block::from("hello").cid().to_owned();
        let mut path = IpldPath::new(cid);
        path.push("key");
        path.push(3);

        assert_eq!(path, res);
    }

    #[test]
    fn test_from_str_errors() {
        assert!(IpldPath::from_str("").is_err());
        assert!(IpldPath::from_str("/").is_err());
        assert!(IpldPath::from_str("/QmRN").is_err());
    }

    #[test]
    fn test_to_string() {
        let cid = Block::from("hello").cid().to_owned();
        let mut path = IpldPath::new(cid);
        path.push("key");
        path.push(3);
        let res = "/QmRN6wdp1S2A5EtjW9A3M1vKSBuQQGcgvuhoMUoEz4iiT5/key/3";
        assert_eq!(path.to_string(), res);
    }
}
