use crate::block::Cid;

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

    pub fn root(&self) -> Cid {
        self.root.to_owned()
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
    fn test_to_string() {
        let cid = Block::from("hello").cid();
        let mut path = IpldPath::new(cid);
        path.push("key");
        path.push(3);
        let res = "/QmRN6wdp1S2A5EtjW9A3M1vKSBuQQGcgvuhoMUoEz4iiT5/key/3";
        assert_eq!(path.to_string(), res);
    }
}
