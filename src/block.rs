//! Block
pub use crate::error::Error;
pub use crate::path::{IpfsPath, PathRoot};
pub use libipld::cid::Cid;

#[derive(Clone, Debug, PartialEq)]
/// An immutable ipfs block.
pub struct Block {
    data: Box<[u8]>,
    cid: Cid,
}

impl Block {
    /// Creates a new immutable ipfs block.
    pub fn new(data: Box<[u8]>, cid: Cid) -> Self {
        Block { data, cid }
    }

    /// Returns the size of the block in bytes.
    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// Returns the content id of the block.
    pub fn cid(&self) -> &Cid {
        &self.cid
    }

    /// Returns the data of the block.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Returns the ipfs path of the block.
    pub fn path(&self, path: &str) -> Result<IpfsPath, Error> {
        IpfsPath::new(PathRoot::Ipld(self.cid.clone())).into_sub_path(path)
    }
}

impl From<&str> for Block {
    fn from(content: &str) -> Block {
        let data = content.as_bytes().to_vec().into_boxed_slice();
        let hash = multihash::Sha2_256::digest(&data);
        let cid = Cid::new_v0(hash).unwrap();
        Block::new(data, cid)
    }
}

impl Into<String> for Block {
    fn into(self) -> String {
        String::from_utf8_lossy(self.data()).to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libipld::cid::Codec;

    #[test]
    fn test_raw_block_cid() {
        let content = b"hello\n";
        let cid = "bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am";
        let hash = multihash::Sha2_256::digest(content);
        let computed_cid = Cid::new_v1(Codec::Raw, hash).to_string();
        assert_eq!(cid, computed_cid);
    }

    #[test]
    fn test_dag_pb_block_cid() {
        let content = "hello\n".as_bytes();
        let cid = "QmUJPTFZnR2CPGAzmfdYPghgrFtYFB6pf1BqMvqfiPDam8";
        let hash = multihash::Sha2_256::digest(content);
        let computed_cid = Cid::new_v0(hash).unwrap().to_string();
        assert_eq!(cid, computed_cid);
    }

    #[test]
    fn test_block() {
        let block = Block::from("hello block\n");
        assert_eq!(
            block.cid().to_string(),
            "QmVNrZhKw9JwYa4YPEZVccQxfgQJq993yP78QEN28927vq"
        );
        assert_eq!(block.size(), 12);
    }
}
