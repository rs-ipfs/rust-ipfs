//! IPFS node implementation
#![deny(missing_docs)]
#![deny(warnings)]
use cid::Cid;
use std::collections::HashMap;
use std::sync::Arc;

pub mod p2p;

/// Ipfs struct creates a new IPFS node and is the main entry point
/// for interacting with IPFS.
pub struct Ipfs {
    blocks: HashMap<Vec<u8>, Block>,
}

impl Ipfs {
    /// Creates a new ipfs node.
    pub fn new() -> Self {
        Ipfs {
            blocks: HashMap::new(),
        }
    }

    /// Puts a block into the ipfs repo.
    pub fn put_block(&mut self, block: Block) -> Result<Cid, ()> {
        let cid = block.cid();
        self.blocks.insert(cid.to_bytes(), block);
        Ok(cid)
    }

    /// Retrives a block from the ipfs repo.
    pub fn get_block(&self, cid: &Cid) -> Result<Block, ()> {
        self.blocks.get(&cid.to_bytes())
            .map_or(Err(()), |block| Ok((*block).clone()))
    }
}

#[derive(Clone, Debug, PartialEq)]
/// An immutable ipfs block.
pub struct Block {
    content: Arc<Vec<u8>>,
}

impl Block {
    /// Creates a new immutable ipfs block.
    pub fn new(content: Vec<u8>) -> Self {
        Block { content: Arc::new(content) }
    }

    /// Returns the size of the block in bytes.
    pub fn size(&self) -> usize {
        self.content.len()
    }

    /// Returns the content id of the block.
    pub fn cid(&self) -> Cid {
        let prefix = cid::Prefix {
            version: cid::Version::V0,
            codec: cid::Codec::DagProtobuf,
            mh_type: multihash::Hash::SHA2256,
            mh_len: 32,
        };
        Cid::new_from_prefix(&prefix, &self.content)
    }
}

impl From<&str> for Block {
    fn from(content: &str) -> Block {
        Block::new(content.as_bytes().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raw_block_cid() {
        let content = "hello\n".as_bytes();
        let cid = "zb2rhcc1wJn2GHDLT2YkmPq5b69cXc2xfRZZmyufbjFUfBkxr";
        let prefix = cid::Prefix {
            version: cid::Version::V1,
            codec: cid::Codec::Raw,
            mh_type: multihash::Hash::SHA2256,
            mh_len: 32,
        };
        let computed_cid = Cid::new_from_prefix(
            &prefix,
            &content,
        ).to_string();
        assert_eq!(cid, computed_cid);
    }

    #[test]
    fn test_dag_pb_block_cid() {
        let content = "hello\n".as_bytes();
        let cid = "QmUJPTFZnR2CPGAzmfdYPghgrFtYFB6pf1BqMvqfiPDam8";
        let prefix = cid::Prefix {
            version: cid::Version::V0,
            codec: cid::Codec::DagProtobuf,
            mh_type: multihash::Hash::SHA2256,
            mh_len: 32,
        };
        let computed_cid = Cid::new_from_prefix(
            &prefix,
            &content,
        ).to_string();
        assert_eq!(cid, computed_cid);
    }

    #[test]
    fn test_put_and_get_block() {
        let mut ipfs = Ipfs::new();
        let block = Block::from("hello block\n");
        assert_eq!(block.cid().to_string(),
                   "QmVNrZhKw9JwYa4YPEZVccQxfgQJq993yP78QEN28927vq");
        assert_eq!(block.size(), 12);

        let cid = ipfs.put_block(block.clone()).unwrap();
        let new_block = ipfs.get_block(&cid).unwrap();
        assert_eq!(block, new_block);
    }
}
