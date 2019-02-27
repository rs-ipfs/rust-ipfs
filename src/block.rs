//! Block
pub use cid::Cid;

#[derive(Clone, Debug, PartialEq)]
/// An immutable ipfs block.
pub struct Block {
    data: Vec<u8>,
    cid: Cid,
}

impl Block {
    /// Creates a new immutable ipfs block.
    pub fn new(data: Vec<u8>, cid: cid::Cid) -> Self {
        Block {
            data,
            cid,
        }
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
    pub fn data(&self) -> &Vec<u8> {
        &self.data
    }
}

impl From<&str> for Block {
    fn from(content: &str) -> Block {
        let prefix = cid::Prefix {
            version: cid::Version::V0,
            codec: cid::Codec::DagProtobuf,
            mh_type: multihash::Hash::SHA2256,
            mh_len: 32,
        };
        let data = content.as_bytes().to_vec();
        let cid = cid::Cid::new_from_prefix(&prefix, &data);
        Block::new(data, cid)
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
        let computed_cid = cid::Cid::new_from_prefix(
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
        let computed_cid = cid::Cid::new_from_prefix(
            &prefix,
            &content,
        ).to_string();
        assert_eq!(cid, computed_cid);
    }

    #[test]
    fn test_block() {
        let block = Block::from("hello block\n");
        assert_eq!(block.cid().to_string(),
                   "QmVNrZhKw9JwYa4YPEZVccQxfgQJq993yP78QEN28927vq");
        assert_eq!(block.size(), 12);
    }
}
