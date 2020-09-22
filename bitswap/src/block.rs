use cid::Cid;

/// An Ipfs block consisting of a [`Cid`] and the bytes of the block.
///
/// Note: At the moment the equality is based on [`Cid`] equality, which is based on the triple
/// `(cid::Version, cid::Codec, multihash)`.
#[derive(Clone, Debug)]
pub struct Block {
    /// The content identifier for this block
    pub cid: Cid,
    /// The data of this block
    pub data: Box<[u8]>,
}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.cid.hash() == other.cid.hash()
    }
}

impl Eq for Block {}

impl Block {
    pub fn new(data: Box<[u8]>, cid: Cid) -> Self {
        Self { cid, data }
    }

    pub fn cid(&self) -> &Cid {
        &self.cid
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.data.into()
    }
}
