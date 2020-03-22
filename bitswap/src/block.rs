use libipld::cid::Cid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Block {
    pub cid: Cid,
    pub data: Box<[u8]>,
}

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
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use libipld::cid::Codec;
    use multihash::Sha2_256;

    pub fn create_block(bytes: &[u8]) -> Block {
        let digest = Sha2_256::digest(bytes);
        let cid = Cid::new_v1(Codec::Raw, digest);
        Block::new(bytes.to_vec().into_boxed_slice(), cid)
    }
}
