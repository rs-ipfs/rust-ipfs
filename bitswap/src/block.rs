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

    pub fn to_vec(self) -> Vec<u8> {
        self.data.into()
    }
}
