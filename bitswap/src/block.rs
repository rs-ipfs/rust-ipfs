use cid::Cid;

#[derive(Clone, Debug)]
pub struct Block {
    pub cid: Cid,
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
