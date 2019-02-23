use crate::block::{Cid, Block};
use crate::ipld::{Ipld, IpldError, IpldPath, SubPath};
use std::collections::HashMap;

pub struct IpldDag {
    blocks: HashMap<Cid, Block>,
}

impl IpldDag {
    pub fn new() -> Self {
        IpldDag {
            blocks: HashMap::new(),
        }
    }

    pub fn put(&mut self, data: &Ipld) -> Result<Cid, IpldError> {
        let block = data.to_dag_cbor()?;
        let cid = block.cid();
        self.blocks.insert(block.cid(), block);
        Ok(cid)
    }

    fn get_ipld(&self, cid: &Cid) -> Result<Option<Ipld>, IpldError> {
        let block = self.blocks.get(cid);
        match block {
            Some(block) => Ok(Some(Ipld::from(block)?)),
            None => Ok(None),
        }
    }

    pub fn get(&self, path: &IpldPath) -> Result<Option<Ipld>, IpldError> {
        let mut ipld = self.get_ipld(&path.root())?;
        for sub_path in path.iter() {
            if ipld.is_none() {
                return Ok(None);
            }
            let ipld_owned = ipld.take().unwrap();
            let new_ipld = match sub_path {
                SubPath::Key(ref key) => {
                    if let Ipld::Object(mut map) = ipld_owned {
                        map.remove(key)
                    } else {
                        None
                    }
                }
                SubPath::Index(index) => {
                    if let Ipld::Array(mut vec) = ipld_owned {
                        if *index < vec.len() {
                            Some(vec.swap_remove(*index))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
            };
            ipld = match new_ipld {
                Some(Ipld::Cid(ref cid)) => self.get_ipld(cid)?,
                _ => new_ipld,
            };
        }
        Ok(ipld)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_root_cid() {
        let mut dag = IpldDag::new();
        let data = Ipld::Array(vec![Ipld::U64(1), Ipld::U64(2), Ipld::U64(3)]);
        let cid = dag.put(&data).unwrap();

        let path = IpldPath::new(cid);
        let res = dag.get(&path).unwrap();
        assert_eq!(res, Some(data));
    }

    #[test]
    fn test_resolve_array_elem() {
        let mut dag = IpldDag::new();
        let data = vec![1, 2, 3].into();
        let cid = dag.put(&data).unwrap();

        let path = IpldPath::from(cid, "1").unwrap();
        let res = dag.get(&path).unwrap();
        assert_eq!(res, Some(Ipld::U64(2)));
    }

    #[test]
    fn test_resolve_nested_array_elem() {
        let mut dag = IpldDag::new();
        let data = Ipld::Array(vec![Ipld::U64(1), Ipld::Array(vec![Ipld::U64(2)]), Ipld::U64(3)]);
        let cid = dag.put(&data).unwrap();

        let path = IpldPath::from(cid, "1/0").unwrap();
        let res = dag.get(&path).unwrap();
        assert_eq!(res, Some(Ipld::U64(2)));
    }

    #[test]
    fn test_resolve_object_elem() {
        let mut dag = IpldDag::new();
        let mut data = HashMap::new();
        data.insert("key", false);
        let cid = dag.put(&data.into()).unwrap();

        let path = IpldPath::from(cid, "key").unwrap();
        let res = dag.get(&path).unwrap();
        assert_eq!(res, Some(Ipld::Bool(false)));
    }

    #[test]
    fn test_resolve_cid_elem() {
        let mut dag = IpldDag::new();
        let data1 = vec![1].into();
        let cid1 = dag.put(&data1).unwrap();
        let data2 = vec![cid1].into();
        let cid2 = dag.put(&data2).unwrap();

        let path = IpldPath::from(cid2, "0/0").unwrap();
        let res = dag.get(&path).unwrap();
        assert_eq!(res, Some(Ipld::U64(1)));
    }
}
