use crate::block::Cid;
use crate::error::Error;
use crate::ipld::Ipld;
use protobuf::Message;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};

mod dag_pb;

pub(crate) fn decode(bytes: &Vec<u8>) -> Result<Ipld, Error> {
    Ok(PbNode::from_bytes(bytes)?.into())
}

pub(crate) fn encode(data: Ipld) -> Result<Vec<u8>, Error> {
    let pb_node: PbNode = data.to_owned().try_into()?;
    Ok(pb_node.into_bytes())
}

struct PbLink {
    pub cid: Cid,
    pub name: String,
    pub size: u64,
}

struct PbNode {
    pub links: Vec<PbLink>,
    pub data: Vec<u8>,
}

impl PbNode {
    fn from_bytes(bytes: &Vec<u8>) -> Result<Self, Error> {
        let proto: dag_pb::PBNode = protobuf::parse_from_bytes(bytes)?;
        let data = proto.get_Data().to_vec();
        let mut links = Vec::new();
        for link in proto.get_Links() {
            let cid = Cid::from(link.get_Hash())?;
            let name = link.get_Name().to_string();
            let size = link.get_Tsize();
            links.push(PbLink {
                cid,
                name,
                size,
            });
        }
        Ok(PbNode {
            links,
            data,
        })
    }

    fn into_bytes(self) -> Vec<u8> {
        let mut proto = dag_pb::PBNode::new();
        proto.set_Data(self.data);
        for link in self.links {
            let mut pb_link = dag_pb::PBLink::new();
            pb_link.set_Hash(link.cid.to_bytes());
            pb_link.set_Name(link.name);
            pb_link.set_Tsize(link.size);
            proto.mut_Links().push(pb_link);
        }
        proto
            .write_to_bytes()
            .expect("there is no situation in which the protobuf message can be invalid")
    }
}

impl Into<Ipld> for PbNode {
    fn into(self) -> Ipld {
        let mut map = HashMap::<&str, Ipld>::new();
        map.insert("Links", self.links.into());
        map.insert("Data", self.data.into());
        map.into()
    }
}

impl Into<Ipld> for PbLink {
    fn into(self) -> Ipld {
        let mut map = HashMap::<&str, Ipld>::new();
        map.insert("Hash", self.cid.into());
        map.insert("Name", self.name.into());
        map.insert("Tsize", self.size.into());
        map.into()
    }
}

impl TryFrom<Ipld> for PbNode {
    type Error = std::option::NoneError;

    fn try_from(ipld: Ipld) -> Result<PbNode, Self::Error> {
        match ipld {
            Ipld::Object(mut map) => {
                let links: Vec<Ipld> = map.remove("Links")?.try_into()?;
                let links: Vec<PbLink> = links.into_iter()
                    .map(|link| link.try_into()).collect::<Result<_, Self::Error>>()?;
                let data: Vec<u8> = map.remove("Data")?.try_into()?;
                Ok(PbNode {
                    links,
                    data,
                })
            }
            _ => None?
        }
    }
}

impl TryFrom<Ipld> for PbLink {
    type Error = std::option::NoneError;

    fn try_from(ipld: Ipld) -> Result<PbLink, Self::Error> {
        match ipld {
            Ipld::Object(mut map) => {
                let cid: Cid = map.remove("Hash")?.try_into()?;
                let name: String = map.remove("Name")?.try_into()?;
                let size: u64 = map.remove("Tsize")?.try_into()?;
                Ok(PbLink {
                    cid,
                    name,
                    size,
                })
            }
            _ => None?
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode() {
        //let pb_link = HashMap::<&str, Ipld>::new();
        //pb_link.insert("Hash", Cid::from());
        //pb_link.insert("Tsize", 13.into());

        let links: Vec<Ipld> = vec![];
        let mut pb_node = HashMap::<&str, Ipld>::new();
        pb_node.insert("Data", "Here is some data\n".as_bytes().to_vec().into());
        pb_node.insert("Links", links.into());
        let data: Ipld = pb_node.into();

        let bytes = encode(data.clone()).unwrap();
        let data2 = decode(&bytes).unwrap();
        assert_eq!(data, data2);
    }
}
