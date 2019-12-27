use crate::block::{Block, Cid};
use crate::error::{Error, TryError};
use crate::ipld::{formats, IpldError};
use crate::path::{IpfsPath, PathRoot};
use cid::Codec;
use std::collections::HashMap;
use std::convert::TryInto;

/// An enum over all possible IPLD types.
#[derive(Clone, Debug, PartialEq)]
pub enum Ipld {
    /// Represents an unsigned integer.
    U64(u64),
    /// Represents a signed integer.
    I64(i64),
    /// Represents a byte string.
    Bytes(Vec<u8>),
    /// Represents an UTF-8 string.
    String(String),
    /// Represents a list.
    Array(Vec<Ipld>),
    /// Represents a map.
    Object(HashMap<String, Ipld>),
    /// Represents a floating point value.
    F64(f64),
    /// Represents a boolean value.
    Bool(bool),
    /// Represents the absence of a value or the value undefined.
    Null,
    /// Represents a link to an Ipld node
    Link(PathRoot),
}

impl Ipld {
    pub fn to_block(&self, codec: Codec) -> Result<Block, Error> {
        let (prefix, bytes) = match codec {
            Codec::DagCBOR => {
                (
                    formats::cbor::PREFIX,
                    formats::cbor::encode(&self)?,
                )
            }
            Codec::DagProtobuf => {
                (
                    formats::pb::PREFIX,
                    formats::pb::encode(self.to_owned())?,
                )
            }
            codec => return Err(IpldError::UnsupportedCodec(codec).into()),
        };
        let cid = cid::Cid::new_from_prefix(&prefix, &bytes);
        Ok(Block::new(bytes, cid))
    }

    pub fn to_dag_cbor(&self) -> Result<Block, Error> {
        self.to_block(Codec::DagCBOR)
    }

    pub fn to_dag_pb(&self) -> Result<Block, Error> {
        self.to_block(Codec::DagProtobuf)
    }

    pub fn from(block: &Block) -> Result<Self, Error> {
        let data = match block.cid().prefix().codec {
            Codec::DagCBOR => {
                formats::cbor::decode(block.data())?
            }
            Codec::DagProtobuf => {
                formats::pb::decode(block.data())?
            }
            codec => return Err(IpldError::UnsupportedCodec(codec).into()),
        };
        Ok(data)
    }
}

impl From<u32> for Ipld {
    fn from(u: u32) -> Self {
        Ipld::U64(u as u64)
    }
}

impl From<u64> for Ipld {
    fn from(u: u64) -> Self {
        Ipld::U64(u)
    }
}

impl From<i32> for Ipld {
    fn from(i: i32) -> Self {
        Ipld::I64(i as i64)
    }
}

impl From<i64> for Ipld {
    fn from(i: i64) -> Self {
        Ipld::I64(i)
    }
}

impl From<Vec<u8>> for Ipld {
    fn from(bytes: Vec<u8>) -> Self {
        Ipld::Bytes(bytes)
    }
}

impl From<String> for Ipld {
    fn from(string: String) -> Self {
        Ipld::String(string)
    }
}

impl From<&str> for Ipld {
    fn from(string: &str) -> Self {
        Ipld::String(string.to_string())
    }
}

impl<T: Into<Ipld>> From<Vec<T>> for Ipld {
    fn from(vec: Vec<T>) -> Self {
        Ipld::Array(vec.into_iter().map(|ipld| ipld.into()).collect())
    }
}

impl<T: Into<Ipld>> From<HashMap<String, T>> for Ipld {
    fn from(map: HashMap<String, T>) -> Self {
        Ipld::Object(map.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

impl<T: Into<Ipld>> From<HashMap<&str, T>> for Ipld {
    fn from(map: HashMap<&str, T>) -> Self {
        Ipld::Object(map.into_iter().map(|(k, v)| (k.to_string(), v.into())).collect())
    }
}

impl From<f64> for Ipld {
    fn from(f: f64) -> Self {
        Ipld::F64(f)
    }
}

impl From<bool> for Ipld {
    fn from(b: bool) -> Self {
        Ipld::Bool(b)
    }
}

impl From<Cid> for Ipld {
    fn from(cid: Cid) -> Self {
        Ipld::Link(cid.into())
    }
}

impl From<PathRoot> for Ipld {
    fn from(root: PathRoot) -> Self {
        Ipld::Link(root)
    }
}

impl From<IpfsPath> for Ipld {
    fn from(path: IpfsPath) -> Self {
        Ipld::Link(path.root().to_owned())
    }
}

impl TryInto<u64> for Ipld {
    type Error = TryError;

    fn try_into(self) -> Result<u64, Self::Error> {
        match self {
            Ipld::U64(u) => Ok(u),
            _ => Err(TryError)
        }
    }
}

impl TryInto<i64> for Ipld {
    type Error = TryError;

    fn try_into(self) -> Result<i64, Self::Error> {
        match self {
            Ipld::I64(i) => Ok(i),
            _ => Err(TryError)
        }
    }
}

impl TryInto<Vec<u8>> for Ipld {
    type Error = TryError;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        match self {
            Ipld::Bytes(bytes) => Ok(bytes),
            _ => Err(TryError)
        }
    }
}

impl TryInto<String> for Ipld {
    type Error = TryError;

    fn try_into(self) -> Result<String, Self::Error> {
        match self {
            Ipld::String(string) => Ok(string),
            _ => Err(TryError)
        }
    }
}

impl TryInto<Vec<Ipld>> for Ipld {
    type Error = TryError;

    fn try_into(self) -> Result<Vec<Ipld>, Self::Error> {
        match self {
            Ipld::Array(vec) => Ok(vec),
            _ => Err(TryError)
        }
    }
}

impl TryInto<HashMap<String, Ipld>> for Ipld {
    type Error = TryError;

    fn try_into(self) -> Result<HashMap<String, Ipld>, Self::Error> {
        match self {
            Ipld::Object(map) => Ok(map),
            _ => Err(TryError)
        }
    }
}

impl TryInto<f64> for Ipld {
    type Error = TryError;

    fn try_into(self) -> Result<f64, Self::Error> {
        match self {
            Ipld::F64(f) => Ok(f),
            _ => Err(TryError)
        }
    }
}

impl TryInto<bool> for Ipld {
    type Error = TryError;

    fn try_into(self) -> Result<bool, Self::Error> {
        match self {
            Ipld::Bool(b) => Ok(b),
            _ => Err(TryError)
        }
    }
}

impl TryInto<PathRoot> for Ipld {
    type Error = TryError;

    fn try_into(self) -> Result<PathRoot, Self::Error> {
        match self {
            Ipld::Link(root) => Ok(root),
            _ => Err(TryError)
        }
    }
}

impl TryInto<Cid> for Ipld {
    type Error = TryError;

    fn try_into(self) -> Result<Cid, Self::Error> {
        match self {
            Ipld::Link(root) => root.try_into(),
            _ => Err(TryError)
        }
    }
}
