//! IPLD dag-json, dag-cbor and some dag-pb functionality.
//!
//! This code was adapted from https://github.com/ipfs-rust/rust-ipld, and most of
//! its code is the same as at revision b2286c53c13f3eeec2a3766387f2926838e8e4c9;
//! it used to be a direct dependency, but recent updates to cid and multihash crates
//! made it incompatible with them.

pub mod dag_cbor;
pub mod dag_json;
pub mod dag_pb;
#[macro_use]
pub mod ipld_macro;

use cid::{Cid, Codec};
use dag_cbor::DagCborCodec;
use dag_json::DagJsonCodec;
use dag_pb::DagPbCodec;
use multihash::Multihash;
use std::collections::BTreeMap;
use thiserror::Error;

/// Ipld
#[derive(Clone, Debug, PartialEq)]
pub enum Ipld {
    /// Represents the absence of a value or the value undefined.
    Null,
    /// Represents a boolean value.
    Bool(bool),
    /// Represents an integer.
    Integer(i128),
    /// Represents a floating point value.
    Float(f64),
    /// Represents an UTF-8 string.
    String(String),
    /// Represents a sequence of bytes.
    Bytes(Vec<u8>),
    /// Represents a list.
    List(Vec<Ipld>),
    /// Represents a map.
    Map(BTreeMap<String, Ipld>),
    /// Represents a link to an Ipld node
    Link(Cid),
}

macro_rules! derive_to_ipld_prim {
    ($enum:ident, $ty:ty, $fn:ident) => {
        impl From<$ty> for Ipld {
            fn from(t: $ty) -> Self {
                Ipld::$enum(t.$fn() as _)
            }
        }
    };
}

macro_rules! derive_to_ipld {
    ($enum:ident, $ty:ty, $fn:ident) => {
        impl From<$ty> for Ipld {
            fn from(t: $ty) -> Self {
                Ipld::$enum(t.$fn())
            }
        }
    };
}

derive_to_ipld!(Bool, bool, clone);
derive_to_ipld_prim!(Integer, i8, clone);
derive_to_ipld_prim!(Integer, i16, clone);
derive_to_ipld_prim!(Integer, i32, clone);
derive_to_ipld_prim!(Integer, i64, clone);
derive_to_ipld_prim!(Integer, i128, clone);
derive_to_ipld_prim!(Integer, isize, clone);
derive_to_ipld_prim!(Integer, u8, clone);
derive_to_ipld_prim!(Integer, u16, clone);
derive_to_ipld_prim!(Integer, u32, clone);
derive_to_ipld_prim!(Integer, u64, clone);
derive_to_ipld_prim!(Integer, usize, clone);
derive_to_ipld_prim!(Float, f32, clone);
derive_to_ipld_prim!(Float, f64, clone);
derive_to_ipld!(String, String, into);
derive_to_ipld!(String, &str, to_string);
derive_to_ipld!(Bytes, Vec<u8>, into);
derive_to_ipld!(Bytes, &[u8], to_vec);
derive_to_ipld!(List, Vec<Ipld>, into);
derive_to_ipld!(Map, BTreeMap<String, Ipld>, to_owned);
derive_to_ipld!(Link, Cid, clone);
derive_to_ipld!(Link, &Cid, to_owned);

/// An index into ipld
pub enum IpldIndex<'a> {
    /// An index into an ipld list.
    List(usize),
    /// An owned index into an ipld map.
    Map(String),
    /// An index into an ipld map.
    MapRef(&'a str),
}

impl<'a> From<usize> for IpldIndex<'a> {
    fn from(index: usize) -> Self {
        Self::List(index)
    }
}

impl<'a> From<String> for IpldIndex<'a> {
    fn from(key: String) -> Self {
        Self::Map(key)
    }
}

impl<'a> From<&'a str> for IpldIndex<'a> {
    fn from(key: &'a str) -> Self {
        Self::MapRef(key)
    }
}

impl Ipld {
    /// Indexes into a ipld list or map.
    pub fn get<'a, T: Into<IpldIndex<'a>>>(&self, index: T) -> Option<&Ipld> {
        match self {
            Ipld::List(l) => match index.into() {
                IpldIndex::List(i) => l.get(i),
                _ => None,
            },
            Ipld::Map(m) => match index.into() {
                IpldIndex::Map(ref key) => m.get(key),
                IpldIndex::MapRef(key) => m.get(key),
                _ => None,
            },
            _ => None,
        }
    }

    /// Returns an iterator.
    pub fn iter(&self) -> IpldIter<'_> {
        IpldIter {
            stack: vec![Box::new(vec![self].into_iter())],
        }
    }
}

/// Ipld iterator.
pub struct IpldIter<'a> {
    stack: Vec<Box<dyn Iterator<Item = &'a Ipld> + 'a>>,
}

impl<'a> Iterator for IpldIter<'a> {
    type Item = &'a Ipld;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(iter) = self.stack.last_mut() {
                if let Some(ipld) = iter.next() {
                    match ipld {
                        Ipld::List(list) => {
                            self.stack.push(Box::new(list.iter()));
                        }
                        Ipld::Map(map) => {
                            self.stack.push(Box::new(map.values()));
                        }
                        _ => {}
                    }
                    return Some(ipld);
                } else {
                    self.stack.pop();
                }
            } else {
                return None;
            }
        }
    }
}

/// Ipld type error.
#[derive(Debug, Error)]
pub enum IpldError {
    /// Expected a boolean.
    #[error("Expected a boolean.")]
    NotBool,
    /// Expected an integer.
    #[error("Expected an integer.")]
    NotInteger,
    /// Expected a float.
    #[error("Expected a float.")]
    NotFloat,
    /// Expected a string.
    #[error("Expected a string.")]
    NotString,
    /// Expected bytes.
    #[error("Expected bytes.")]
    NotBytes,
    /// Expected a list.
    #[error("Expected a list.")]
    NotList,
    /// Expected a map.
    #[error("Expected a map.")]
    NotMap,
    /// Expected a cid.
    #[error("Expected a cid.")]
    NotLink,
    /// Expected a key.
    #[error("Expected a key.")]
    NotKey,
    /// Index not found.
    #[error("Index not found.")]
    IndexNotFound,
    /// Key not found.
    #[error("Key not found.")]
    KeyNotFound,
}

/// Block error.
#[derive(Debug, Error)]
pub enum BlockError {
    /// Block exceeds MAX_BLOCK_SIZE.
    #[error("Block size {0} exceeds MAX_BLOCK_SIZE.")]
    BlockTooLarge(usize),
    /// Hash does not match the CID.
    #[error("Hash does not match the CID.")]
    InvalidHash(Multihash),
    /// The codec is unsupported.
    #[error("Unsupported codec {0:?}.")]
    UnsupportedCodec(cid::Codec),
    /// The multihash is unsupported.
    #[error("Unsupported multihash {0:?}.")]
    UnsupportedMultihash(multihash::Code),
    /// The codec returned an error.
    #[error("Codec error: {0}")]
    CodecError(Box<dyn std::error::Error + Send + Sync>),
    /// Io error.
    #[error("{0}")]
    Io(std::io::Error),
    /// Cid error.
    #[error("{0}")]
    Cid(cid::Error),
    /// Link error.
    #[error("Invalid link.")]
    InvalidLink,
}

impl From<std::io::Error> for BlockError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<cid::Error> for BlockError {
    fn from(err: cid::Error) -> Self {
        Self::Cid(err)
    }
}

/// The maximum block size is 1MiB.
pub const MAX_BLOCK_SIZE: usize = 1_048_576;

/// Validate a block.
pub fn validate(cid: &Cid, data: &[u8]) -> Result<(), BlockError> {
    if data.len() > MAX_BLOCK_SIZE {
        return Err(BlockError::BlockTooLarge(data.len()));
    }
    let hash = cid.hash().algorithm().digest(&data);
    if hash.as_ref() != cid.hash() {
        return Err(BlockError::InvalidHash(hash));
    }
    Ok(())
}

/// Encode ipld to bytes.
pub fn encode_ipld(ipld: &Ipld, codec: Codec) -> Result<Box<[u8]>, BlockError> {
    let bytes = match codec {
        Codec::DagCBOR => DagCborCodec::encode(ipld)?,
        Codec::DagProtobuf => DagPbCodec::encode(ipld)?,
        Codec::DagJSON => DagJsonCodec::encode(ipld)?,
        Codec::Raw => {
            if let Ipld::Bytes(bytes) = ipld {
                bytes.to_vec().into_boxed_slice()
            } else {
                return Err(BlockError::CodecError(IpldError::NotBytes.into()));
            }
        }
        _ => return Err(BlockError::UnsupportedCodec(codec)),
    };
    Ok(bytes)
}

/// Decode block to ipld.
pub fn decode_ipld(cid: &Cid, data: &[u8]) -> Result<Ipld, BlockError> {
    let ipld = match cid.codec() {
        Codec::DagCBOR => DagCborCodec::decode(data)?,
        Codec::DagProtobuf => DagPbCodec::decode(data)?,
        Codec::DagJSON => DagJsonCodec::decode(data)?,
        Codec::Raw => Ipld::Bytes(data.to_vec()),
        _ => return Err(BlockError::UnsupportedCodec(cid.codec())),
    };
    Ok(ipld)
}
