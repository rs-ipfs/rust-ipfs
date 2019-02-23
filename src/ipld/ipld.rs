use crate::block::{Block, Cid};
use crate::ipld::{IpldError, cbor};
use cid::{Codec, Prefix};
use rustc_serialize::{Encodable, Encoder as RustcEncoder};
use std::collections::HashMap;

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
    Cid(Cid),
}

impl Ipld {
    pub fn to_block_with_prefix(&self, prefix: &Prefix) -> Result<Block, IpldError> {
        let bytes = match prefix.codec {
            Codec::DagCBOR => {
                cbor::encode(&self)?
            }
            codec => return Err(IpldError::UnsupportedCodec(codec)),
        };
        let cid = cid::Cid::new_from_prefix(prefix, &bytes);
        Ok(Block::new(bytes, cid))
    }

    pub fn to_block(&self, codec: Codec) -> Result<Block, IpldError> {
        let prefix = Prefix {
            version: cid::Version::V1,
            codec: codec,
            mh_type: multihash::Hash::SHA2256,
            mh_len: 32,
        };
        self.to_block_with_prefix(&prefix)
    }

    pub fn to_dag_cbor(&self) -> Result<Block, IpldError> {
        self.to_block(Codec::DagCBOR)
    }

    pub fn from(block: &Block) -> Result<Self, IpldError> {
        let codec = block.cid().prefix().codec;
        let bytes = (*block.data()).clone();
        let data = match codec {
            Codec::DagCBOR => {
                cbor::decode(bytes)?
            }
            codec => return Err(IpldError::UnsupportedCodec(codec)),
        };
        Ok(data)
    }
}

impl Encodable for Ipld {
    fn encode<E: RustcEncoder>(&self, e: &mut E) -> Result<(), E::Error> {
        match *self {
            Ipld::U64(ref u) => {
                u.encode(e)
            }
            Ipld::I64(ref i) => {
                i.encode(e)
            }
            Ipld::Bytes(ref bytes) => {
                cbor::CborBytes(bytes.to_owned()).encode(e)
            }
            Ipld::String(ref string) => {
                string.encode(e)
            }
            Ipld::Array(ref vec) => {
                vec.encode(e)
            }
            Ipld::Object(ref map) => {
                map.encode(e)
            }
            Ipld::F64(f) => {
                f.encode(e)
            },
            Ipld::Bool(b) => {
                b.encode(e)
            },
            Ipld::Null => {
                e.emit_nil()
            },
            Ipld::Cid(ref cid) => {
                // TODO generalize
                let bytes = cbor::CborBytes(cid.to_bytes());
                cbor::CborTagEncode::new(42, &bytes).encode(e)
            }
        }
    }
}
