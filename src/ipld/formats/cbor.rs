use cbor::{Cbor, Decoder, Encoder};
pub use cbor::{CborBytes, CborTagEncode, CborError, ReadError};
use cid::Prefix;
use crate::block::Cid;
use crate::error::Error;
use crate::ipld::Ipld;
use rustc_serialize::{Encodable, Encoder as RustcEncoder};

pub(crate) const PREFIX: Prefix = Prefix {
    version: cid::Version::V1,
    codec: cid::Codec::DagCBOR,
    mh_type: multihash::Hash::SHA2256,
    mh_len: 32,
};

pub(crate) fn decode(bytes: Vec<u8>) -> Result<Ipld, Error> {
    let mut d = Decoder::from_bytes(bytes);
    let cbor: Cbor = d.read_data_item(None)?;
    cbor_to_ipld(cbor)
}

pub(crate) fn encode(data: &Ipld) -> Result<Vec<u8>, Error> {
    let mut e = Encoder::from_memory();
    data.encode(&mut e)?;
    Ok(e.as_bytes().to_owned())
}

fn cbor_to_ipld(cbor: Cbor) -> Result<Ipld, Error> {
    let ipld = match cbor {
        Cbor::Break => {
            let err = ReadError::Other("Break.".into());
            return Err(CborError::Decode(err).into())
        }
        Cbor::Undefined => Ipld::Null,
        Cbor::Null => Ipld::Null,
        Cbor::Bool(b) => Ipld::Bool(b),
        Cbor::Unsigned(u) => Ipld::U64(u.into_u64()),
        Cbor::Signed(i) => Ipld::I64(i.into_i64()),
        Cbor::Float(f) => Ipld::F64(f.into_f64()),
        Cbor::Bytes(bytes) => Ipld::Bytes(bytes.0),
        Cbor::Unicode(string) => Ipld::String(string),
        Cbor::Array(vec) => {
            let ipld_vec = vec.into_iter()
                .map(|item| cbor_to_ipld(item))
                .collect::<Result<_, _>>()?;
            Ipld::Array(ipld_vec)
        }
        Cbor::Map(map) => {
            let ipld_map = map.into_iter()
                .map(|(k, v)| {
                    Ok((k, cbor_to_ipld(v)?))
                })
                .collect::<Result<_, Error>>()?;
            Ipld::Object(ipld_map)
        }
        Cbor::Tag(tag) => {
            if tag.tag == 42 {
                if let Cbor::Bytes(bytes) = *tag.data {
                    Ipld::Cid(Cid::from(bytes.0)?)
                } else {
                    println!("{:?}", *tag.data);
                    let err = ReadError::Other("Invalid CID.".into());
                    return Err(CborError::Decode(err).into())
                }
            } else {
                let err = ReadError::Other("Unknown tag {}.".into());
                return Err(CborError::Decode(err).into())
            }
        }
    };
    Ok(ipld)
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
                let bytes = cbor::CborBytes(cid.to_bytes());
                cbor::CborTagEncode::new(42, &bytes).encode(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::Block;

    #[test]
    fn test_encode_decode() {
        let data = Ipld::Array(vec![Ipld::U64(1), Ipld::U64(2), Ipld::U64(3)]);
        let bytes = encode(&data).unwrap();
        let data2 = decode(bytes).unwrap();
        assert_eq!(data, data2);
    }

    #[test]
    fn test_cid_encode_decode() {
        let cid = Block::from("hello").cid().to_owned();
        let data = Ipld::Cid(cid);
        let bytes = encode(&data).unwrap();
        let data2 = decode(bytes).unwrap();
        assert_eq!(data, data2);
    }
}
