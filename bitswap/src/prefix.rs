use std::convert::TryFrom;

use libipld::cid::{self, Cid, Codec, Version};
use multihash::Code;
use unsigned_varint::{decode as varint_decode, encode as varint_encode};

/// Prefix represents all metadata of a CID, without the actual content.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Prefix {
    /// The version of CID.
    pub version: Version,
    /// The codec of CID.
    pub codec: Codec,
    /// The multihash type of CID.
    pub mh_type: Code,
    /// The multihash length of CID.
    pub mh_len: usize,
}

impl Prefix {
    /// Create a new prefix from encoded bytes.
    pub fn new(data: &[u8]) -> Result<Prefix, cid::Error> {
        let (raw_version, remain) = varint_decode::u64(data)?;
        let version = Version::try_from(raw_version)?;

        let (raw_codec, remain) = varint_decode::u64(remain)?;
        let codec = Codec::try_from(raw_codec)?;

        let (raw_mh_type, remain) = varint_decode::u64(remain)?;
        let mh_type = match multihash::Code::from_u64(raw_mh_type) {
            multihash::Code::Custom(_) => return Err(cid::Error::UnknownCodec),
            code => code,
        };

        let (mh_len, _remain) = varint_decode::usize(remain)?;

        Ok(Prefix {
            version,
            codec,
            mh_type,
            mh_len,
        })
    }

    /// Convert the prefix to encoded bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut res = Vec::with_capacity(4);

        let mut buf = varint_encode::u64_buffer();
        let version = varint_encode::u64(self.version.into(), &mut buf);
        res.extend_from_slice(version);
        let mut buf = varint_encode::u64_buffer();
        let codec = varint_encode::u64(self.codec.into(), &mut buf);
        res.extend_from_slice(codec);
        let mut buf = varint_encode::u64_buffer();
        let mh_type = varint_encode::u64(self.mh_type.to_u64(), &mut buf);
        res.extend_from_slice(mh_type);
        let mut buf = varint_encode::u64_buffer();
        let mh_len = varint_encode::u64(self.mh_len as u64, &mut buf);
        res.extend_from_slice(mh_len);

        res
    }

    /// Create a CID out of the prefix and some data that will be hashed
    pub fn to_cid(&self, data: &[u8]) -> Result<Cid, cid::Error> {
        let mut hash = self.mh_type.hasher().unwrap().digest(data);
        if self.mh_len < hash.digest().len() {
            hash = multihash::wrap(hash.algorithm(), &hash.digest()[..self.mh_len]);
        }
        Cid::new(self.version, self.codec, hash)
    }
}

impl From<&Cid> for Prefix {
    fn from(cid: &Cid) -> Self {
        Self {
            version: cid.version(),
            codec: cid.codec(),
            mh_type: cid.hash().algorithm(),
            mh_len: cid.hash().digest().len(),
        }
    }
}
