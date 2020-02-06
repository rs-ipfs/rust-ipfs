use crate::error::Error;
use crate::ipns::ipns_pb as proto;
use crate::path::IpfsPath;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use libp2p::core::PublicKey;
use libp2p::identity::Keypair;
use std::time::{Duration, SystemTime};
use std::str::FromStr;
use std::convert::TryFrom;

#[derive(Clone, Debug, PartialEq)]
pub struct IpnsEntry {
    value: String,
    seq: u64,
    validity: SystemTime,
    public_key: PublicKey,
    signature: Vec<u8>,
}

impl IpnsEntry {
    pub fn new(value: String, seq: u64, ttl: Duration, key: &Keypair) -> Self {
        let validity = SystemTime::now() + ttl;
        let public_key = key.public();
        let signature = IpnsEntry::sign(&validity, &value, &key);
        IpnsEntry {
            value,
            seq,
            validity,
            public_key,
            signature,
        }
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn from_path(path: &IpfsPath, seq: u64, key: &Keypair) -> Self {
        let value = path.to_string();
        // TODO what is a reasonable default?
        let ttl = Duration::new(1, 0);
        IpnsEntry::new(value, seq, ttl, key)
    }

    fn sign(_validity: &SystemTime, _value: &str, _key: &Keypair) -> Vec<u8> {
        // TODO
        Vec::new()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.into()
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        Self::try_from(bytes)
    }

    pub fn is_valid(&self) -> bool {
        // TODO
        true
    }

    pub fn resolve(&self) -> Result<IpfsPath, Error> {
        IpfsPath::from_str(&self.value)
    }
}

use std::io::Cursor;
use prost::Message;

impl TryFrom<&[u8]> for IpnsEntry {
    type Error = Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let proto: proto::IpnsEntry = proto::IpnsEntry::decode(bytes)?;
        let value = String::from_utf8_lossy(&proto.value).to_string();
        let public_key = PublicKey::from_protobuf_encoding(&proto.pub_key)?;
        let nanos = Cursor::new(&proto.validity).read_u64::<BigEndian>()?;
        let validity = SystemTime::UNIX_EPOCH + Duration::from_nanos(nanos);
        let ipns = IpnsEntry {
            value,
            seq: proto.sequence,
            validity,
            signature: proto.signature.to_vec(),
            public_key,
        };
        Ok(ipns)
    }
}

impl Into<Vec<u8>> for &IpnsEntry {
    fn into(self) -> Vec<u8> {
        let nanos = self.validity
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut validity = vec![];
        validity.write_u64::<BigEndian>(nanos as u64).unwrap();
        let mut proto = proto::IpnsEntry::default();
        proto.value = self.value.as_bytes().to_vec();
        proto.sequence = self.seq;
        proto.validity_type = proto::ipns_entry::ValidityType::Eol.into();
        proto.validity = validity;
        proto.signature = self.signature.clone();
        proto.pub_key = self.public_key.clone().into_protobuf_encoding();
        let mut res = Vec::with_capacity(proto.encoded_len());
        proto.encode(&mut res).expect("there is no situation in which the protobuf message can be invalid");
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generate_key() -> Keypair {
        Keypair::Ed25519(libp2p::core::identity::ed25519::Keypair::generate())
    }

    #[test]
    fn test_valid() {
        let value = "/ipfs/".into();
        let duration = Duration::new(1, 0);
        let key = generate_key();
        let ipns = IpnsEntry::new(value, 0, duration, &key);
        assert!(ipns.is_valid());
    }

    #[test]
    fn test_to_from_bytes() {
        let value = "/ipfs/".into();
        let duration = Duration::new(1, 0);
        let key = generate_key();
        let ipns = IpnsEntry::new(value, 0, duration, &key);
        let bytes = ipns.to_bytes();
        let ipns2 = IpnsEntry::from_bytes(&bytes).unwrap();
        assert_eq!(ipns, ipns2);
    }

    #[test]
    fn test_from_path() {
        let key = generate_key();
        let path = IpfsPath::from_str("/ipfs/QmUJPTFZnR2CPGAzmfdYPghgrFtYFB6pf1BqMvqfiPDam8").unwrap();
        let ipns = IpnsEntry::from_path(&path, 0, &key);
        assert_eq!(path, ipns.resolve().unwrap());
    }
}
