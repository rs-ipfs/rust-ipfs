use crate::error::Error;
use crate::ipns::ipns_pb as proto;
use crate::path::IpfsPath;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use libp2p::core::PublicKey;
use libp2p::identity::Keypair;
use protobuf::{self, ProtobufError, Message as ProtobufMessage};
use std::time::{Duration, SystemTime};

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
        let public_key = key.to_public_key();
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

    fn sign(_validity: &SystemTime, _value: &String, _key: &Keypair) -> Vec<u8> {
        // TODO
        Vec::new()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut proto = proto::IpnsEntry::new();
        proto.set_value(self.value.as_bytes().to_vec());
        proto.set_sequence(self.seq);
        let nanos = self.validity
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut validity = vec![];
        validity.write_u64::<BigEndian>(nanos as u64).unwrap();
        proto.set_validityType(proto::IpnsEntry_ValidityType::EOL);
        proto.set_validity(validity);
        proto.set_signature(self.signature.clone());
        proto.set_pubKey(self.public_key.clone().into_protobuf_encoding());
        proto
            .write_to_bytes()
            .expect("there is no situation in which the protobuf message can be invalid")
    }

    pub fn from_bytes(bytes: &Vec<u8>) -> Result<Self, ProtobufError> {
        let proto: proto::IpnsEntry = protobuf::parse_from_bytes(bytes)?;
        let value = String::from_utf8_lossy(proto.get_value()).to_string();
        let public_key = PublicKey::from_protobuf_encoding(proto.get_pubKey())?;
        let nanos = proto.get_validity().read_u64::<BigEndian>()?;
        let validity = SystemTime::UNIX_EPOCH + Duration::from_nanos(nanos);
        let ipns = IpnsEntry {
            value,
            seq: proto.get_sequence(),
            validity,
            signature: proto.get_signature().to_vec(),
            public_key,
        };
        Ok(ipns)
    }

    pub fn is_valid(&self) -> bool {
        // TODO
        true
    }

    pub fn resolve(&self) -> Result<IpfsPath, Error> {
        IpfsPath::from_str(&self.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid() {
        let value = "/ipfs/".into();
        let duration = Duration::new(1, 0);
        let key = SecioKeyPair::ed25519_generated().unwrap();
        let ipns = IpnsEntry::new(value, 0, duration, &key);
        assert!(ipns.is_valid());
    }

    #[test]
    fn test_to_from_bytes() {
        let value = "/ipfs/".into();
        let duration = Duration::new(1, 0);
        let key = SecioKeyPair::ed25519_generated().unwrap();
        let ipns = IpnsEntry::new(value, 0, duration, &key);
        let bytes = ipns.to_bytes();
        let ipns2 = IpnsEntry::from_bytes(&bytes).unwrap();
        assert_eq!(ipns, ipns2);
    }

    #[test]
    fn test_from_path() {
        let key = SecioKeyPair::ed25519_generated().unwrap();
        let path = IpfsPath::from_str("/ipfs/QmUJPTFZnR2CPGAzmfdYPghgrFtYFB6pf1BqMvqfiPDam8").unwrap();
        let ipns = IpnsEntry::from_path(&path, 0, &key);
        assert_eq!(path, ipns.resolve().unwrap());
    }
}
