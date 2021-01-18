//! go-ipfs compatible configuration file handling and setup.

use ipfs::{multiaddr, Multiaddr};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::num::NonZeroU16;
use std::path::Path;
use std::str::FromStr;
use structopt::StructOpt;
use thiserror::Error;

/// Temporary module required to de/ser config files base64'd protobuf rsa private key format.
/// Temporary until the private key import/export can be PR'd into rust-libp2p.
mod keys_proto {
    include!(concat!(env!("OUT_DIR"), "/keys_proto.rs"));
}

/// Defines the configuration types supported by the API.
#[derive(Debug, StructOpt)]
pub enum Profile {
    Test,
    Default,
}

// Required for structopt.
impl FromStr for Profile {
    type Err = InitializationError;

    fn from_str(profile: &str) -> Result<Self, Self::Err> {
        match profile {
            "test" => Ok(Profile::Test),
            "default" => Ok(Profile::Default),
            _ => Err(InitializationError::InvalidProfile(profile.to_string())),
        }
    }
}

/// The way things can go wrong when calling [`init`].
#[derive(Error, Debug)]
pub enum InitializationError {
    #[error("repository creation failed: {0}")]
    DirectoryCreationFailed(std::io::Error),
    #[error("configuration file creation failed: {0}")]
    ConfigCreationFailed(std::io::Error),
    #[error("invalid RSA key length given: {0}")]
    InvalidRsaKeyLength(u16),
    #[error("unsupported profiles selected: {0:?}")]
    InvalidProfile(String),
    #[error("key generation failed: {0}")]
    KeyGeneration(Box<dyn std::error::Error + 'static>),
    #[error("key encoding failed: {0}")]
    PrivateKeyEncodingFailed(prost::EncodeError),
    #[error("config serialization failed: {0}")]
    ConfigWritingFailed(Box<dyn std::error::Error + 'static>),
}

/// Creates the IPFS_PATH directory structure and creates a new compatible configuration file with
/// RSA key of length `bits`. Returns the Peer ID.
pub fn init(
    ipfs_path: &Path,
    bits: NonZeroU16,
    profiles: Vec<Profile>,
) -> Result<String, InitializationError> {
    use multibase::Base::Base64Pad;
    use prost::Message;
    use std::fs::OpenOptions;
    use std::io::{BufWriter, Write};

    if profiles.len() != 1 {
        unimplemented!("Multiple profiles are currently unsupported!")
    }

    let bits = bits.get();

    if !(2048..=16 * 1024).contains(&bits) {
        // Ring won't accept less than a 2048 bit key.
        return Err(InitializationError::InvalidRsaKeyLength(bits));
    }

    let pk = openssl::rsa::Rsa::generate(bits as u32)
        .map_err(|e| InitializationError::KeyGeneration(Box::new(e)))?;

    // sadly the pkcs8 to der functions are not yet exposed via the nicer interface
    // https://github.com/sfackler/rust-openssl/issues/880
    let pkcs8 = openssl::pkey::PKey::from_rsa(pk.clone())
        .and_then(|pk| pk.private_key_to_pem_pkcs8())
        .map_err(|e| InitializationError::KeyGeneration(Box::new(e)))?;

    let mut pkcs8 = pem_to_der(&pkcs8);

    let kp = ipfs::Keypair::rsa_from_pkcs8(&mut pkcs8)
        .expect("Failed to turn pkcs#8 into libp2p::identity::Keypair");

    let peer_id = kp.public().into_peer_id().to_string();

    // TODO: this part could be PR'd to rust-libp2p as they already have some public key
    // import/export but probably not if ring does not support these required conversions.

    let pkcs1 = pk
        .private_key_to_der()
        .map_err(|e| InitializationError::KeyGeneration(Box::new(e)))?;

    let key_desc = keys_proto::PrivateKey {
        r#type: keys_proto::KeyType::Rsa as i32,
        data: pkcs1,
    };

    let private_key = {
        let mut buf = Vec::with_capacity(key_desc.encoded_len());
        key_desc
            .encode(&mut buf)
            .map_err(InitializationError::PrivateKeyEncodingFailed)?;
        buf
    };

    let private_key = Base64Pad.encode(&private_key);

    let api_addr = match profiles[0] {
        Profile::Test => multiaddr!(Ip4([127, 0, 0, 1]), Tcp(0u16)),
        Profile::Default => multiaddr!(Ip4([127, 0, 0, 1]), Tcp(4004u16)),
    };

    let config_contents = CompatibleConfigFile {
        identity: Identity {
            peer_id: peer_id.clone(),
            private_key,
        },
        addresses: Addresses {
            swarm: vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
            api: api_addr,
        },
    };

    let config_path = ipfs_path.join("config");

    let config_file = fs::create_dir_all(&ipfs_path)
        .map_err(InitializationError::DirectoryCreationFailed)
        .and_then(|_| {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&config_path)
                .map_err(InitializationError::ConfigCreationFailed)
        })?;

    let mut writer = BufWriter::new(config_file);

    serde_json::to_writer_pretty(&mut writer, &config_contents)
        .map_err(|e| InitializationError::ConfigWritingFailed(Box::new(e)))?;

    writer
        .flush()
        .map_err(|e| InitializationError::ConfigWritingFailed(Box::new(e)))?;

    Ok(peer_id)
}

/// The facade for the configuration of the API.
pub struct Config {
    /// Keypair for the ipfs node.
    pub keypair: ipfs::Keypair,
    /// Peer addresses for the ipfs node.
    pub swarm: Vec<Multiaddr>,
    /// Address to run the API daemon on.
    pub api_addr: Multiaddr,
}

/// Things which can go wrong when loading a `go-ipfs` compatible configuration file.
#[derive(Error, Debug)]
pub enum LoadingError {
    #[error("failed to open the configuration file: {0}")]
    ConfigurationFileOpening(std::io::Error),
    #[error("failed to read the configuration file: {0}")]
    ConfigurationFileFormat(serde_json::Error),
    #[error("failed to load the private key: {0}")]
    PrivateKeyLoadingFailed(Box<dyn std::error::Error + 'static>),
    #[error("unsupported private key format: {0}")]
    UnsupportedPrivateKeyType(i32),
    #[error("loaded PeerId {loaded:?} is not the same as in configuration file {stored:?}, this is likely a bug in rust-ipfs-http")]
    PeerIdMismatch { loaded: String, stored: String },
}

/// Loads a `go-ipfs` compatible configuration file from the given file.
///
/// Returns only the keypair and listening addresses or [`LoadingError`] but this should be
/// extended to contain the bootstrap nodes at least later when we need to support those for
/// testing purposes.
pub fn load(config: File) -> Result<Config, LoadingError> {
    use std::io::BufReader;

    let config_file: CompatibleConfigFile = serde_json::from_reader(BufReader::new(config))
        .map_err(LoadingError::ConfigurationFileFormat)?;

    let kp = config_file.identity.load_keypair()?;

    let peer_id = kp.public().into_peer_id().to_string();

    if peer_id != config_file.identity.peer_id {
        return Err(LoadingError::PeerIdMismatch {
            loaded: peer_id,
            stored: config_file.identity.peer_id,
        });
    }

    let config = Config {
        keypair: kp,
        swarm: config_file.addresses.swarm,
        api_addr: config_file.addresses.api,
    };

    Ok(config)
}

/// Converts a PEM format to DER where PEM is a container for Base64 data with padding, starting on
/// the first line with a magic 5 dashes, "BEGIN" and the end of line is a tag which is expected to
/// be found in the end, in a separate line with magic 5 dashes, "END" and the tag. DER is the
/// decoded representation of the Base64 data.
///
/// Between the start and end lines there might be some rules on how long lines the base64 encoded
/// bytes are split to, but this function does not make any checks on that.
///
/// Returns the DER bytes (decoded base64) in the first tag delimited part (PEM files could have
/// multiple) regardless of the tag contents, as long as they match.
///
/// ### Panics
///
/// * If the buffer is not valid utf-8 * If the buffer does not start with five dashes and "BEGIN"
/// * If the buffer does not end with five dashes and "END", and the corresponding start tag
///   * Garbage is allowed after this
/// * If the base64 decoding fails for the middle part
///
/// This is used only to get `PKCS#8` from `openssl` crate to DER format expected by `rust-libp2p`
/// and `ring`. The `PKCS#8` pem tag `PRIVATE KEY` is not validated.
fn pem_to_der(bytes: &[u8]) -> Vec<u8> {
    use multibase::Base::Base64Pad;

    // Initially tried this with `pem` crate but it will give back bytes for the ascii, but we need
    // the ascii for multibase's base64pad decoding.
    let mut base64_encoded = String::new();

    let pem = std::str::from_utf8(&bytes).expect("PEM should be utf8");

    // this will hold the end of the line after -----BEGIN
    let mut begin_tag = None;
    let mut found_end_tag = false;

    for line in pem.lines() {
        if begin_tag.is_none() {
            assert!(
                line.starts_with("-----BEGIN"),
                "Unexpected first line in PEM: {}",
                line
            );
            begin_tag = Some(&line[(5 + 5)..]);
            continue;
        }

        if line.starts_with("-----END") {
            let tag = begin_tag.unwrap();

            assert_eq!(tag, &line[(5 + 3)..], "Unexpected ending in PEM: {}", line);
            found_end_tag = true;
            break;
        }

        base64_encoded.push_str(line);
    }
    assert!(
        found_end_tag,
        "Failed to parse PEM, failed to find the end tag"
    );

    Base64Pad
        .decode(base64_encoded)
        .expect("PEM should contain Base64Pad")
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CompatibleConfigFile {
    identity: Identity,
    addresses: Addresses,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Addresses {
    swarm: Vec<Multiaddr>,
    #[serde(rename = "API")]
    api: Multiaddr,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Identity {
    #[serde(rename = "PeerID")]
    peer_id: String,
    #[serde(rename = "PrivKey")]
    private_key: String,
}

impl Identity {
    fn load_keypair(&self) -> Result<ipfs::Keypair, LoadingError> {
        use keys_proto::KeyType;
        use multibase::Base::Base64Pad;
        use prost::Message;

        let bytes = Base64Pad
            .decode(&self.private_key)
            .map_err(|e| LoadingError::PrivateKeyLoadingFailed(Box::new(e)))?;

        let private_key = keys_proto::PrivateKey::decode(bytes.as_slice())
            .map_err(|e| LoadingError::PrivateKeyLoadingFailed(Box::new(e)))?;

        Ok(match KeyType::from_i32(private_key.r#type) {
            Some(KeyType::Rsa) => {
                let pk = openssl::rsa::Rsa::private_key_from_der(&private_key.data)
                    .map_err(|e| LoadingError::PrivateKeyLoadingFailed(Box::new(e)))?;

                let pkcs8 = openssl::pkey::PKey::from_rsa(pk)
                    .and_then(|pk| pk.private_key_to_pem_pkcs8())
                    .map_err(|e| LoadingError::PrivateKeyLoadingFailed(Box::new(e)))?;

                let mut pkcs8 = pem_to_der(&pkcs8);

                ipfs::Keypair::rsa_from_pkcs8(&mut pkcs8)
                    .map_err(|e| LoadingError::PrivateKeyLoadingFailed(Box::new(e)))?
            }
            _keytype => return Err(LoadingError::UnsupportedPrivateKeyType(private_key.r#type)),
        })
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn pem_to_der_helloworld() {
        use super::pem_to_der;
        let input = "-----BEGIN something anything-----
aGVsbG8gd29ybGQ=
-----END something anything-----

garbage in the end is ignored";

        assert_eq!(pem_to_der(input.as_bytes()), b"hello world");
    }

    #[test]
    #[should_panic]
    fn pem_to_der_tag_mismatch() {
        use super::pem_to_der;
        let input = "-----BEGIN something something-----
aGVsbG8gd29ybGQ=
-----END something foobar-----";

        pem_to_der(input.as_bytes());
    }

    #[test]
    fn read_private_key_from_goipfs() {
        use super::Identity;

        // generated with go-ipfs 0.4.23, init --bits 2048
        let input = Identity {
            peer_id: String::from("QmVNXj4TENKBjaUmQQzYMawDXu5LcEEzLyf4K6Ds3WgcF3"),
            private_key: String::from("CAASqQkwggSlAgEAAoIBAQDVwj2MoXUccztSbZarmjQusB+7dZw1ZDycnGlOtLTjsc/Fl7keESwQB+nSXvt3DjV+ftTmK3nPODNVY2c+nooyX3k9svQogSmHxfQIwHkKe11VmrMNTdsYwfswcDq4PgNWrGX8/vUBtfvVb0qzgevBXwc4C9+SDIhRtjiHRNSexc2vFx59tQv03VTfj3sbxdBTwWN+ReeCTyf+7nE3Mg7NdHQ78mysMDFT3w1HDwZ+qt4dpyH5mZRm0anNWQUBtQue7IwzUsHzVCUzm+NeYXJf/miSNw2CCQUfA245+H6zu1F0SJFvTVTKCEmZ7D2ZkseRG73Srm0rdD1jajLiBhUtAgMBAAECggEBALy/mHOuOefWRGKDjBCYyE0Vjd+MeVOX4AF2B3LNFBEeeFWEpJxNE3hQVIJDBo7ZCBlbSwi3CQcWHBXhAVCE04ipTzhQ5VFCw/Y0sEhuFDNSPVcSk9pCjh1tZC0gXGlFsNL+xcvBIXzSQb30WKTrKs6D567wpQikclacrYucFpbee5/wE6GdE9mtrXK69vP5vgAtLQmg0TZljDI78agPwbEUlTVoVxA1JCcroWBfVjuY/xPjBcHUO+8fKsh2P5vqsiwcvbd8Pc4BwqJsct1LbE4sFvHjUvj6XQR3bS38z7XsaqWV9y65s/xgNQdw5zpt+wlRwNjN6+7djPKYRrSZO6ECgYEA6Fdv54Z4Yk7JRwjWSe2aWR3Mz/4o7UM2ILhMhb6DxEpiXfcErbPTqdFdnAuZ3Yp8cEyR2TZB4PYEAxh9zmS+akO1CqG9XaD6ZX76pvM/5p+Kpd6M/wbDNtYFY7tTuLX7J7IXA6vsUaMF4nZxsEp0EvF1wXB29ZiRp4oan7C/FYUCgYEA64ZlbjYb7LSfFGyJl/VaH5ka2Y1L9XWApgY+YphV6e2gCT7kaOKjxve0t0quYQMnpPJKw9MWPSNh2TE9XjJJcpR/EgkEX9/rBMg8VScqyxtItS/voUrW79qCwrHhRR5iY77a9rAZwVkl0EDyIx+cq7ebyK0OCz6891//FBWdnYkCgYBhfxeBU0c/EYqa2VV6zk7fqIainSe1cGfNUSkjUm/etcwTXC3FalmewDGE4sVdVtijEy58tKzuZq4GUoewTUwuMV1OKdLZ8ExCvQcXeanN8BLxSbNm7QKMB0FZuWkHcK4E2VGZA9L16u/0OPm6HXQZ4uMkGjqBEtXENUq4yiVVNQKBgQCzshydU+dGWCCvYogwSl/yj8vuhGGZ64a2JTlf3D5gdo6Nv1BhvdmbKs7UscQN/Gw46yuj8N+c0ewL3AeoYNGs/CNfTUXrKFqVkXiGt5Vs1WpJ40L/WqxW3+64QSNQqvgChlFlucJMxImXNJYJukq8sR/IolB+v+VJEBL77eoNkQKBgQCFQYL064rQZqEBc1dWy2Cucf5eWH5VFBHxCPC5Y6orxpmljYuduAIO0+InoVC+KEAkRPjHU3gFGdBvlDif3x2a8eFsZl//RCd9QdpTToynhl+WNKqQH87kfjsBoFW1L5QYLTbKK558QUp9yR6siKW0viXDbOvB7lK8WaDdYX8lcA=="),
        };

        let peer_id = input
            .load_keypair()
            .unwrap()
            .public()
            .into_peer_id()
            .to_string();

        assert_eq!(peer_id, input.peer_id);
    }
}
