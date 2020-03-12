//! go-ipfs compatible configuration file handling or at least setup.

use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::num::NonZeroU16;
use std::path::Path;
use thiserror::Error;

/// Temporary module required to de/ser config files base64'd protobuf rsa private key format.
/// Temporary until the private key import/export can be PR'd into rust-libp2p.
mod keys_proto {
    include!(concat!(env!("OUT_DIR"), "/keys_proto.rs"));
}

/// The way things can go wrong when calling [`initialize`].
#[derive(Error, Debug)]
pub enum InitializationError {
    #[error("repository creation failed: {0}")]
    DirectoryCreationFailed(std::io::Error),
    #[error("configuration file creation failed: {0}")]
    ConfigCreationFailed(std::io::Error),
    #[error("invalid RSA key length given: {0}")]
    InvalidRsaKeyLength(u16),
    #[error("unsupported profiles selected: {0:?}")]
    InvalidProfiles(Vec<String>),
    #[error("key generation failed: {0}")]
    KeyGeneration(Box<dyn std::error::Error + 'static>),
    #[error("key encoding failed: {0}")]
    PrivateKeyEncodingFailed(prost::EncodeError),
    #[error("config serialization failed: {0}")]
    ConfigWritingFailed(serde_json::Error),
}

/// Creates the IPFS_PATH directory structure and creates a new compatible configuration file with
/// RSA key of length `bits`.
pub fn initialize(
    ipfs_path: &Path,
    bits: NonZeroU16,
    profiles: Vec<String>,
) -> Result<(), InitializationError> {
    let config_path = ipfs_path.join("config");

    fs::create_dir_all(&ipfs_path)
        .map_err(InitializationError::DirectoryCreationFailed)
        .and_then(|_| {
            fs::File::create(&config_path).map_err(InitializationError::ConfigCreationFailed)
        })
        .and_then(|config_file| create(config_file, bits, profiles))
}

fn create(
    config: File,
    bits: NonZeroU16,
    profiles: Vec<String>,
) -> Result<(), InitializationError> {
    use multibase::Base::Base64Pad;
    use prost::Message;
    use std::io::BufWriter;

    let bits = bits.get();

    if bits < 2048 || bits > 16 * 1024 {
        // ring will not accept a less than 2048 key
        return Err(InitializationError::InvalidRsaKeyLength(bits));
    }

    if profiles.len() != 1 || profiles[0] != "test" {
        return Err(InitializationError::InvalidProfiles(profiles));
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

    let config_contents = CompatibleConfigFile {
        identity: Identity {
            peer_id,
            private_key,
        },
    };

    serde_json::to_writer_pretty(BufWriter::new(config), &config_contents)
        .map_err(InitializationError::ConfigWritingFailed)?;

    Ok(())
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
/// Returns only the [`ipfs::KeyPair`] or [`LoadingError`] but this should be extended to contain
/// the bootstrap nodes at least later when we need to support those for testing purposes.
pub fn load(config: File) -> Result<ipfs::Keypair, LoadingError> {
    use keys_proto::KeyType;
    use multibase::Base::Base64Pad;
    use prost::Message;
    use std::io::BufReader;

    let CompatibleConfigFile { identity } = serde_json::from_reader(BufReader::new(config))
        .map_err(LoadingError::ConfigurationFileFormat)?;

    let bytes = Base64Pad
        .decode(identity.private_key)
        .map_err(|e| LoadingError::PrivateKeyLoadingFailed(Box::new(e)))?;

    let private_key = keys_proto::PrivateKey::decode(bytes.as_slice())
        .map_err(|e| LoadingError::PrivateKeyLoadingFailed(Box::new(e)))?;

    let kp = match KeyType::from_i32(private_key.r#type) {
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
    };

    let peer_id = kp.public().into_peer_id().to_string();

    if peer_id != identity.peer_id {
        return Err(LoadingError::PeerIdMismatch {
            loaded: peer_id,
            stored: identity.peer_id,
        });
    }

    Ok(kp)
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
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Identity {
    #[serde(rename = "PeerID")]
    peer_id: String,
    #[serde(rename = "PrivKey")]
    private_key: String,
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
}
