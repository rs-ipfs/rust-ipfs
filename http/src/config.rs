//! go-ipfs compatible configuration file handling or at least setup.

use libp2p_core::identity::{ed25519, Keypair};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
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
    #[error("unsupported profiles selected: {0:?}")]
    InvalidProfiles(Vec<String>),
    #[error("key generation failed: {0}")]
    KeyGeneration(Box<dyn std::error::Error + 'static>),
    #[error("key encoding failed: {0}")]
    PrivateKeyEncodingFailed(prost::EncodeError),
    #[error("config serialization failed: {0}")]
    ConfigWritingFailed(serde_json::Error),
}

/// Creates the IPFS_PATH directory structure and creates a new compatible configuration file
pub fn initialize(ipfs_path: &Path, profiles: Vec<String>) -> Result<(), InitializationError> {
    let config_path = ipfs_path.join("config");

    fs::create_dir_all(&ipfs_path)
        .map_err(InitializationError::DirectoryCreationFailed)
        .and_then(|_| {
            fs::File::create(&config_path).map_err(InitializationError::ConfigCreationFailed)
        })
        .and_then(|config_file| create(config_file, profiles))
}

fn create(config: File, profiles: Vec<String>) -> Result<(), InitializationError> {
    use multibase::Base::Base64Pad;
    use prost::Message;
    use std::io::BufWriter;

    if profiles.len() != 1 || profiles[0] != "test" {
        // profiles are expected to be (comma separated) "test" as there are no bootstrap peer
        // handling yet. the conformance test cases seem to init `go-ipfs` in this profile where
        // it does not have any bootstrap nodes, and multi node tests later call swarm apis to
        // dial the nodes together.
        return Err(InitializationError::InvalidProfiles(profiles));
    }

    let kp = ed25519::Keypair::generate();
    let peer_id = Keypair::Ed25519(kp.clone())
        .public()
        .into_peer_id()
        .to_string();

    let key_desc = keys_proto::PrivateKey {
        r#type: keys_proto::KeyType::Ed25519 as i32,
        data: kp.encode().to_vec(),
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
    use std::io::BufReader;

    let CompatibleConfigFile { identity } = serde_json::from_reader(BufReader::new(config))
        .map_err(LoadingError::ConfigurationFileFormat)?;

    let kp = identity.load_keypair()?;

    let peer_id = kp.public().into_peer_id().to_string();

    if peer_id != identity.peer_id {
        return Err(LoadingError::PeerIdMismatch {
            loaded: peer_id,
            stored: identity.peer_id,
        });
    }

    Ok(kp)
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

impl Identity {
    fn load_keypair(&self) -> Result<ipfs::Keypair, LoadingError> {
        use keys_proto::KeyType;
        use multibase::Base::Base64Pad;
        use prost::Message;

        let bytes = Base64Pad
            .decode(&self.private_key)
            .map_err(|e| LoadingError::PrivateKeyLoadingFailed(Box::new(e)))?;

        let mut private_key = keys_proto::PrivateKey::decode(bytes.as_slice())
            .map_err(|e| LoadingError::PrivateKeyLoadingFailed(Box::new(e)))?;

        Ok(match KeyType::from_i32(private_key.r#type) {
            Some(KeyType::Ed25519) => {
                let kp = ed25519::Keypair::decode(&mut private_key.data)
                    .map_err(|e| LoadingError::PrivateKeyLoadingFailed(Box::new(e)))?;
                Keypair::Ed25519(kp)
            }
            _keytype => return Err(LoadingError::UnsupportedPrivateKeyType(private_key.r#type)),
        })
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn read_private_key_from_goipfs() {
        use super::Identity;

        // generated with go-ipfs 0.7.0-rc2, init
        let input = Identity {
            peer_id: String::from("12D3KooWAPm5eb4mqMAW6ZqC4f2dmWdNw3XeezqoaebdTL8byqMg"),
            private_key: String::from("CAESQJw7A9j4lW53GtGEEl7CuKHCwpf5LeyAhsvYDfuh+FhqCI4S09NZYclzYM7fwQC4us0s8VcSBkWqRt1SPRaHI28="),
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
