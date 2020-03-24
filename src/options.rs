use crate::config::ConfigFile;
use crate::error::Error;
use crate::repo::{BlockStore, DataStore};
use async_std::path::PathBuf;
use libp2p::core::{Multiaddr, PeerId};
use libp2p::identity::Keypair;
use std::borrow::Borrow;
use std::fmt;

/// All types can be changed at compile time by implementing
/// `IpfsTypes`.
pub trait IpfsTypes: Clone + 'static {
    type TBlockStore: BlockStore;
    type TDataStore: DataStore;
}

/// Default IPFS types.
#[derive(Clone, Debug)]
pub struct Types;
impl IpfsTypes for Types {
    type TBlockStore = crate::repo::fs::FsBlockStore;
    #[cfg(feature = "rocksdb")]
    type TDataStore = crate::repo::fs::RocksDataStore;
    #[cfg(not(feature = "rocksdb"))]
    type TDataStore = crate::repo::mem::MemDataStore;
}

/// Testing IPFS types
#[derive(Clone, Debug)]
pub struct TestTypes;
impl IpfsTypes for TestTypes {
    type TBlockStore = crate::repo::mem::MemBlockStore;
    type TDataStore = crate::repo::mem::MemDataStore;
}

/// Ipfs options
#[derive(Clone)]
pub struct IpfsOptions {
    /// The path of the ipfs repo.
    pub ipfs_path: PathBuf,
    /// The keypair used with libp2p.
    pub keypair: Keypair,
    /// Nodes dialed during startup.
    pub bootstrap: Vec<(Multiaddr, PeerId)>,
    /// Enables mdns for peer discovery when true.
    pub mdns: bool,
}

impl fmt::Debug for IpfsOptions {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        // needed since libp2p::identity::Keypair does not have a Debug impl, and the IpfsOptions
        // is a struct with all public fields, so don't enforce users to use this wrapper.
        fmt.debug_struct("IpfsOptions")
            .field("ipfs_path", &self.ipfs_path)
            .field("bootstrap", &self.bootstrap)
            .field("keypair", &DebuggableKeypair(&self.keypair))
            .field("mdns", &self.mdns)
            .finish()
    }
}

/// Workaround for libp2p::identity::Keypair missing a Debug impl, works with references and owned
/// keypairs.
#[derive(Clone)]
pub(crate) struct DebuggableKeypair<I: Borrow<Keypair>>(pub I);

impl<I: Borrow<Keypair>> fmt::Debug for DebuggableKeypair<I> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self.as_ref() {
            Keypair::Ed25519(_) => "Ed25519",
            Keypair::Rsa(_) => "Rsa",
            Keypair::Secp256k1(_) => "Secp256k1",
        };

        write!(fmt, "Keypair::{}", kind)
    }
}

impl<I: Borrow<Keypair>> AsRef<Keypair> for DebuggableKeypair<I> {
    fn as_ref(&self) -> &Keypair {
        self.0.borrow()
    }
}

impl IpfsOptions {
    /// Creates a new options struct.
    pub fn new(
        ipfs_path: PathBuf,
        keypair: Keypair,
        bootstrap: Vec<(Multiaddr, PeerId)>,
        mdns: bool,
    ) -> Self {
        Self {
            ipfs_path,
            keypair,
            bootstrap,
            mdns,
        }
    }

    /// Creates an inmemory store backed node for tests
    pub fn inmemory_with_generated_keys(mdns: bool) -> Self {
        Self::new(
            std::env::temp_dir().into(),
            Keypair::generate_ed25519(),
            vec![],
            mdns,
        )
    }

    /// Create `IpfsOptions` from environment.
    pub fn from_env() -> Result<Self, Error> {
        let ipfs_path = if let Ok(path) = std::env::var("IPFS_PATH") {
            PathBuf::from(path)
        } else {
            let root = if let Some(home) = dirs::home_dir() {
                home
            } else {
                std::env::current_dir().unwrap()
            };
            root.join(".rust-ipfs").into()
        };
        let config_path = dirs::config_dir()
            .unwrap()
            .join("rust-ipfs")
            .join("config.json");
        let config = ConfigFile::new(config_path)?;
        let keypair = config.secio_key_pair();
        let bootstrap = config.bootstrap();

        Ok(IpfsOptions {
            ipfs_path,
            keypair,
            bootstrap,
            mdns: true,
        })
    }

    /// Returns the peer id.
    pub fn peer_id(&self) -> PeerId {
        self.keypair.public().into_peer_id()
    }
}
