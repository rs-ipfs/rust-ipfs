//! P2P handling for IPFS nodes.
use crate::bitswap::Strategy;
use crate::config::ConfigFile;
use crate::repo::RepoTypes;
use libp2p::{Multiaddr, PeerId};
use libp2p::core::Swarm;
use libp2p::secio::SecioKeyPair;
use std::marker::PhantomData;

mod behaviour;
mod transport;

pub type TSwarm<SwarmTypes> = Swarm<transport::TTransport, behaviour::TBehaviour<SwarmTypes>>;

pub trait SwarmTypes: RepoTypes + Sized {
    type TStrategy: Strategy<Self>;
}

pub struct SwarmOptions<TSwarmTypes: SwarmTypes> {
    _marker: PhantomData<TSwarmTypes>,
    pub key_pair: SecioKeyPair,
    pub peer_id: PeerId,
    pub bootstrap: Vec<(Multiaddr, PeerId)>,
}

impl<TSwarmTypes: SwarmTypes> From<&ConfigFile> for SwarmOptions<TSwarmTypes> {
    fn from(config: &ConfigFile) -> Self {
        let key_pair = config.secio_key_pair();
        let peer_id = key_pair.to_peer_id();
        let bootstrap = config.bootstrap();
        SwarmOptions {
            _marker: PhantomData,
            key_pair,
            peer_id,
            bootstrap,
        }
    }
}

/// Creates a new IPFS swarm.
pub fn create_swarm<TSwarmTypes: SwarmTypes>(options: SwarmOptions<TSwarmTypes>, repo: TSwarmTypes::TRepo) -> TSwarm<TSwarmTypes> {
    let peer_id = options.peer_id.clone();

    // Set up an encrypted TCP transport over the Mplex protocol.
    let transport = transport::build_transport(&options);

    // Create a Kademlia behaviour
    let behaviour = behaviour::build_behaviour(options, repo);

    // Create a Swarm
    let mut swarm = libp2p::core::Swarm::new(transport, behaviour, peer_id);

    // Listen on all interfaces and whatever port the OS assigns
    let addr = Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
    info!("Listening on {:?}", addr);

    swarm
}
