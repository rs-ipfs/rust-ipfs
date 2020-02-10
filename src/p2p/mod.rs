//! P2P handling for IPFS nodes.
use crate::bitswap::Strategy;
use crate::IpfsOptions;
use crate::repo::{Repo, RepoTypes};
use libp2p::{Multiaddr, PeerId};
use libp2p::Swarm;
use libp2p::identity::Keypair;
use std::marker::PhantomData;

mod behaviour;
mod transport;

pub type TSwarm<SwarmTypes> = Swarm<behaviour::Behaviour<SwarmTypes>>;

pub trait SwarmTypes: RepoTypes + Sized {
    type TStrategy: Strategy<Self>;
}

pub struct SwarmOptions<TSwarmTypes: SwarmTypes> {
    _marker: PhantomData<TSwarmTypes>,
    pub key_pair: Keypair,
    pub peer_id: PeerId,
    pub bootstrap: Vec<(Multiaddr, PeerId)>,
}

impl<TSwarmTypes: SwarmTypes> From<&IpfsOptions<TSwarmTypes>> for SwarmOptions<TSwarmTypes> {
    fn from(options: &IpfsOptions<TSwarmTypes>) -> Self {
        let key_pair = options.config.secio_key_pair();
        let peer_id = key_pair.public().into_peer_id();
        let bootstrap = options.config.bootstrap();
        SwarmOptions {
            _marker: PhantomData,
            key_pair,
            peer_id,
            bootstrap,
        }
    }
}

/// Creates a new IPFS swarm.
pub async fn create_swarm<TSwarmTypes: SwarmTypes>(options: SwarmOptions<TSwarmTypes>, repo: Repo<TSwarmTypes>) -> TSwarm<TSwarmTypes> {
    let peer_id = options.peer_id.clone();

    // Set up an encrypted TCP transport over the Mplex protocol.
    let transport = transport::build_transport(&options);

    // Create a Kademlia behaviour
    let behaviour = behaviour::build_behaviour(options, repo).await;

    // Create a Swarm
    let mut swarm = libp2p::Swarm::new(transport, behaviour, peer_id);

    // Listen on all interfaces and whatever port the OS assigns
    let addr = Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
    info!("Listening on {:?}", addr);

    swarm
}
