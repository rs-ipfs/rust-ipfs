//! P2P handling for IPFS nodes.
use crate::repo::RepoTypes;
use crate::Ipfs;
use crate::IpfsOptions;
use core::marker::PhantomData;
use libp2p::identity::Keypair;
use libp2p::Swarm;
use libp2p::{Multiaddr, PeerId};

mod behaviour;
pub(crate) mod pubsub;
mod swarm;
mod transport;

pub use swarm::Connection;

pub type TSwarm<T> = Swarm<behaviour::Behaviour<T>>;

pub trait SwarmTypes: RepoTypes + Sized {}

pub struct SwarmOptions<TSwarmTypes: SwarmTypes> {
    _marker: PhantomData<TSwarmTypes>,
    pub keypair: Keypair,
    pub peer_id: PeerId,
    pub bootstrap: Vec<(Multiaddr, PeerId)>,
    pub mdns: bool,
}

impl<TSwarmTypes: SwarmTypes> From<&IpfsOptions<TSwarmTypes>> for SwarmOptions<TSwarmTypes> {
    fn from(options: &IpfsOptions<TSwarmTypes>) -> Self {
        let keypair = options.keypair.clone();
        let peer_id = keypair.public().into_peer_id();
        let bootstrap = options.bootstrap.clone();
        let mdns = options.mdns;
        SwarmOptions {
            _marker: PhantomData,
            keypair,
            peer_id,
            bootstrap,
            mdns,
        }
    }
}

/// Creates a new IPFS swarm.
pub async fn create_swarm<TSwarmTypes: SwarmTypes>(
    options: SwarmOptions<TSwarmTypes>,
    ipfs: Ipfs<TSwarmTypes>,
) -> TSwarm<TSwarmTypes> {
    let peer_id = options.peer_id.clone();

    // Set up an encrypted TCP transport over the Mplex protocol.
    let transport = transport::build_transport(options.keypair.clone());

    // Create a Kademlia behaviour
    let behaviour = behaviour::build_behaviour(options, ipfs).await;

    // Create a Swarm
    let mut swarm = libp2p::Swarm::new(transport, behaviour, peer_id);

    // Listen on all interfaces and whatever port the OS assigns
    let addr = Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
    info!("Listening on {:?}", addr);

    swarm
}
