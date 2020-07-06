//! P2P handling for IPFS nodes.
use crate::repo::RepoTypes;
use crate::IpfsOptions;
use bitswap::BitswapEvent;
use core::marker::PhantomData;
use futures::channel::mpsc::{channel, Receiver};
use libp2p::identity::Keypair;
use libp2p::Swarm;
use libp2p::{Multiaddr, PeerId};

mod behaviour;
pub(crate) mod pubsub;
mod swarm;
mod transport;

pub use swarm::Connection;

pub type TSwarm = Swarm<behaviour::Behaviour>;

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
) -> (TSwarm, Receiver<BitswapEvent>) {
    let peer_id = options.peer_id.clone();

    // Set up an encrypted TCP transport over the Mplex protocol.
    let transport = transport::build_transport(options.keypair.clone());

    // HACK: this should instead be achieved with better encapsulation / event propagation.
    let (bitswap_event_sender, bitswap_event_receiver) = channel(1);

    // Create a Kademlia behaviour
    let behaviour = behaviour::build_behaviour(options, bitswap_event_sender).await;

    // Create a Swarm
    let mut swarm = libp2p::Swarm::new(transport, behaviour, peer_id);

    // Listen on all interfaces and whatever port the OS assigns
    let addr = Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
    info!("Listening on {:?}", addr);

    (swarm, bitswap_event_receiver)
}
