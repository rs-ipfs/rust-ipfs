//! P2P handling for IPFS nodes.
use crate::options::IpfsOptions;
use libp2p::Swarm;

mod behaviour;
mod swarm;
mod transport;

pub use behaviour::{Behaviour, BehaviourEvent};
pub use swarm::Connection;

pub type TSwarm = Swarm<Behaviour>;

/// Creates a new IPFS swarm.
pub fn create_swarm(options: &IpfsOptions) -> TSwarm {
    // Set up an encrypted TCP transport over the Mplex protocol.
    let transport = transport::build_transport(options.keypair.clone());

    // Create a Kademlia behaviour
    let behaviour = Behaviour::new(&options);

    // Create a Swarm
    let mut swarm = Swarm::new(transport, behaviour, options.peer_id());

    // Listen on all interfaces and whatever port the OS assigns
    let addr = Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
    info!("Listening on {:?}", addr);

    swarm
}
