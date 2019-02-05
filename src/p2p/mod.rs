//! P2P handling for IPFS nodes.
use crate::config::NetworkConfig;

mod behaviour;
mod transport;

pub type Swarm = libp2p::core::Swarm<transport::TTransport, behaviour::TBehaviour>;

/// Creates a new IPFS swarm.
pub fn create_swarm(config: NetworkConfig) -> Swarm {
    // Set up an encrypted TCP transport over the Mplex protocol.
    let transport = transport::build_transport(&config);

    // Create a Kademlia behaviour
    let behaviour = behaviour::build_behaviour(&config);

    // Create a Swarm
    libp2p::core::Swarm::new(transport, behaviour, config.peer_id)
}
