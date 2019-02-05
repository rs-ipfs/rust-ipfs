//! P2P handling for IPFS nodes.
use crate::bitswap::Strategy;
use crate::config::NetworkConfig;

mod behaviour;
mod transport;

pub type Swarm<TStrategy> = libp2p::core::Swarm<transport::TTransport, behaviour::TBehaviour<TStrategy>>;

/// Creates a new IPFS swarm.
pub fn create_swarm<TStrategy: Strategy>(config: NetworkConfig<TStrategy>) -> Swarm<TStrategy> {
    let peer_id = config.peer_id.clone();

    // Set up an encrypted TCP transport over the Mplex protocol.
    let transport = transport::build_transport(&config);

    // Create a Kademlia behaviour
    let behaviour = behaviour::build_behaviour(config);

    // Create a Swarm
    libp2p::core::Swarm::new(transport, behaviour, peer_id)
}
