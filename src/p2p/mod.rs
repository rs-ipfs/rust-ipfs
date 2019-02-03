//! P2P handling for IPFS nodes.
pub use libp2p::secio::SecioKeyPair;

mod behaviour;
mod transport;

pub type Swarm = libp2p::core::Swarm<transport::TTransport, behaviour::TBehaviour>;

/// Creates a new IPFS swarm.
pub fn create_swarm(
    local_private_key: SecioKeyPair,
) -> Swarm {
    let local_peer_id = local_private_key.to_peer_id();

    // Set up an encrypted TCP transport over the Mplex protocol.
    let transport = transport::build_transport(local_private_key);

    // Create a Kademlia behaviour
    let behaviour = behaviour::build_behaviour(local_peer_id.clone());

    // Create a Swarm
    libp2p::core::Swarm::new(transport, behaviour, local_peer_id)
}
