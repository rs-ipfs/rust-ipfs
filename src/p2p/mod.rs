//! P2P handling for IPFS nodes.
use libp2p::core::Swarm;
use libp2p::secio::SecioKeyPair;

mod behaviour;
mod transport;

/// IPFS Service
pub struct Service {
    /// The swarm.
    pub swarm: Swarm<transport::TTransport, behaviour::TBehaviour>
}

impl Service {
    /// Creates a new IPFS Service.
    pub fn new() -> Self {
        // Create a random key for ourselves.
        let local_key = SecioKeyPair::ed25519_generated().unwrap();
        let local_peer_id = local_key.to_peer_id();

        // Set up an encrypted TCP transport over the Mplex protocol.
        let transport = transport::build_transport(local_key);

        // Create a Kademlia behaviour
        let behaviour = behaviour::build_behaviour(local_peer_id.clone());

        // Create a Swarm
        let swarm = Swarm::new(transport, behaviour, local_peer_id);

        Service { swarm }
    }
}
