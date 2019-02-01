//! P2P handling for IPFS nodes.
use libp2p::core::Swarm;
use libp2p::secio::SecioKeyPair;

mod behaviour;
mod topology;
mod transport;

/// IPFS Service
pub struct Service {
    /// The swarm.
    pub swarm: Swarm<transport::TTransport, behaviour::TBehaviour, topology::TTopology>
}

impl Service {
    /// Creates a new IPFS Service.
    pub fn new() -> Self {
        // Create a random key for ourselves.
        let local_key = SecioKeyPair::ed25519_generated().unwrap();
        let local_pub_key = local_key.to_public_key();
        let peer_id = local_pub_key.clone().into_peer_id();

        // Set up an encrypted TCP transport over the Mplex protocol.
        let transport = transport::build_transport(local_key);

        // Create the topology of the network with the IPFS bootstrap nodes.
        let topology = topology::build_topology(local_pub_key);

        // Create a Kademlia behaviour
        let behaviour = behaviour::build_behaviour(peer_id);

        // Create a Swarm
        let swarm = Swarm::new(transport, behaviour, topology);

        Service { swarm }
    }
}
