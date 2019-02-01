//! P2P handling for IPFS nodes.
use libp2p::core::Swarm;
use libp2p::secio::SecioKeyPair;
use tokio::prelude::*;

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

    /// Find IPFS peer using bootstrap nodes.
    pub fn find_peer(&mut self, peer_id: &str) {
        self.swarm.find_node(peer_id.parse().expect("Failed to parse peer id."))
    }
}

/// Run the service.
pub fn run_service(mut service: Service) {
    tokio::run(future::poll_fn(move || -> Result<_, ()> {
        loop {
            match service.swarm.poll().expect("Error while polling swarm") {
                Async::Ready(Some(event)) => {
                    println!("Result: {:#?}", event);
                    return Ok(Async::Ready(()));
                },
                Async::Ready(None) | Async::NotReady => break,
            }
        }

        Ok(Async::NotReady)
    }));
}

/*#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_peer() {
        let mut service = Service::new();
        service.find_peer("QmdiXyMWRbsP8681LjnJG2Qz7maMpomTMaKQmqEy7Ato9x");
        run_service(service);
    }
}
*/
