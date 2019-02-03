//! Bitswap protocol implementation
use crate::block::Cid;
use crate::p2p::{create_swarm, SecioKeyPair, Swarm};
use futures::prelude::*;
use parity_multihash::Multihash;
use std::io::Error;

//mod behaviour;
mod ledger;
mod protobuf_structs;
pub mod strategy;
mod protocol;

use self::ledger::Ledger;
pub use self::strategy::{AltruisticStrategy, Strategy};

pub struct Bitswap<S: Strategy> {
    swarm: Swarm,
    ledger: Ledger,
    _strategy: S,
}

impl<S: Strategy> Bitswap<S> {
    pub fn new(local_private_key: SecioKeyPair, strategy: S) -> Self {
        Bitswap {
            swarm: create_swarm(local_private_key),
            ledger: Ledger::new(),
            _strategy: strategy,
        }
    }

    pub fn want_block(&mut self, cid: Cid) {
        let hash = Multihash::from_bytes(cid.hash.clone()).unwrap();
        self.swarm.get_providers(hash);

        self.ledger.want_block(cid, 1);
    }

    pub fn provide_block(&mut self, cid: &Cid) {
        let hash = Multihash::from_bytes(cid.hash.clone()).unwrap();
        self.swarm.add_providing(hash);
    }

    pub fn stop_providing_block(&mut self, cid: &Cid) {
        let hash = Multihash::from_bytes(cid.hash.clone()).unwrap();
        self.swarm.remove_providing(&hash);
    }
}

impl<S: Strategy> Stream for Bitswap<S> {
    type Item = ();
    type Error = Error;

    // TODO: hookup ledger and strategy properly
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        println!("polling bitswap");
        let peer_id = Swarm::local_peer_id(&self.swarm);
        self.ledger.peer_connected(peer_id.clone());
        self.ledger.peer_disconnected(&peer_id);
        self.ledger.send_messages();
        self.ledger.receive_message(peer_id, Vec::new());
        loop {
            match self.swarm.poll().expect("Error while polling swarm") {
                    Async::Ready(Some(event)) => {
                        println!("Result: {:#?}", event);
                        return Ok(Async::Ready(Some(())));
                    },
                    Async::Ready(None) | Async::NotReady => break,
                }
        }

        Ok(Async::NotReady)
    }
}
