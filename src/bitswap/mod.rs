//! Bitswap protocol implementation
use cid::Cid;
use crate::p2p::Service;
use futures::prelude::*;
use parity_multihash::Multihash;
use std::io::Error;
use std::sync::{Arc, Mutex};

mod protobuf_structs;

#[derive(Clone)]
pub struct Bitswap {
    p2p: Arc<Mutex<Service>>,
}

impl Bitswap {
    pub fn new() -> Self {
        Bitswap {
            p2p: Arc::new(Mutex::new(Service::new())),
        }
    }

    pub fn get_block(&mut self, cid: Arc<Cid>) {
        println!("retriving block");
        let hash = Multihash::from_bytes(cid.hash.clone()).unwrap();
        self.p2p.lock().unwrap().swarm.get_providers(hash);
    }
}

impl Stream for Bitswap {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        println!("polling bitswap");
        loop {
            match self.p2p.lock().unwrap()
                .swarm.poll().expect("Error while polling swarm") {
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
