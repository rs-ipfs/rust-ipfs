#![allow(missing_docs)]
use crate::bitswap::Strategy;
use libp2p::{Multiaddr, PeerId};
use libp2p::multiaddr::Protocol;
use libp2p::secio::SecioKeyPair;
use rand::{Rng, rngs::EntropyRng};
use serde_derive::{Serialize, Deserialize};
use std::fs;
use std::path::Path;

const APP_NAME: &'static str = "rust-ipfs";
const CONFIG_FILE: &'static str = "config.json";

const BOOTSTRAP_NODES: &[&'static str] = &[
    "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
    "/ip4/104.236.179.241/tcp/4001/p2p/QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM",
    "/ip4/104.236.76.40/tcp/4001/p2p/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64",
    "/ip4/128.199.219.111/tcp/4001/p2p/QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu",
    "/ip4/178.62.158.247/tcp/4001/p2p/QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd",
    "/ip6/2400:6180:0:d0::151:6001/tcp/4001/p2p/QmSoLSafTMBsPKadTEgaXctDQVcqN88CNLHXMkTNwMKPnu",
    "/ip6/2604:a880:1:20::203:d001/tcp/4001/p2p/QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KrGM",
    "/ip6/2604:a880:800:10::4a:5001/tcp/4001/p2p/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64",
    "/ip6/2a03:b0c0:0:1010::23:1001/tcp/4001/p2p/QmSoLer265NRgSp2LA3dPaeykiS1J6DifTC88f5uVQKNAd",
];

#[derive(Serialize, Deserialize)]
pub struct Configuration {
    raw_key: [u8; 32],
    bootstrap: Vec<Multiaddr>,
}

impl Configuration {
    pub fn new() -> Self {
        let xdg_dirs = xdg::BaseDirectories::with_prefix(APP_NAME).unwrap();
        let path = xdg_dirs.place_config_file(CONFIG_FILE).unwrap();
        Configuration::from_file(path)
    }

    pub fn secio_key_pair(&self) -> SecioKeyPair {
        SecioKeyPair::ed25519_raw_key(&self.raw_key).unwrap()
    }

    pub fn bootstrap(&self) -> Vec<(Multiaddr, PeerId)> {
        let mut bootstrap = Vec::new();
        for addr in &self.bootstrap {
            let mut addr = addr.to_owned();
            let peer_id = match addr.pop() {
                Some(Protocol::P2p(hash)) => PeerId::from_multihash(hash).unwrap(),
                _ => panic!("No peer id for addr"),
            };
            bootstrap.push((addr, peer_id));
        }
        bootstrap
    }

    pub fn generate() -> Self {
	      let raw_key: [u8; 32] = EntropyRng::new().gen();
        let bootstrap = BOOTSTRAP_NODES.iter().map(|node| {
            node.parse().unwrap()
        }).collect();
        Configuration {
            raw_key,
            bootstrap,
        }
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> Self {
        fs::read_to_string(&path).map(|content| {
            serde_json::from_str(&content).unwrap()
        }).unwrap_or_else(|_| {
            let config = Configuration::generate();
            let string = serde_json::to_string(&config).unwrap();
            fs::write(path, string).unwrap();
            config
        })
    }
}

pub struct NetworkConfig<TStrategy: Strategy> {
    pub key_pair: SecioKeyPair,
    pub peer_id: PeerId,
    pub bootstrap: Vec<(Multiaddr, PeerId)>,
    pub strategy: TStrategy,
}

impl<TStrategy: Strategy> NetworkConfig<TStrategy> {
    pub fn from_config(config: &Configuration, strategy: TStrategy) -> Self {
        let key_pair = config.secio_key_pair();
        let peer_id = key_pair.to_peer_id();
        let bootstrap = config.bootstrap();
        NetworkConfig {
            key_pair,
            peer_id,
            bootstrap,
            strategy,
        }
    }
}
