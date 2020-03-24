//! IPFS node implementation
//#![deny(missing_docs)]

#![cfg_attr(feature = "nightly", feature(external_doc))]
#![cfg_attr(feature = "nightly", doc(include = "../README.md"))]

#[macro_use]
extern crate log;

mod config;
mod daemon;
//mod dag;
mod error;
//mod ipns;
mod options;
mod p2p;
//mod path;
mod registry;
mod repo;
//mod unixfs;

pub use crate::daemon::Ipfs;
pub use crate::error::Error;
pub use crate::options::{IpfsOptions, IpfsTypes, TestTypes, Types};
pub use crate::p2p::Connection;
pub use libipld::cid::Cid;
pub use libp2p::core::{Multiaddr, PeerId, PublicKey};
pub use libp2p::identity::Keypair;
