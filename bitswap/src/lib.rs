//! Bitswap protocol implementation
#[macro_use]
extern crate log;

mod behaviour;
mod block;
mod error;
mod ledger;
mod prefix;
mod protocol;
mod strategy;

pub use self::behaviour::Bitswap;
pub use self::block::Block;
pub use self::error::BitswapError;
pub use self::ledger::Priority;
pub use self::strategy::{AltruisticStrategy, BitswapStore, Strategy};

mod bitswap_pb {
    include!(concat!(env!("OUT_DIR"), "/bitswap_pb.rs"));
}
