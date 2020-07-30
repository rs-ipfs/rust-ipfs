//! Bitswap protocol implementation
#[macro_use]
extern crate tracing;

mod behaviour;
mod block;
mod error;
mod ledger;
mod prefix;
mod protocol;

pub use self::behaviour::{Bitswap, BitswapEvent, Stats};
pub use self::block::Block;
pub use self::error::BitswapError;
pub use self::ledger::Priority;

mod bitswap_pb {
    include!(concat!(env!("OUT_DIR"), "/bitswap_pb.rs"));
}
