//! Bitswap protocol implementation
#[macro_use]
extern crate log;

mod behaviour;
mod block;
mod error;
mod ledger;
mod prefix;
mod protocol;

pub use self::behaviour::{Bitswap, BitswapEvent};
pub use self::block::Block;
pub use self::error::BitswapError;
pub use self::ledger::{Priority, Stats};

mod bitswap_pb {
    include!(concat!(env!("OUT_DIR"), "/bitswap_pb.rs"));
}
