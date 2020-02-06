//! Bitswap protocol implementation
pub mod behaviour;
pub mod ledger;
mod bitswap_pb;
pub mod strategy;
pub mod protocol;

pub use self::behaviour::Bitswap;
pub use self::protocol::BitswapError;
pub use self::ledger::Priority;
pub use self::strategy::{AltruisticStrategy, Strategy};

mod bitswap_pb_2 {
    include!(concat!(env!("OUT_DIR"), "/bitswap_pb.rs"));
}