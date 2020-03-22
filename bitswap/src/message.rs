use crate::block::Block;
use crate::error::BitswapError;
use core::convert::TryFrom;
use libipld::cid::{Cid, Prefix};
use prost::Message;
use std::collections::{HashMap, HashSet};

mod bitswap_pb {
    include!(concat!(env!("OUT_DIR"), "/bitswap_pb.rs"));
}

/// Priority of a wanted block.
pub type Priority = i32;

/// A bitswap message.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct BitswapMessage {
    /// Wanted blocks.
    want: HashMap<Cid, Priority>,
    /// Blocks to cancel.
    cancel: HashSet<Cid>,
    /// Wheather it is the full list of wanted blocks.
    full: bool,
    /// List of blocks to send.
    blocks: Vec<Block>,
}

impl core::fmt::Debug for BitswapMessage {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::fmt::Result {
        for (cid, priority) in self.want() {
            writeln!(fmt, "want: {} {}", cid.to_string(), priority)?;
        }
        for cid in self.cancel() {
            writeln!(fmt, "cancel: {}", cid.to_string())?;
        }
        for block in self.blocks() {
            writeln!(fmt, "block: {}", block.cid().to_string())?;
        }
        Ok(())
    }
}

impl BitswapMessage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Is message empty.
    pub fn is_empty(&self) -> bool {
        self.want.is_empty() && self.cancel.is_empty() && self.blocks.is_empty()
    }

    /// Returns the list of blocks.
    pub fn blocks(&self) -> &[Block] {
        &self.blocks
    }

    /// Returns the list of wanted blocks.
    pub fn want(&self) -> impl Iterator<Item = (&Cid, &Priority)> {
        self.want.iter()
    }

    /// Returns the list of cancelled blocks.
    pub fn cancel(&self) -> impl Iterator<Item = &Cid> {
        self.cancel.iter()
    }

    /// Adds a `Block` to the message.
    pub fn add_block(&mut self, block: Block) {
        self.blocks.push(block);
    }

    /// Removes the block from the message.
    pub fn remove_block(&mut self, cid: &Cid) {
        self.blocks.retain(|block| block.cid() != cid);
    }

    /// Adds a block to the want list.
    pub fn want_block(&mut self, cid: &Cid, priority: Priority) {
        self.cancel.remove(cid);
        self.want.insert(cid.clone(), priority);
    }

    /// Adds a block to the cancel list.
    pub fn cancel_block(&mut self, cid: &Cid) {
        if self.want.contains_key(cid) {
            self.want.remove(cid);
        } else {
            self.cancel.insert(cid.clone());
        }
    }

    /// Turns this `Message` into a message that can be sent to a substream.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut proto = bitswap_pb::Message::default();
        let mut wantlist = bitswap_pb::message::Wantlist::default();
        for (cid, priority) in self.want() {
            let mut entry = bitswap_pb::message::wantlist::Entry::default();
            entry.block = cid.to_bytes();
            entry.priority = *priority;
            wantlist.entries.push(entry);
        }
        for cid in self.cancel() {
            let mut entry = bitswap_pb::message::wantlist::Entry::default();
            entry.block = cid.to_bytes();
            entry.cancel = true;
            wantlist.entries.push(entry);
        }
        for block in self.blocks() {
            let mut payload = bitswap_pb::message::Block::default();
            payload.prefix = block.cid().prefix().as_bytes();
            payload.data = block.data().to_vec();
            proto.payload.push(payload);
        }
        if !wantlist.entries.is_empty() {
            proto.wantlist = Some(wantlist);
        }
        let mut res = Vec::with_capacity(proto.encoded_len());
        proto
            .encode(&mut res)
            .expect("there is no situation in which the protobuf message can be invalid");
        res
    }

    /// Creates a `Message` from bytes that were received from a substream.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, BitswapError> {
        Self::try_from(bytes)
    }
}

impl TryFrom<&[u8]> for BitswapMessage {
    type Error = BitswapError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let proto: bitswap_pb::Message = bitswap_pb::Message::decode(bytes)?;
        let mut message = Self::new();
        for entry in proto.wantlist.unwrap_or_default().entries {
            let cid = Cid::try_from(entry.block)?;
            if entry.cancel {
                message.cancel_block(&cid);
            } else {
                message.want_block(&cid, entry.priority);
            }
        }
        for payload in proto.payload {
            let prefix = Prefix::new_from_bytes(&payload.prefix)?;
            let cid = Cid::new_from_prefix(&prefix, &payload.data);
            let block = Block {
                cid,
                data: payload.data.to_vec().into_boxed_slice(),
            };
            message.add_block(block);
        }
        Ok(message)
    }
}

impl From<()> for BitswapMessage {
    fn from(_: ()) -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::tests::create_block;

    #[test]
    fn test_empty_message_to_from_bytes() {
        let message = BitswapMessage::new();
        let bytes = message.to_bytes();
        let new_message = BitswapMessage::from_bytes(&bytes).unwrap();
        assert_eq!(message, new_message);
    }

    #[test]
    fn test_want_message_to_from_bytes() {
        let mut message = BitswapMessage::new();
        let block = create_block(b"hello world");
        message.want_block(&block.cid(), 1);
        let bytes = message.to_bytes();
        let new_message = BitswapMessage::from_bytes(&bytes).unwrap();
        assert_eq!(message, new_message);
    }

    #[test]
    fn test_cancel_message_to_from_bytes() {
        let mut message = BitswapMessage::new();
        let block = create_block(b"hello world");
        message.cancel_block(&block.cid());
        let bytes = message.to_bytes();
        let new_message = BitswapMessage::from_bytes(&bytes).unwrap();
        assert_eq!(message, new_message);
    }

    #[test]
    fn test_payload_message_to_from_bytes() {
        let mut message = BitswapMessage::new();
        let block = create_block(b"hello world");
        message.add_block(block);
        let bytes = message.to_bytes();
        let new_message = BitswapMessage::from_bytes(&bytes).unwrap();
        assert_eq!(message, new_message);
    }
}
