use crate::block::{Block, Cid};
use crate::bitswap::bitswap_pb;
use crate::error::Error;
use protobuf::Message as ProtobufMessage;
use std::collections::HashMap;
use std::marker::PhantomData;

pub type Priority = i32;

/// The Ledger contains the history of transactions with a peer.
#[derive(Debug)]
pub struct Ledger {
    /// The number of blocks sent to the peer.
    sent_blocks: usize,
    /// The number of blocks received from the peer.
    received_blocks: usize,
    /// The list of wanted blocks sent to the peer.
    sent_want_list: HashMap<Cid, Priority>,
    /// The list of wanted blocks received from the peer.
    received_want_list: HashMap<Cid, Priority>,
}

impl Ledger {
    /// Creates a new `PeerLedger`.
    pub fn new() -> Self {
        Ledger {
            sent_blocks: 0,
            received_blocks: 0,
            sent_want_list: HashMap::new(),
            received_want_list: HashMap::new(),
        }
    }

    pub fn send_block(&mut self, block: Block) -> Message<O> {
        let mut message = Message::new();
        message.add_block(block);
        message
    }

    pub fn want_block(&mut self, cid: &Cid, priority: Priority) -> Message<O> {
        let mut message = Message::new();
        message.want_block(cid, priority);
        message
    }

    pub fn cancel_block(&mut self, cid: &Cid) -> Option<Message<O>> {
        if self.sent_want_list.contains_key(cid) {
            let mut message = Message::new();
            message.cancel_block(cid);
            Some(message)
        } else {
            None
        }
    }

    pub fn update_outgoing_stats(&mut self, message: &Message<O>) {
        self.sent_blocks += message.blocks.len();
        for cid in message.cancel() {
            self.sent_want_list.remove(cid);
        }
        for (cid, priority) in message.want() {
            self.sent_want_list.insert(cid.to_owned(), *priority);
        }
    }

    pub fn update_incoming_stats(&mut self, message: &Message<I>) {
        self.received_blocks += message.blocks.len();
        for cid in message.cancel() {
            self.received_want_list.remove(cid);
        }
        for (cid, priority) in message.want() {
            self.received_want_list.insert(cid.to_owned(), *priority);
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct I;
#[derive(Debug, Clone, PartialEq)]
pub struct O;

/// A bitswap message.
#[derive(Clone, PartialEq)]
pub struct Message<T> {
    /// Message tag
    _phantom_data: PhantomData<T>,
    /// List of wanted blocks.
    want: HashMap<Cid, Priority>,
    /// List of blocks to cancel.
    cancel: Vec<Cid>,
    /// Wheather it is the full list of wanted blocks.
    full: bool,
    /// List of blocks to send.
    blocks: Vec<Block>,
}

impl<T> Message<T> {
    /// Creates a new bitswap message.
    pub fn new() -> Self {
        Message {
            _phantom_data: PhantomData,
            want: HashMap::new(),
            cancel: Vec::new(),
            full: false,
            blocks: Vec::new(),
        }
    }

    /// Returns the list of blocks.
    pub fn blocks(&self) -> &Vec<Block> {
        &self.blocks
    }

    /// Returns the list of wanted blocks.
    pub fn want(&self) -> &HashMap<Cid, Priority> {
        &self.want
    }

    /// Returns the list of cancelled blocks.
    pub fn cancel(&self) -> &Vec<Cid> {
        &self.cancel
    }

    /// Adds a `Block` to the message.
    pub fn add_block(&mut self, block: Block) {
        self.blocks.push(block);
    }

    /// Removes the block from the message.
    #[allow(unused)]
    pub fn remove_block(&mut self, cid: &Cid) {
        self.blocks.drain_filter(|block| block.cid() == cid);
    }

    /// Adds a block to the want list.
    pub fn want_block(&mut self, cid: &Cid, priority: Priority) {
        self.want.insert(cid.to_owned(), priority);
    }

    /// Adds a block to the cancel list.
    pub fn cancel_block(&mut self, cid: &Cid) {
        self.cancel.push(cid.to_owned());
    }

    /// Removes the block from the want list.
    #[allow(unused)]
    pub fn remove_want_block(&mut self, cid: &Cid) {
        self.want.remove(cid);
    }
}

impl Message<O> {
    /// Turns this `Message` into a message that can be sent to a substream.
    pub fn into_bytes(&self) -> Vec<u8> {
        let mut proto = bitswap_pb::Message::new();
        let mut wantlist = bitswap_pb::Message_Wantlist::new();
        for (cid, priority) in self.want() {
            let mut entry = bitswap_pb::Message_Wantlist_Entry::new();
            entry.set_block(cid.to_bytes());
            entry.set_priority(*priority as _);
            wantlist.mut_entries().push(entry);
        }
        for cid in self.cancel() {
            let mut entry = bitswap_pb::Message_Wantlist_Entry::new();
            entry.set_block(cid.to_bytes());
            entry.set_cancel(true);
            wantlist.mut_entries().push(entry);
        }
        proto.set_wantlist(wantlist);
        for block in self.blocks() {
            let mut payload = bitswap_pb::Message_Block::new();
            payload.set_prefix(block.cid().prefix().as_bytes());
            payload.set_data(block.data().to_vec());
            proto.mut_payload().push(payload);
        }
        proto
            .write_to_bytes()
            .expect("there is no situation in which the protobuf message can be invalid")
    }

}

impl Message<I> {
    /// Creates a `Message` from bytes that were received from a substream.
    pub fn from_bytes(bytes: &Vec<u8>) -> Result<Self, Error> {
        let proto: bitswap_pb::Message = protobuf::parse_from_bytes(bytes)?;
        let mut message = Message::new();
        for entry in proto.get_wantlist().get_entries() {
            let cid = Cid::from(entry.get_block())?;
            if entry.get_cancel() {
                message.cancel_block(&cid);
            } else {
                message.want_block(&cid, entry.get_priority() as _);
            }
        }
        for payload in proto.get_payload() {
            let prefix = cid::Prefix::new_from_bytes(payload.get_prefix())?;
            let cid = cid::Cid::new_from_prefix(&prefix, payload.get_data());
            let block = Block::new(payload.get_data().to_vec(), cid);
            message.add_block(block);
        }
        Ok(message)
    }
}

impl<T> std::fmt::Debug for Message<T> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
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

#[cfg(test)]
mod tests {
    /*
    use super::*;

    #[test]
    fn test_empty_message_to_from_bytes() {
        let message = Message::new();
        let bytes = message.clone().into_bytes();
        let new_message = Message::from_bytes(&bytes).unwrap();
        assert_eq!(message, new_message);
    }

    #[test]
    fn test_want_message_to_from_bytes() {
        let mut message = Message::new();
        let block = Block::from("hello world");
        message.want_block(&block.cid(), 1);
        let bytes = message.clone().into_bytes();
        let new_message = Message::from_bytes(&bytes).unwrap();
        assert_eq!(message, new_message);
    }

    #[test]
    fn test_cancel_message_to_from_bytes() {
        let mut message = Message::new();
        let block = Block::from("hello world");
        message.cancel_block(&block.cid());
        let bytes = message.clone().into_bytes();
        let new_message = Message::from_bytes(&bytes).unwrap();
        assert_eq!(message, new_message);
    }

    #[test]
    fn test_payload_message_to_from_bytes() {
        let mut message = Message::new();
        let block = Block::from("hello world");
        message.add_block(block);
        let bytes = message.clone().into_bytes();
        let new_message = Message::from_bytes(&bytes).unwrap();
        assert_eq!(message, new_message);
    }

    #[test]
    fn test_ledger_send_block() {
        let block_1 = Block::from("1");
        let block_2 = Block::from("2");
        let mut ledger = Ledger::new();
        ledger.add_block(block_1);
        ledger.add_block(block_2);
        ledger.send_message().unwrap();
        assert_eq!(ledger.sent_blocks, 2);
    }

    #[test]
    fn test_ledger_remove_block() {
        let block_1 = Block::from("1");
        let block_2 = Block::from("2");
        let mut ledger = Ledger::new();
        ledger.add_block(block_1.clone());
        ledger.add_block(block_2);
        ledger.remove_block(&block_1.cid());
        ledger.send_message().unwrap();
        assert_eq!(ledger.sent_blocks, 1);
    }

    #[test]
    fn test_ledger_send_want() {
        let block_1 = Block::from("1");
        let block_2 = Block::from("2");
        let mut ledger = Ledger::new();
        ledger.want_block(&block_1.cid(), 1);
        ledger.want_block(&block_2.cid(), 1);
        ledger.cancel_block(&block_1.cid());
        ledger.send_message().unwrap();
        let mut want_list = HashMap::new();
        want_list.insert(block_2.cid(), 1);
        assert_eq!(ledger.sent_want_list, want_list);
    }

    #[test]
    fn test_ledger_send_cancel() {
        let block_1 = Block::from("1");
        let block_2 = Block::from("2");
        let mut ledger = Ledger::new();
        ledger.want_block(&block_1.cid(), 1);
        ledger.want_block(&block_2.cid(), 1);
        ledger.send_message().unwrap();
        ledger.cancel_block(&block_1.cid());
        ledger.send_message().unwrap();
        let mut want_list = HashMap::new();
        want_list.insert(block_2.cid(), 1);
        assert_eq!(ledger.sent_want_list, want_list);
    }

    #[test]
    fn test_ledger_receive() {
        let block_1 = Block::from("1");
        let block_2 = Block::from("2");
        let mut message = Message::new();
        message.add_block(block_1);
        message.want_block(&block_2.cid(), 1);

        let mut ledger = Ledger::new();
        ledger.receive_message(&message);

        assert_eq!(ledger.received_blocks, 1);
        let mut want_list = HashMap::new();
        want_list.insert(block_2.cid(), 1);
        assert_eq!(ledger.received_want_list, want_list);

        let mut message = Message::new();
        message.cancel_block(&block_2.cid());
        ledger.receive_message(&message);
        assert_eq!(ledger.received_want_list, HashMap::new());
    }
    */
}
