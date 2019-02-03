use crate::block::{Block, Cid};
use crate::bitswap::protobuf_structs::bitswap as proto;
use libp2p::PeerId;
use protobuf::{ProtobufError, Message as ProtobufMessage};
use std::collections::HashMap;

pub type Priority = u8;

/// The Ledger contains all the state of all transactions.
#[derive(Debug)]
pub struct Ledger {
    peers: HashMap<PeerId, PeerLedger>,
    received_blocks: Vec<Block>,
    received_wants: Vec<(PeerId, Cid, Priority)>,
    wanted_blocks: HashMap<Cid, Priority>,
}

impl Ledger {
    /// Creates a new `Ledger`.
    pub fn new() -> Ledger {
        Ledger {
            peers: HashMap::new(),
            wanted_blocks: HashMap::new(),
            received_blocks: Vec::new(),
            received_wants: Vec::new(),
        }
    }

    /// Creates a new ledger entry for the peer and sends our want list.
    pub fn peer_connected(&mut self, peer_id: PeerId) {
        // TODO: load stats from previous interactions
        let ledger = PeerLedger::from_wanted_blocks(&self.wanted_blocks);
        self.peers.insert(peer_id, ledger);
    }

    /// Removes the ledger of a disconnected peer.
    pub fn peer_disconnected(&mut self, peer_id: &PeerId) {
        // TODO: persist stats for future interactions
        self.peers.remove(peer_id);
    }

    /// Queues the block to be sent to the peer.
    pub fn send_block(&mut self, peer_id: &PeerId, block: Block) {
        let ledger = self.peers.get_mut(peer_id).expect("Peer not in ledger?!");
        ledger.add_block(block);
    }

    /// Adds the block to our want list and updates all peers.
    pub fn want_block(&mut self, cid: Cid, priority: u8) {
        for (_peer_id, ledger) in self.peers.iter_mut() {
            ledger.want_block(&cid, priority);
        }
        self.wanted_blocks.insert(cid, priority);
    }

    /// Removes the block from our want list and updates all peers.
    pub fn cancel_block(&mut self, cid: &Cid) {
        for (_peer_id, ledger) in self.peers.iter_mut() {
            ledger.cancel_block(cid);
        }
        self.wanted_blocks.remove(cid);
    }

    /// Parses and processes the message.
    ///
    /// If a block was received it adds it to the received blocks queue and
    /// cancels the request for the block.
    /// If a want was received it adds it to the received wants queue for
    /// processing through a `Strategy`.
    /// If a want was cancelled it removes it from the received wants queue.
    pub fn receive_message(&mut self, peer_id: &PeerId, bytes: Vec<u8>) {
        let ledger = self.peers.get_mut(peer_id).expect("Peer not in ledger?!");
        let message = match ledger.receive_message(bytes) {
            Ok(message) => message,
            Err(err) => {
                println!("{}", err);
                return;
            }
        };
        for cid in message.cancel() {
            // If a previous block was queued but has not been sent yet, remove it
            // from the queue.
            ledger.remove_block(cid);
            // If the bitswap strategy has not processed the request yet, remove
            // it from the queue.
            self.received_wants.drain_filter(|(peer_id2, cid2, _)| {
                peer_id == peer_id2 && cid == cid2
            });
        }
        // Queue new requests
        for (cid, priority) in message.want() {
            self.received_wants.push((peer_id.to_owned(), cid.to_owned(), *priority));
        }
        for block in message.blocks() {
            // Add block to received blocks
            self.received_blocks.push(block.to_owned());
            // Cancel the block.
            self.cancel_block(&block.cid());
        }
    }

    /// Sends all queued messages.
    pub fn send_messages(&mut self) {
        for (_peer_id, ledger) in self.peers.iter_mut() {
            // TODO
            let _bytes_to_send = ledger.send_message();
        }
    }
}

/// The LedgerEntry contains all the state of all transactions with a peer.
#[derive(Debug)]
pub struct PeerLedger {
    /// The number of blocks sent to the peer.
    sent_blocks: usize,
    /// The number of blocks received from the peer.
    received_blocks: usize,
    /// The list of wanted blocks sent to the peer.
    sent_want_list: HashMap<Cid, Priority>,
    /// The list of wanted blocks received from the peer.
    received_want_list: HashMap<Cid, Priority>,
    /// The next message to send to the peer.
    queued_message: Option<Message>,
}

impl PeerLedger {
    /// Creates a new `PeerLedger`.
    fn new() -> Self {
        PeerLedger {
            sent_blocks: 0,
            received_blocks: 0,
            sent_want_list: HashMap::new(),
            received_want_list: HashMap::new(),
            queued_message: None,
        }
    }

    /// Creates a new `PeerLedger` and queues a message with the wanted blocks.
    fn from_wanted_blocks(wanted_blocks: &HashMap<Cid, Priority>) -> Self {
        let message = if wanted_blocks.is_empty() {
            None
        } else {
            Some(Message::from_wanted(wanted_blocks.to_owned()))
        };
        let mut ledger = PeerLedger::new();
        ledger.queued_message = message;
        ledger
    }

    /// Adds a block to the queued message.
    fn add_block(&mut self, block: Block) {
        if self.queued_message.is_none() {
            self.queued_message = Some(Message::new());
        }
        self.queued_message.as_mut().unwrap().add_block(block);
    }

    /// Removes a block from the queued message.
    fn remove_block(&mut self, cid: &Cid) {
        if self.queued_message.is_none() {
            self.queued_message = Some(Message::new());
        }
        self.queued_message.as_mut().unwrap().remove_block(cid);
    }

    /// Adds a block to the want list.
    fn want_block(&mut self, cid: &Cid, priority: Priority) {
        if self.queued_message.is_none() {
            self.queued_message = Some(Message::new());
        }
        self.queued_message.as_mut().unwrap().want_block(cid, priority);
    }

    /// Removes the block from the want list.
    fn cancel_block(&mut self, cid: &Cid) {
        if self.sent_want_list.contains_key(cid) {
            self.queued_message.as_mut().unwrap().cancel_block(cid);
        } else {
            self.queued_message.as_mut().unwrap().soft_cancel_block(cid);
        }
    }

    /// Finalizes the message and returns it's contents.
    ///
    /// Updates the number of sent blocks and the sent want list entries.
    fn send_message(&mut self) -> Option<Vec<u8>> {
        if self.queued_message.is_none() {
            return None;
        }
        let message = self.queued_message.take().unwrap();
        self.sent_blocks += message.blocks.len();
        for (cid, priority) in message.want() {
            self.sent_want_list.insert(cid.to_owned(), *priority);
        }
        Some(message.into_bytes())
    }

    /// Parses a message.
    ///
    /// Updates the number of received blocks and the received want list entries.
    fn receive_message(&mut self, bytes: Vec<u8>) -> Result<Message, ProtobufError> {
        let message = Message::from_bytes(&bytes)?;
        self.received_blocks += message.blocks.len();
        for cid in message.cancel() {
            self.received_want_list.remove(cid);
        }
        for (cid, priority) in message.want() {
            self.received_want_list.insert(cid.to_owned(), *priority);
        }
        Ok(message)
    }
}

/// A bitswap message.
#[derive(Debug, Clone)]
pub struct Message {
    /// List of wanted blocks.
    want: HashMap<Cid, Priority>,
    /// List of blocks to cancel.
    cancel: Vec<Cid>,
    /// Wheather it is the full list of wanted blocks.
    full: bool,
    /// List of blocks to send.
    blocks: Vec<Block>,
}

impl Message {
    /// Creates a new bitswap message.
    pub fn new() -> Self {
        Message {
            want: HashMap::new(),
            cancel: Vec::new(),
            full: false,
            blocks: Vec::new(),
        }
    }

    /// Creates a new bitswap message from a want list.
    pub fn from_wanted(wanted: HashMap<Cid, Priority>) -> Self {
        Message {
            want: wanted,
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
    pub fn remove_block(&mut self, cid: &Cid) {
        self.blocks.drain_filter(|block| &block.cid() == cid);
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
    pub fn soft_cancel_block(&mut self, cid: &Cid) {
        self.want.remove(cid);
    }

    /// Turns this `Message` into a message that can be sent to a substream.
    pub fn into_bytes(self) -> Vec<u8> {
        let mut proto = proto::Message::new();
        let mut wantlist = proto::Message_Wantlist::new();
        for (cid, priority) in self.want {
            let mut entry = proto::Message_Wantlist_Entry::new();
            entry.set_block(cid.to_bytes());
            entry.set_priority(priority as _);
            wantlist.mut_entries().push(entry);
        }
        for cid in self.cancel {
            let mut entry = proto::Message_Wantlist_Entry::new();
            entry.set_block(cid.to_bytes());
            entry.set_cancel(true);
            wantlist.mut_entries().push(entry);
        }
        proto.set_wantlist(wantlist);
        for block in self.blocks {
            let mut payload = proto::Message_Block::new();
            payload.set_prefix(block.cid().prefix().as_bytes());
            payload.set_data(block.data().to_vec());
            proto.mut_payload().push(payload);
        }
        proto
            .write_to_bytes()
            .expect("there is no situation in which the protobuf message can be invalid")
    }

    /// Creates a `Message` from bytes that were received from a substream.
    pub fn from_bytes(bytes: &Vec<u8>) -> Result<Self, ProtobufError> {
        let proto: proto::Message = protobuf::parse_from_bytes(bytes)?;
        let mut message = Message::new();
        for entry in proto.get_wantlist().get_entries() {
            let cid = std::sync::Arc::new(cid::Cid::from(entry.get_block()).unwrap());
            if entry.get_cancel() {
                message.cancel_block(&cid);
            } else {
                message.want_block(&cid, entry.get_priority() as _);
            }
        }
        for payload in proto.get_payload() {
            let prefix = cid::Prefix::new_from_bytes(payload.get_prefix()).unwrap();
            let cid = cid::Cid::new_from_prefix(&prefix, payload.get_data());
            let block = Block::new(payload.get_data().to_vec(), cid);
            message.add_block(block);
        }
        Ok(message)
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_ledger_send() {

    }
}
