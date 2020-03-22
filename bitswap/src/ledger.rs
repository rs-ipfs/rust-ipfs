use crate::bitswap_pb;
use crate::block::Block;
use crate::error::BitswapError;
use core::convert::TryFrom;
use core::marker::PhantomData;
use libipld::cid::{Cid, Prefix};
use prost::Message as ProstMessage;
use std::collections::HashMap;

/// The Ledger contains the history of transactions with a peer.
#[derive(Debug, Default)]
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
        Self::default()
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
/*
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
