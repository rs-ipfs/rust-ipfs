use crate::block::{Block, Cid};
use crate::bitswap::Priority;
use crate::repo::Repo;
use libp2p::PeerId;
use std::collections::VecDeque;

pub trait Strategy {
    fn new(repo: Repo) -> Self;
    fn process_want(&mut self, source: PeerId, cid: Cid, priority: Priority);
    fn process_block(&mut self, source: PeerId, block: Block);
    fn poll(&mut self) -> Option<StrategyEvent>;
}

pub enum StrategyEvent {
    Send {
        peer_id: PeerId,
        block: Block,
    }
}

pub struct AltruisticStrategy {
    repo: Repo,
    events: VecDeque<StrategyEvent>,
}

impl Strategy for AltruisticStrategy {
    fn new(repo: Repo) -> Self {
        AltruisticStrategy {
            repo,
            events: VecDeque::new(),
        }
    }

    fn process_want(
        &mut self,
        source: PeerId,
        cid: Cid,
        priority: Priority,
    ) {
        println!("Peer {} wants block {} with priority {}",
                 source.to_base58(), cid.to_string(), priority);
        let block = self.repo.get(&cid);
        if block.is_some() {
            self.events.push_back(StrategyEvent::Send {
                peer_id: source,
                block: block.unwrap(),
            });
        }
    }

    fn process_block(&mut self, source: PeerId, block: Block) {
        println!("Received block {} from peer {}",
                 block.cid().to_string(),
                 source.to_base58());
        self.repo.put(block);
    }

    fn poll(&mut self) -> Option<StrategyEvent> {
        self.events.pop_front()
    }
}

#[cfg(test)]
mod tests {
    /*use super::*;
    use crate::block::Block;

    #[test]
    fn test_altruistic_strategy() {
        let block_1 = Block::from("1");
        let block_2 = Block::from("2");
        let repo = Repo::new();
        repo.put(block_1.clone());
        let mut strategy = AltruisticStrategy::new(repo);
        let mut ledger = Ledger::new();
        let peer_id = PeerId::random();
        ledger.peer_connected(peer_id.clone());
        strategy.receive_want(&mut ledger, &peer_id, block_1.cid(), 1);
        strategy.receive_want(&mut ledger, &peer_id, block_2.cid(), 1);
        ledger.send_messages();
        let peer_ledger = ledger.peer_ledger(&peer_id);
        assert_eq!(peer_ledger.sent_blocks(), 1);
    }*/
}
