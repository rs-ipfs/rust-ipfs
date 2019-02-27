use crate::block::{Block, Cid};
use crate::bitswap::Priority;
use crate::repo::{BlockStore, Repo, RepoTypes};
use libp2p::PeerId;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

pub trait Strategy<TRepoTypes: RepoTypes>: Send + Unpin {
    fn new(repo: Repo<TRepoTypes>) -> Self;
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

pub struct AltruisticStrategy<TRepoTypes: RepoTypes> {
    repo: Repo<TRepoTypes>,
    events: Arc<Mutex<VecDeque<StrategyEvent>>>,
}

impl<TRepoTypes: RepoTypes> Strategy<TRepoTypes> for AltruisticStrategy<TRepoTypes> {
    fn new(repo: Repo<TRepoTypes>) -> Self {
        AltruisticStrategy {
            repo,
            events: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    fn process_want(
        &mut self,
        source: PeerId,
        cid: Cid,
        priority: Priority,
    ) {
        info!("Peer {} wants block {} with priority {}",
              source.to_base58(), cid.to_string(), priority);
        let events = self.events.clone();
        let block_store = self.repo.block_store.clone();
        let future = block_store.get(&cid);
        tokio::spawn_async(async move {
            if let Some(block) = await!(future).unwrap() {
                events.lock().unwrap().push_back(StrategyEvent::Send {
                    peer_id: source,
                    block: block,
                });
            }
        });
    }

    fn process_block(&mut self, source: PeerId, block: Block) {
        info!("Received block {} from peer {}",
              block.cid().to_string(),
              source.to_base58());
        let future = self.repo.block_store.put(block);
        tokio::spawn_async(async move {
            await!(future).unwrap();
        });
    }

    fn poll(&mut self) -> Option<StrategyEvent> {
        self.events.lock().unwrap().pop_front()
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
