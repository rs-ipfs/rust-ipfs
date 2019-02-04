use crate::block::Cid;
use crate::bitswap::Priority;
use crate::repo::Repo;
use libp2p::PeerId;
use crate::p2p::Swarm;

pub trait Strategy {
    fn new(repo: Repo) -> Self;
    fn receive_want(&mut self, swarm: &mut Swarm, source: PeerId, cid: Cid, priority: Priority);
}

pub struct AltruisticStrategy {
    repo: Repo,
}

impl Strategy for AltruisticStrategy {
    fn new(repo: Repo) -> Self {
        AltruisticStrategy {
            repo,
        }
    }

    fn receive_want(
        &mut self,
        swarm: &mut Swarm,
        source: PeerId,
        cid: Cid,
        _priority: Priority,
    ) {
        let block = self.repo.get(&cid);
        if block.is_some() {
            swarm.send_block(source, block.unwrap());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    }
}
