use crate::block::Cid;
use crate::repo::Repo;
use libp2p::PeerId;
use crate::bitswap::ledger::{Ledger, Priority};

pub trait Strategy {
    fn new(repo: Repo) -> Self;
    fn receive_want(&mut self, ledger: &mut Ledger, peer_id: &PeerId, cid: Cid, priority: Priority);
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
        ledger: &mut Ledger,
        peer_id: &PeerId,
        cid: Cid,
        _priority: Priority,
    ) {
        let block = self.repo.get(&cid);
        if block.is_some() {
            ledger.send_block(peer_id, block.unwrap());
        }
    }
}
