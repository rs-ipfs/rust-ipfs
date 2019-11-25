use crate::block::{Block, Cid};
use crate::bitswap::Priority;
use crate::repo::{Repo, RepoTypes};
use libp2p::PeerId;
use std::sync::mpsc::{channel, Sender, Receiver};

pub trait Strategy<TRepoTypes: RepoTypes>: Send + Unpin {
    fn new(repo: Repo<TRepoTypes>) -> Self;
    fn process_want(&self, source: PeerId, cid: Cid, priority: Priority);
    fn process_block(&self, source: PeerId, block: Block);
    fn poll(&self) -> Option<StrategyEvent>;
}

pub enum StrategyEvent {
    Send {
        peer_id: PeerId,
        block: Block,
    }
}

pub struct AltruisticStrategy<TRepoTypes: RepoTypes> {
    repo: Repo<TRepoTypes>,
    events: (Sender<StrategyEvent>, Receiver<StrategyEvent>),
}

impl<TRepoTypes: RepoTypes> Strategy<TRepoTypes> for AltruisticStrategy<TRepoTypes> {
    fn new(repo: Repo<TRepoTypes>) -> Self {
        AltruisticStrategy {
            repo,
            events: channel::<StrategyEvent>(),
        }
    }

    fn process_want(&self, source: PeerId, cid: Cid, priority: Priority) {
        use futures::FutureExt;
        use futures::TryFutureExt;
        info!("Peer {} wants block {} with priority {}", source.to_base58(), cid.to_string(), priority);
        let events = self.events.0.clone();
        let repo = self.repo.clone();

        tokio::spawn(async move {
            let res = repo.get_block(&cid).await;

            let block = if let Err(e) = res {
                warn!("Peer {} wanted block {} but we failed: {}", source.to_base58(), cid, e);
                return;
            } else {
                res.unwrap()
            };

            let req = StrategyEvent::Send {
                peer_id: source.clone(),
                block: block,
            };

            if let Err(e) = events.send(req) {
                warn!("Peer {} wanted block {} we failed start sending it: {}", source.to_base58(), cid, e);
            }
        }.unit_error().boxed().compat());
    }

    fn process_block(&self, source: PeerId, block: Block) {
        use futures::FutureExt;
        use futures::TryFutureExt;
        let cid = block.cid().to_string();
        info!("Received block {} from peer {}", cid, source.to_base58());

        // FIXME: this is difficult because of mpsc::Sender<RepoEvent> being !Send
        // likely related to https://github.com/rust-lang/rust/issues/63768
        // but boxing introduces a Sync requirement which I wasn't able to do without introducing
        // Arc<Mutex<mpsc::Sender<_>> and adding locking to repo
        let repo = self.repo.clone();

        tokio::spawn(async move {
            let future = repo.put_block(block).boxed();
            if let Err(e) = future.await {
                debug!("Got block {} from peer {} but failed to store it: {}", cid, source.to_base58(), e);
            }
        }.unit_error().boxed().compat());
    }

    fn poll(&self) -> Option<StrategyEvent> {
        self.events.1.try_recv().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::Block;

    #[test]
    fn test_altruistic_strategy() {
        unimplemented!();
        /*
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
        */
    }
}
