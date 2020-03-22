use bitswap::{Block, Priority};
use async_std::task;
use async_trait::async_trait;
use libipld::cid::Cid;
use libp2p_core::PeerId;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;

#[async_trait]
pub trait BitswapStore: Send + Sync {
    async fn get_block(&self, cid: &Cid) -> Result<Option<Block>, anyhow::Error>;

    async fn put_block(&self, block: Block) -> Result<(), anyhow::Error>;
}

pub trait Strategy: Send + Unpin + 'static {
    fn new(store: Arc<dyn BitswapStore>) -> Self;
    fn process_want(&self, source: PeerId, cid: Cid, priority: Priority);
    fn process_block(&self, source: PeerId, block: Block);
    fn poll(&self) -> Option<StrategyEvent>;
}

#[derive(Debug)]
pub enum StrategyEvent {
    Send { peer_id: PeerId, block: Block },
}

pub struct AltruisticStrategy {
    store: Arc<dyn BitswapStore>,
    events: (Sender<StrategyEvent>, Receiver<StrategyEvent>),
}

impl Strategy for AltruisticStrategy {
    fn new(store: Arc<dyn BitswapStore>) -> Self {
        AltruisticStrategy {
            store,
            events: channel::<StrategyEvent>(),
        }
    }

    fn process_want(&self, source: PeerId, cid: Cid, priority: Priority) {
        info!(
            "Peer {} wants block {} with priority {}",
            source.to_base58(),
            cid.to_string(),
            priority
        );
        let events = self.events.0.clone();
        let store = self.store.clone();

        task::spawn(async move {
            let block = match store.get_block(&cid).await {
                Ok(None) => return,
                Ok(Some(block)) => block,
                Err(err) => {
                    warn!(
                        "Peer {} wanted block {} but we failed: {}",
                        source.to_base58(),
                        cid,
                        err,
                    );
                    return;
                }
            };

            let req = StrategyEvent::Send {
                peer_id: source.clone(),
                block,
            };

            if let Err(e) = events.send(req) {
                warn!(
                    "Peer {} wanted block {} we failed start sending it: {}",
                    source.to_base58(),
                    cid,
                    e
                );
            }
        });
    }

    fn process_block(&self, source: PeerId, block: Block) {
        let cid = block.cid().to_string();
        info!("Received block {} from peer {}", cid, source.to_base58());

        let store = self.store.clone();

        task::spawn(async move {
            let res = store.put_block(block).await;
            if let Err(e) = res {
                debug!(
                    "Got block {} from peer {} but failed to store it: {}",
                    cid,
                    source.to_base58(),
                    e
                );
            }
        });
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
        let block_1 = Block::from("1");
        let block_2 = Block::from("2");
        let repo = Repo::new();
        repo.put(block_1.clone());
        let mut strategy = AltruisticStrategy::new(repo);
        let mut ledger = Ledger::new();
        let peer_id = PeerId::random();
        ledger.peer_connected(peer_id.clone());
        strategy.process_want(&mut ledger, &peer_id, block_1.cid(), 1);
        strategy.process_want(&mut ledger, &peer_id, block_2.cid(), 1);
        ledger.send_messages();
        let peer_ledger = ledger.peer_ledger(&peer_id);
        assert_eq!(peer_ledger.sent_blocks(), 1);
    }
}
