use crate::block::Block;
use crate::ledger::Priority;
use async_std::task;
use async_trait::async_trait;
use futures::channel::mpsc::{unbounded, UnboundedReceiver as Receiver, UnboundedSender as Sender};
use libipld::cid::Cid;
use libp2p_core::PeerId;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

#[derive(Debug, PartialEq, Eq)]
pub enum BlockPut {
    Stored,
    Duplicate,
}

#[async_trait]
pub trait BitswapStore: Send + Sync {
    async fn get_block(&self, cid: &Cid) -> Result<Option<Block>, anyhow::Error>;

    async fn put_block(&self, block: Block) -> Result<BlockPut, anyhow::Error>;
}

pub trait Strategy: Send + Unpin + 'static {
    fn new(store: Arc<dyn BitswapStore>) -> Self;
    fn process_want(&self, source: PeerId, cid: Cid, priority: Priority);
    fn process_block(&self, source: PeerId, block: Block);
    fn poll(&self, ctx: &mut Context<'_>) -> Poll<Option<StrategyEvent>>;
}

#[derive(Debug)]
pub enum StrategyEvent {
    Send { peer_id: PeerId, block: Block },
    DuplicateBlockReceived { source: PeerId, bytes: u64 },
    NewBlockStored { source: PeerId, bytes: u64 },
}

pub struct AltruisticStrategy {
    store: Arc<dyn BitswapStore>,
    event_sender: Sender<StrategyEvent>,
    events: Mutex<Receiver<StrategyEvent>>,
}

impl Strategy for AltruisticStrategy {
    fn new(store: Arc<dyn BitswapStore>) -> Self {
        let (tx, rx) = unbounded();
        AltruisticStrategy {
            store,
            event_sender: tx,
            events: Mutex::new(rx),
        }
    }

    fn process_want(&self, source: PeerId, cid: Cid, priority: Priority) {
        info!(
            "Peer {} wants block {} with priority {}",
            source.to_base58(),
            cid.to_string(),
            priority
        );
        let sender = self.event_sender.clone();
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

            let _ = sender.unbounded_send(req);
        });
    }

    fn process_block(&self, source: PeerId, block: Block) {
        let cid = block.cid().to_string();
        info!("Received block {} from peer {}", cid, source.to_base58());

        let store = self.store.clone();
        let sender = self.event_sender.clone();

        task::spawn(async move {
            let bytes = block.data().len() as u64;
            let res = store.put_block(block).await;
            let evt = match res {
                Ok(BlockPut::Stored) => StrategyEvent::NewBlockStored { source, bytes },
                Ok(BlockPut::Duplicate) => StrategyEvent::DuplicateBlockReceived { source, bytes },
                Err(e) => {
                    debug!(
                        "Got block {} from peer {} but failed to store it: {}",
                        cid,
                        source.to_base58(),
                        e
                    );
                    return;
                }
            };

            let _ = sender.unbounded_send(evt);
        });
    }

    /// Can return Poll::Ready(None) multiple times, Poll::Pending
    fn poll(&self, ctx: &mut Context) -> Poll<Option<StrategyEvent>> {
        use futures::stream::StreamExt;

        let mut g = self
            .events
            .try_lock()
            .expect("Failed to acquire the uncontended mutex right away");

        g.poll_next_unpin(ctx)
    }
}

#[cfg(test)]
mod tests {
    /*
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
    }*/
}
