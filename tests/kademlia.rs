use cid::{Cid, Codec};
use ipfs::{p2p::MultiaddrWithPeerId, Block, Node};
use libp2p::{multiaddr::Protocol, Multiaddr, PeerId};
use multihash::Sha2_256;
#[cfg(feature = "test_dht_with_go")]
use rand::prelude::*;
#[cfg(feature = "test_dht_with_go")]
use serde::Deserialize;
use std::{convert::TryInto, time::Duration};
#[cfg(feature = "test_dht_with_go")]
use std::{
    env, fs,
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
};
use tokio::time::timeout;

fn strip_peer_id(addr: Multiaddr) -> Multiaddr {
    let MultiaddrWithPeerId { multiaddr, .. } = addr.try_into().unwrap();
    multiaddr.into()
}

#[cfg(feature = "test_dht_with_go")]
#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct GoNodeId {
    #[serde(rename = "ID")]
    id: String,
    #[serde(skip)]
    public_key: String,
    addresses: Vec<String>,
    #[serde(skip)]
    agent_version: String,
    #[serde(skip)]
    protocol_version: String,
}

#[cfg(feature = "test_dht_with_go")]
struct GoIpfsNode {
    daemon: Child,
    id: GoNodeId,
    dir: PathBuf,
}

#[cfg(not(feature = "test_dht_with_go"))]
struct GoIpfsNode;

#[cfg(feature = "test_dht_with_go")]
impl Drop for GoIpfsNode {
    fn drop(&mut self) {
        let _ = self.daemon.kill();
        let _ = fs::remove_dir_all(&self.dir);
    }
}

/// Check if `Ipfs::find_peer` works without DHT involvement.
#[tokio::test(max_threads = 1)]
async fn find_peer_local() {
    let node_a = Node::new("a").await;
    let node_b = Node::new("b").await;

    let (b_id, mut b_addrs) = node_b.identity().await.unwrap();
    let b_id = b_id.into_peer_id();

    node_a.connect(b_addrs[0].clone()).await.unwrap();

    // while node_a is connected to node_b, they know each other's
    // addresses and can find them without using the DHT
    let found_addrs = node_a.find_peer(b_id).await.unwrap();

    // remove Protocol::P2p from b_addrs
    for addr in &mut b_addrs {
        assert!(matches!(addr.pop(), Some(Protocol::P2p(_))));
    }

    assert_eq!(found_addrs, b_addrs);
}

// starts the specified number of rust IPFS nodes connected in a chain.
#[cfg(not(feature = "test_dht_with_go"))]
async fn start_nodes_in_chain(
    count: usize,
) -> (Vec<Node>, Vec<(PeerId, Multiaddr)>, Option<GoIpfsNode>) {
    // fire up count nodes and register their PeerIds and
    // Multiaddrs (without the PeerId) for add_peer purposes
    let mut nodes = Vec::with_capacity(count);
    let mut ids_and_addrs = Vec::with_capacity(count);
    for i in 0..count {
        let node = Node::new(i.to_string()).await;

        let (key, mut addrs) = node.identity().await.unwrap();
        let id = key.into_peer_id();
        let addr = strip_peer_id(addrs.pop().unwrap());

        nodes.push(node);
        ids_and_addrs.push((id, addr));
    }

    // register the nodes' addresses so they can bootstrap against
    // one another in a chain; node 0 is only aware of the existence
    // of node 1 and so on; bootstrap them eagerly/quickly, so that
    // they don't have a chance to form the full picture in the DHT
    for i in 0..count {
        let (next_id, next_addr) = if i < count - 1 {
            ids_and_addrs[i + 1].clone()
        } else {
            // the last node in the chain also needs to know some address
            // in order to bootstrap, so give it its neighbour's information
            // and then bootstrap it as well
            ids_and_addrs[count - 2].clone()
        };

        nodes[i].add_peer(next_id, next_addr).await.unwrap();
        nodes[i].bootstrap().await.unwrap();
    }

    // make sure that the nodes are not actively connected to each other
    // and that we are actually going to be testing the DHT here
    for node in &nodes {
        assert!([1usize, 2].contains(&node.peers().await.unwrap().len()));
    }

    (nodes, ids_and_addrs, None)
}

#[cfg(feature = "test_dht_with_go")]
fn start_go_node() -> GoIpfsNode {
    // GO_IPFS_PATH should point to the location of the go-ipfs binary
    let go_ipfs_path = env::vars()
        .find(|(key, _val)| key == "GO_IPFS_PATH")
        .expect("the GO_IPFS_PATH environment variable not found")
        .1;

    let mut tmp_dir = env::temp_dir();
    let mut rng = rand::thread_rng();
    tmp_dir.push(&format!("ipfs_test_{}", rng.gen::<u64>()));
    let _ = fs::create_dir(&tmp_dir);

    Command::new(&go_ipfs_path)
        .env("IPFS_PATH", &tmp_dir)
        .arg("init")
        .arg("-p")
        .arg("test")
        .arg("--bits")
        .arg("2048")
        .stdout(Stdio::null())
        .status()
        .unwrap();

    let go_daemon = Command::new(&go_ipfs_path)
        .env("IPFS_PATH", &tmp_dir)
        .arg("daemon")
        .stdout(Stdio::null())
        .spawn()
        .unwrap();

    // give the go-ipfs daemon a little bit of time to start
    thread::sleep(Duration::from_secs(1));

    let go_id = Command::new(&go_ipfs_path)
        .env("IPFS_PATH", &tmp_dir)
        .arg("id")
        .output()
        .unwrap()
        .stdout;

    let go_id_stdout = String::from_utf8_lossy(&go_id);
    let go_id: GoNodeId = serde_json::de::from_str(&go_id_stdout).unwrap();

    GoIpfsNode {
        daemon: go_daemon,
        id: go_id,
        dir: tmp_dir,
    }
}

// most of the setup is the same as in the not(feature = "test_dht_with_go") case, with
// the addition of a go-ipfs node in the middle of the chain; the first half of the chain
// learns about the next peer, the go-ipfs node being the last one, and the second half
// learns about the previous peer, the go-ipfs node being the first one; a visualization:
// r[0] > r[1] > .. > go < .. < r[count - 3] < r[count - 2]
#[cfg(feature = "test_dht_with_go")]
async fn start_nodes_in_chain(
    count: usize,
) -> (Vec<Node>, Vec<(PeerId, Multiaddr)>, Option<GoIpfsNode>) {
    let go_node = start_go_node();

    let mut nodes = Vec::with_capacity(count - 1);
    let mut ids_and_addrs = Vec::with_capacity(count - 1);
    // exclude one node to make room for the intermediary go-ipfs node
    for i in 0..(count - 1) {
        let node = Node::new(i.to_string()).await;

        let (key, mut addrs) = node.identity().await.unwrap();
        let id = key.into_peer_id();
        let addr = strip_peer_id(addrs.pop().unwrap());

        nodes.push(node);
        ids_and_addrs.push((id, addr));
    }

    let go_peer_id = go_node.id.id.parse::<PeerId>().unwrap();
    let go_addr = strip_peer_id(go_node.id.addresses[0].parse::<Multiaddr>().unwrap());

    // skip the last index again, as there is a go node without one bound to it
    for i in 0..(count - 1) {
        let (next_id, next_addr) = if i == count / 2 - 1 || i == count / 2 {
            println!("telling rust node {} about the go node", i);
            (go_peer_id.clone(), go_addr.clone())
        } else if i < count / 2 {
            println!("telling rust node {} about rust node {}", i, i + 1);
            ids_and_addrs[i + 1].clone()
        } else {
            println!("telling rust node {} about rust node {}", i, i - 1);
            ids_and_addrs[i - 1].clone()
        };

        nodes[i].add_peer(next_id, next_addr).await.unwrap();
        nodes[i].bootstrap().await.unwrap();
    }

    // in this case we can't make sure that all the nodes only have 1 or 2 peers but it's not a big
    // deal, since in reality this kind of extreme conditions are unlikely and we already test that
    // in the pure-rust setup

    (nodes, ids_and_addrs, Some(go_node))
}

/// Check if `Ipfs::find_peer` works using DHT.
#[tokio::test(max_threads = 1)]
async fn dht_find_peer() {
    // works for numbers >=2, though 2 would essentially just
    // be the same as find_peer_local, so it should be higher
    const CHAIN_LEN: usize = 10;
    let (nodes, ids_and_addrs, go_node) = start_nodes_in_chain(CHAIN_LEN).await;
    let last_index = CHAIN_LEN - if go_node.is_none() { 1 } else { 2 };

    // node 0 now tries to find the address of the very last node in the
    // chain; the chain should be long enough for it not to automatically
    // be connected to it after the bootstrap
    let found_addrs = nodes[0]
        .find_peer(ids_and_addrs[last_index].0.clone())
        .await
        .unwrap();

    assert_eq!(found_addrs, vec![ids_and_addrs[last_index].1.clone()]);
}

#[tokio::test(max_threads = 1)]
async fn dht_get_closest_peers() {
    const CHAIN_LEN: usize = 10;
    let (nodes, ids_and_addrs, _go_node) = start_nodes_in_chain(CHAIN_LEN).await;

    assert_eq!(
        nodes[0]
            .get_closest_peers(ids_and_addrs[0].0.clone())
            .await
            .unwrap()
            .len(),
        CHAIN_LEN - 1
    );
}

#[ignore = "targets an actual bootstrapper, so random failures can happen"]
#[tokio::test(max_threads = 1)]
async fn dht_popular_content_discovery() {
    let (bootstrapper_id, bootstrapper_addr): (PeerId, Multiaddr) = (
        "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"
            .parse()
            .unwrap(),
        "/ip4/104.131.131.82/tcp/4001".parse().unwrap(),
    );

    let peer = Node::new("a").await;

    // connect it to one of the well-known bootstrappers
    assert!(peer
        .add_peer(bootstrapper_id, bootstrapper_addr)
        .await
        .is_ok());

    // the Cid of the IPFS logo
    let cid: Cid = "bafkreicncneocapbypwwe3gl47bzvr3pkpxmmobzn7zr2iaz67df4kjeiq"
        .parse()
        .unwrap();

    assert!(timeout(Duration::from_secs(10), peer.get_block(&cid))
        .await
        .is_ok());
}

/// Check if Ipfs::{get_providers, provide} does its job.
#[tokio::test(max_threads = 1)]
async fn dht_providing() {
    const CHAIN_LEN: usize = 10;
    let (nodes, ids_and_addrs, go_node) = start_nodes_in_chain(CHAIN_LEN).await;
    let last_index = CHAIN_LEN - if go_node.is_none() { 1 } else { 2 };

    // the last node puts a block in order to have something to provide
    let data = b"hello block\n".to_vec().into_boxed_slice();
    let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));
    nodes[last_index]
        .put_block(Block {
            cid: cid.clone(),
            data,
        })
        .await
        .unwrap();

    // the last node then provides the Cid
    nodes[last_index].provide(cid.clone()).await.unwrap();

    // and the first node should be able to learn that the last one provides it
    assert!(nodes[0]
        .get_providers(cid)
        .await
        .unwrap()
        .contains(&ids_and_addrs[last_index].0.clone()));
}
