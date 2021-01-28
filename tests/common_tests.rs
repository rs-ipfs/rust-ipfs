mod common;
use common::{spawn_nodes, Topology};

// these tests are here instead of being under common/mod.rs so that they wont be executed as part
// of every `test/` case which includes `mod common;`.

const N: usize = 5;

#[tokio::test]
async fn check_topology_line() {
    let nodes = spawn_nodes(N, Topology::Line).await;

    for (i, node) in nodes.iter().enumerate() {
        if i == 0 || i == N - 1 {
            assert_eq!(node.peers().await.unwrap().len(), 1);
        } else {
            assert_eq!(node.peers().await.unwrap().len(), 2);
        }
    }
}

#[tokio::test]
async fn check_topology_ring() {
    let nodes = spawn_nodes(N, Topology::Ring).await;

    for node in &nodes {
        assert_eq!(node.peers().await.unwrap().len(), 2);
    }
}

#[tokio::test]
async fn check_topology_mesh() {
    let nodes = spawn_nodes(N, Topology::Mesh).await;

    for node in &nodes {
        assert_eq!(node.peers().await.unwrap().len(), N - 1);
    }
}

#[tokio::test]
async fn check_topology_star() {
    let nodes = spawn_nodes(N, Topology::Star).await;

    for (i, node) in nodes.iter().enumerate() {
        if i == 0 {
            assert_eq!(node.peers().await.unwrap().len(), N - 1);
        } else {
            assert_eq!(node.peers().await.unwrap().len(), 1);
        }
    }
}
