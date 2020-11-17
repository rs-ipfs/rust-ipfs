pub mod interop;

use ipfs::Node;

/// The way in which nodes are connected to each other; to be used with spawn_nodes.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Topology {
    /// no connections
    None,
    /// a > b > c
    Line,
    /// a > b > c > a
    Ring,
    /// a <> b <> c <> a
    Mesh,
    /// a > b, a > c
    Star,
}

#[allow(dead_code)]
pub async fn spawn_nodes(count: usize, topology: Topology) -> Vec<Node> {
    let mut nodes = Vec::with_capacity(count);

    for i in 0..count {
        let node = Node::new(i.to_string()).await;
        nodes.push(node);
    }

    match topology {
        Topology::Line | Topology::Ring => {
            for i in 0..(count - 1) {
                nodes[i]
                    .connect(nodes[i + 1].addrs[0].clone())
                    .await
                    .unwrap();
            }
            if topology == Topology::Ring {
                nodes[count - 1]
                    .connect(nodes[0].addrs[0].clone())
                    .await
                    .unwrap();
            }
        }
        Topology::Mesh => {
            for i in 0..count {
                for (j, peer) in nodes.iter().enumerate() {
                    if i != j {
                        nodes[i].connect(peer.addrs[0].clone()).await.unwrap();
                    }
                }
            }
        }
        Topology::Star => {
            for node in nodes.iter().skip(1) {
                nodes[0].connect(node.addrs[0].clone()).await.unwrap();
            }
        }
        Topology::None => {}
    }

    nodes
}

#[cfg(test)]
mod tests {
    use super::*;

    const N: usize = 5;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn check_topology_ring() {
        let nodes = spawn_nodes(N, Topology::Ring).await;

        for node in &nodes {
            assert_eq!(node.peers().await.unwrap().len(), 2);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn check_topology_mesh() {
        let nodes = spawn_nodes(N, Topology::Mesh).await;

        for node in &nodes {
            assert_eq!(node.peers().await.unwrap().len(), N - 1);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
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
}
