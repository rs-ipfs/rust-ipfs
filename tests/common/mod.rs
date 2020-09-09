pub mod interop;

use ipfs::Node;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Topology {
    Line,
    Ring,
    Mesh,
}

#[allow(dead_code)]
pub async fn spawn_connected_nodes(count: usize, topology: Topology) -> Vec<Node> {
    let mut nodes = Vec::with_capacity(count);

    for i in 0..count {
        let node = Node::new(i.to_string()).await;
        nodes.push(node);
    }

    if topology != Topology::Mesh {
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
    } else {
        for i in 0..count {
            for (j, peer) in nodes.iter().enumerate() {
                if i != j {
                    nodes[i].connect(peer.addrs[0].clone()).await.unwrap();
                }
            }
        }
    }

    nodes
}
