#![recursion_limit = "512"]

use tokio::{io::{AsyncBufReadExt, BufReader, stdin}, task};
use cid::{Cid, Codec};
use ipfs::{Block, Ipfs, TestTypes, UninitializedIpfs};
use multihash::Sha2_256;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // this example demonstrates
    //  - block building
    //  - local swarm communication with go-ipfs

    // Start daemon and initialize repo
    let (ipfs, fut): (Ipfs<TestTypes>, _) =
        UninitializedIpfs::default().await.start().await.unwrap();
    task::spawn(fut);

    let data = b"block-want\n".to_vec().into_boxed_slice();
    let wanted = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));

    let (public_key, addresses) = ipfs.identity().await.unwrap();
    assert!(!addresses.is_empty(), "Zero listening addresses");

    eprintln!("Please connect an ipfs node having {} to:\n", wanted);

    let peer_id = public_key.into_peer_id().to_string();

    for address in addresses {
        eprintln!(" - {}/p2p/{}", address, peer_id);
    }

    eprintln!();
    eprintln!("The block wanted in this example can be created on the other node:");
    eprintln!("    echo block-want | ipfs block put -f raw");
    eprintln!();

    // Create a Block
    let data = b"block-provide\n".to_vec().into_boxed_slice();
    let cid = Cid::new_v1(Codec::Raw, Sha2_256::digest(&data));
    let provided = ipfs.put_block(Block::new(data, cid)).await.unwrap();

    eprintln!(
        "After connecting the node, it can be used to get block: {}",
        provided
    );
    eprintln!("This should print out \"block-provide\\n\":");
    eprintln!("    ipfs block get {}", provided);
    eprintln!();

    // Retrive a Block
    let block = ipfs.get_block(&wanted).await.unwrap();
    let contents = std::str::from_utf8(block.data()).unwrap();
    eprintln!("Block retrieved: {:?}", contents);

    eprintln!();
    eprintln!("Press enter or CTRL-C to exit this example.");

    let _ = BufReader::new(stdin()).read_line(&mut String::new()).await;

    ipfs.exit_daemon().await;
}
