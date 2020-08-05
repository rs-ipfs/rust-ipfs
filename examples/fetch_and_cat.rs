#![recursion_limit = "512"]

use cid::Cid;
use futures::io::AsyncWriteExt;
use futures::pin_mut;
use futures::stream::StreamExt; // needed for StreamExt::next
use ipfs::{Ipfs, TestTypes, UninitializedIpfs};
use std::convert::TryFrom;
use std::env;
use std::process::exit;

fn main() {
    tracing_subscriber::fmt::init();

    // this example will wait forever attempting to fetch a CID provided at command line. It is
    // expected to be used by connecting another ipfs peer to it and providing the blocks from that
    // peer.

    let cid = match env::args().nth(1).map(Cid::try_from) {
        Some(Ok(cid)) => cid,
        Some(Err(e)) => {
            eprintln!(
                "Failed to parse {} as Cid: {}",
                env::args().nth(1).unwrap(),
                e
            );
            exit(1);
        }
        None => {
            eprintln!("Usage: fetch_and_cat CID");
            eprintln!(
                "Example will accept connections and print all bytes of the unixfs file to \
                stdout."
            );
            exit(0);
        }
    };

    async_std::task::block_on(async move {
        // Start daemon and initialize repo
        let (ipfs, fut): (Ipfs<TestTypes>, _) =
            UninitializedIpfs::default().await.start().await.unwrap();
        async_std::task::spawn(fut);

        let (public_key, addresses) = ipfs.identity().await.unwrap();
        assert!(!addresses.is_empty(), "Zero listening addresses");

        eprintln!("Please connect an ipfs node having {} to:\n", cid);

        let peer_id = public_key.into_peer_id().to_string();

        for address in addresses {
            eprintln!(" - {}/p2p/{}", address, peer_id);
        }

        eprintln!();

        let stream = ipfs.cat_unixfs(cid, None).await.unwrap_or_else(|e| {
            eprintln!("Error: {}", e);
            exit(1);
        });
        // The stream needs to be pinned on the stack to be used with StreamExt::next
        pin_mut!(stream);
        let mut stdout = async_std::io::stdout();

        loop {
            // This could be made more performant by polling the stream while writing to stdout.
            match stream.next().await {
                Some(Ok(bytes)) => {
                    stdout.write_all(&bytes).await.unwrap();
                }
                Some(Err(e)) => {
                    eprintln!("Error: {}", e);
                    exit(1);
                }
                None => break,
            }
        }
    })
}
