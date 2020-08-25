use futures::pin_mut;
use futures::stream::StreamExt; // needed for StreamExt::next
use ipfs::{Ipfs, IpfsPath, TestTypes, UninitializedIpfs};
use std::env;
use std::process::exit;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // this example will wait forever attempting to fetch a CID provided at command line. It is
    // expected to be used by connecting another ipfs peer to it and providing the blocks from that
    // peer.

    let path = match env::args().nth(1).map(|s| s.parse::<IpfsPath>()) {
        Some(Ok(cid)) => cid,
        Some(Err(e)) => {
            eprintln!(
                "Failed to parse {} as IpfsPath: {}",
                env::args().nth(1).unwrap(),
                e
            );
            exit(1);
        }
        None => {
            eprintln!("Usage: fetch_and_cat <IPFS_PATH | CID>");
            eprintln!(
                "Example will accept connections and print all bytes of the unixfs file to \
                stdout."
            );
            exit(0);
        }
    };

    if path.root().cid().is_none() {
        eprintln!(
            "Unsupported path: ipns resolution is not available yet: {}",
            path
        );
        exit(1);
    }

    // Start daemon and initialize repo
    let (ipfs, fut): (Ipfs<TestTypes>, _) =
        UninitializedIpfs::default().await.start().await.unwrap();
    tokio::task::spawn(fut);

    let (_, addresses) = ipfs.identity().await.unwrap();
    assert!(!addresses.is_empty(), "Zero listening addresses");

    eprintln!("Please connect an ipfs node having {} to:\n", path);

    for address in addresses {
        eprintln!(" - {}", address);
    }

    eprintln!();

    let stream = ipfs.cat_unixfs(path, None).await.unwrap_or_else(|e| {
        eprintln!("Error: {}", e);
        exit(1);
    });
    // The stream needs to be pinned on the stack to be used with StreamExt::next
    pin_mut!(stream);
    let mut stdout = tokio::io::stdout();

    let mut total = 0;

    loop {
        // This could be made more performant by polling the stream while writing to stdout.
        match stream.next().await {
            Some(Ok(bytes)) => {
                total += bytes.len();
                stdout.write_all(&bytes).await.unwrap();

                eprintln!(
                    "Received: {:>12} bytes, Total: {:>12} bytes",
                    bytes.len(),
                    total
                );
            }
            Some(Err(e)) => {
                eprintln!("Error: {}", e);
                exit(1);
            }
            None => break,
        }
    }

    eprintln!("Total received: {} bytes", total);
}
