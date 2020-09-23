use futures::pin_mut;
use futures::stream::StreamExt; // needed for StreamExt::next
use ipfs::{Error, Ipfs, IpfsOptions, IpfsPath, MultiaddrWithPeerId, TestTypes, UninitializedIpfs};
use std::env;
use std::process::exit;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // This example attempts to fetch a CID provided at command line. It is expected to be used by
    // either:
    //
    //  - connecting another ipfs peer to it
    //  - be given the other peers address as the last argument
    //
    // The other connecting or connected peer must be providing the requested CID or this will hang
    // forever.

    let (path, target) = match parse_options() {
        Ok(Some(tuple)) => tuple,
        Ok(None) => {
            eprintln!("Usage: fetch_and_cat <IPFS_PATH | CID> [MULTIADDR]");
            eprintln!(
                "Example will accept connections and print all bytes of the unixfs file to \
                stdout."
            );
            eprintln!("If second argument is present, it is expected to be a Multiaddr with \
                peer_id. The given Multiaddr will be connected to instead of awaiting an incoming connection.");
            exit(0);
        }
        Err(e) => {
            eprintln!("Invalid argument: {:?}", e);
            exit(1);
        }
    };

    // Initialize the repo and start a daemon.
    //
    // Here we are using the IpfsOptions::inmemory_with_generated_keys, which creates a new random
    // key and in-memory storage for blocks and pins.
    let mut opts = IpfsOptions::inmemory_with_generated_keys();

    // Disable MDNS to explicitly connect or be connected just in case there are multiple IPFS
    // nodes running.
    opts.mdns = false;

    // UninitializedIpfs will handle starting up the repository and return the facade (ipfs::Ipfs)
    // and the background task (ipfs::IpfsFuture).
    let (ipfs, fut): (Ipfs<TestTypes>, _) = UninitializedIpfs::new(opts).start().await.unwrap();

    // The background task must be spawned to use anything other than the repository; most notably,
    // the libp2p.
    tokio::task::spawn(fut);

    if let Some(target) = target {
        ipfs.connect(target).await.unwrap();
    } else {
        let (_, addresses) = ipfs.identity().await.unwrap();
        assert!(!addresses.is_empty(), "Zero listening addresses");

        eprintln!("Please connect an ipfs node having {} to:\n", path);

        for address in addresses {
            eprintln!(" - {}", address);
        }

        eprintln!();
    }

    // Calling Ipfs::cat_unixfs returns a future of a stream, because the path resolving
    // and the initial block loading will require at least one async call before any actual file
    // content can be *streamed*.
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

fn parse_options() -> Result<Option<(IpfsPath, Option<MultiaddrWithPeerId>)>, Error> {
    let mut args = env::args().skip(1);

    let path = if let Some(path) = args.next() {
        path.parse::<IpfsPath>()
            .map_err(|e| e.context(format!("failed to parse {:?} as IpfsPath", path)))?
    } else {
        return Ok(None);
    };

    let target = if let Some(multiaddr) = args.next() {
        let ma = multiaddr.parse::<MultiaddrWithPeerId>().map_err(|e| {
            Error::new(e).context(format!(
                "failed to parse {:?} as MultiaddrWithPeerId",
                multiaddr
            ))
        })?;
        Some(ma)
    } else {
        None
    };

    Ok(Some((path, target)))
}
