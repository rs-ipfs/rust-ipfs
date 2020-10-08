use futures::pin_mut;
use futures::stream::StreamExt; // needed for StreamExt::next
use ipfs::{Error, Ipfs, IpfsOptions, IpfsPath, MultiaddrWithPeerId, TestTypes, UninitializedIpfs};
use std::env;
use std::process::exit;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let (bootstrappers, path, target) = match parse_options() {
        Ok(Some(tuple)) => tuple,
        Ok(None) => {
            eprintln!(
                "Usage: fetch_and_cat [--default-bootstrappers] <IPFS_PATH | CID> [MULTIADDR]"
            );
            eprintln!();
            eprintln!(
                "Example will try to find the file by the given IPFS_PATH and print its contents to stdout."
            );
            eprintln!();
            eprintln!("The example has three modes in the order of precedence:");
            eprintln!(
                "1. When --default-bootstrappers is given, use default bootstrappers to find the content"
            );
            eprintln!(
                "2. When IPFS_PATH and MULTIADDR are given, connect to MULTIADDR to get the file"
            );
            eprintln!(
                "3. When only IPFS_PATH is given, wait to be connected to by another ipfs node"
            );
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

    if bootstrappers == BootstrapperOption::RestoreDefault {
        // applications wishing to find content on the global IPFS swarm should restore the latest
        // bootstrappers which are hopefully updated between releases
        ipfs.restore_bootstrappers().await.unwrap();
    } else if let Some(target) = target {
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
}

#[derive(PartialEq)]
enum BootstrapperOption {
    RestoreDefault,
    ConnectionsOnly,
}

fn parse_options(
) -> Result<Option<(BootstrapperOption, IpfsPath, Option<MultiaddrWithPeerId>)>, Error> {
    let mut args = env::args().skip(1).peekable();

    // by default use only the manual connections
    let mut bootstrappers = BootstrapperOption::ConnectionsOnly;

    while let Some(option) = args.peek() {
        if !option.starts_with("--") {
            break;
        }

        let option = args.next().expect("already checked when peeking");

        if option == "--default-bootstrappers" {
            bootstrappers = BootstrapperOption::RestoreDefault;
        } else {
            return Err(anyhow::format_err!("unknown option: {}", option));
        }
    }

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

    Ok(Some((bootstrappers, path, target)))
}
