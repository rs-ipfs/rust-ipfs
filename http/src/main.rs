use std::num::NonZeroU16;
use std::path::PathBuf;
use structopt::StructOpt;

use ipfs::{Ipfs, IpfsOptions, IpfsTypes, UninitializedIpfs};
use ipfs::{Multiaddr, Protocol};
use ipfs_http::{config, v0};

#[macro_use]
extern crate tracing;

#[derive(Debug, StructOpt)]
enum Options {
    /// Should initialize the repository (create directories and such). `js-ipfsd-ctl` calls this
    /// with two arguments by default, `--bits 1024` and `--profile test`.
    Init {
        /// Generated key length
        #[structopt(long)]
        bits: NonZeroU16,
        /// List of configuration profiles to apply. Currently only the `Test` and `Default`
        /// profiles are supported.
        ///
        /// `Test` uses ephemeral ports (necessary for conformance tests), `Default` uses `4004`.
        #[structopt(long, use_delimiter = true)]
        profile: Vec<config::Profile>,
    },
    /// Start the IPFS node in the foreground (not detaching from parent process).
    Daemon,
}

fn main() {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var(
            "RUST_LOG",
            "ipfs_http=trace,ipfs=trace,bitswap=trace,ipfs_unixfs=trace",
        );
    }

    tracing_subscriber::fmt::init();

    let opts = Options::from_args();

    println!("Invoked with args: {:?}", opts);

    // go-ipfs seems to deduce like this
    let home = std::env::var_os("IPFS_PATH")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|tilde| {
                let mut path = PathBuf::from(tilde);
                path.push(".rust-ipfs");
                path
            })
        });

    // FIXME: need to process cmdline args here, but trying to understand js-ipfsd-ctl right now a
    // bit more.

    let home = home.unwrap_or_else(|| {
        eprintln!("IPFS_PATH and HOME unset");
        std::process::exit(1);
    });

    let config_path = home.join("config");

    let config = match opts {
        Options::Init { bits, profile } => {
            println!("initializing IPFS node at {:?}", home);

            if config_path.is_file() {
                eprintln!("Error: ipfs configuration file already exists!");
                eprintln!("Reinitializing would override your keys.");
                std::process::exit(1);
            }

            let result = config::init(&home, bits, profile);

            match result {
                Ok(peer_id) => {
                    // go-ipfs prints here (in addition to earlier "initializing ..."):
                    //
                    // generating {}-bit RSA keypair...done
                    println!("peer identity: {}", peer_id);
                    std::process::exit(0);
                }
                Err(config::InitializationError::DirectoryCreationFailed(e)) => {
                    eprintln!("Error: failed to create repository path {:?}: {}", home, e);
                    std::process::exit(1);
                }
                Err(config::InitializationError::ConfigCreationFailed(_)) => {
                    // this can be any number of errors like permission denied but these are the
                    // strings from go-ipfs
                    eprintln!("Error: ipfs configuration file already exists!");
                    eprintln!("Reinitializing would override your keys.");
                    std::process::exit(1);
                }
                Err(config::InitializationError::InvalidRsaKeyLength(bits)) => {
                    eprintln!("Error: --bits out of range [1024, 16384]: {}", bits);
                    eprintln!("This is a fake version of ipfs cli which does not support much");
                    std::process::exit(1);
                }
                Err(config::InitializationError::InvalidProfile(profile)) => {
                    eprintln!("Error: unsupported profile selection: {:?}", profile);
                    eprintln!("This is a fake version of ipfs cli which does not support much");
                    std::process::exit(1);
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    std::process::exit(1);
                }
            }
        }
        Options::Daemon => {
            // FIXME: toctou, should just match for this err?
            if !config_path.is_file() {
                eprintln!("Error: no IPFS repo found in {:?}", home);
                eprintln!("please run: 'ipfs init'");
                std::process::exit(1);
            }

            std::fs::File::open(config_path)
                .map_err(config::LoadingError::ConfigurationFileOpening)
                .and_then(config::load)
                .unwrap()
        }
    };

    println!("IPFS_PATH: {:?}", home);
    println!("Process id: {}", std::process::id());

    // TODO: sigterm should initiate graceful shutdown, second time should shutdown right now
    // NOTE: sigkill ... well surely it will stop the process right away

    let rt = tokio::runtime::Runtime::new().expect("Failed to create event loop");

    rt.block_on(async move {
        let opts = IpfsOptions {
            ipfs_path: home.clone(),
            keypair: config.keypair,
            bootstrap: Vec::new(),
            mdns: false,
            kad_protocol: None,
            listening_addrs: config.swarm,
            span: None,
        };

        // TODO: handle errors more gracefully.
        let (ipfs, task): (Ipfs<ipfs::Types>, _) = UninitializedIpfs::new(opts)
            .start()
            .await
            .expect("Initialization failed");

        tokio::spawn(task);

        let api_link_file = home.join("api");

        let (addr, server) = serve(&ipfs, config.api_addr);

        // shutdown future will handle signalling the exit
        drop(ipfs);

        // We can't simply reuse the address from the config as the test profile uses ephemeral
        // ports.
        let api_multiaddr = format!("/ip4/{}/tcp/{}", addr.ip(), addr.port());

        // this file is looked for when js-ipfsd-ctl checks optimistically if the IPFS_PATH has a
        // daemon running already. go-ipfs file does not contain newline at the end.
        let wrote = tokio::fs::write(&api_link_file, &api_multiaddr)
            .await
            .is_ok();

        println!("API listening on {}", api_multiaddr);
        println!("daemon is running");

        server.await;

        if wrote {
            // FIXME: this should probably make sure the contents match what we wrote or do some
            // locking on the repo, unsure how go-ipfs locks the fsstore
            let _ = tokio::fs::File::create(&api_link_file)
                .await
                .map_err(|e| info!("Failed to truncate {:?}: {}", api_link_file, e));
        }
    });

    info!("Shutdown complete");
}

fn serve<Types: IpfsTypes>(
    ipfs: &Ipfs<Types>,
    listening_addr: Multiaddr,
) -> (std::net::SocketAddr, impl std::future::Future<Output = ()>) {
    use std::net::SocketAddr;
    use warp::Filter;

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);

    let routes = v0::routes(ipfs, shutdown_tx);
    let routes = routes.with(warp::log(env!("CARGO_PKG_NAME")));

    let ipfs = ipfs.clone();

    let components = listening_addr.iter().collect::<Vec<_>>();

    let socket_addr = match components.as_slice() {
        [Protocol::Ip4(ip), Protocol::Tcp(port)] => SocketAddr::new(ip.clone().into(), *port),
        _ => panic!(
            "Couldn't convert MultiAddr into SocketAddr: {}",
            listening_addr
        ),
    };

    warp::serve(routes).bind_with_graceful_shutdown(socket_addr, async move {
        let _ = shutdown_rx.recv().await;
        info!("Shutdown trigger received; starting shutdown");
        ipfs.exit_daemon().await;
    })
}
