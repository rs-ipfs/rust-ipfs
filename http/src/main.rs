use serde::Serialize;
use std::num::NonZeroU16;
use std::path::PathBuf;
use structopt::StructOpt;

use rust_ipfs_http::{config, v0};

#[derive(Debug, StructOpt)]
enum Options {
    /// Should initialize the repository (create directories and such). `js-ipfsd-ctl` calls this
    /// with two arguments by default, `--bits 1024` and `--profile test`.
    Init {
        /// Generated key length
        #[structopt(long)]
        bits: NonZeroU16,
        /// List of configuration profiles to apply
        #[structopt(long, use_delimiter = true)]
        profile: Vec<String>,
    },
    /// Start the IPFS node in the foreground (not detaching from parent process).
    Daemon,
}

fn main() {
    if std::env::var_os("RUST_LOG").is_none() {
        // FIXME: see if tracing could be used as the frontend for log macros
        // FIXME: use log macros here as well
        std::env::set_var("RUST_LOG", "rust-ipfs-http=trace,rust-ipfs=trace");
    }

    env_logger::init();

    let opts = Options::from_args();

    println!("Invoked with args: {:?}", opts);

    // go-ipfs seems to deduce like this
    let home = std::env::var_os("IPFS_PATH")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|tilde| {
                let mut path = PathBuf::from(tilde);
                path.push(".ipfs");
                path
            })
        });

    // FIXME: need to process cmdline args here, but trying to understand js-ipfsd-ctl right now a
    // bit more.

    let home = match home {
        Some(path) => path,
        None => {
            eprintln!("IPFS_PATH and HOME unset");
            std::process::exit(1);
        }
    };

    let config_path = home.join("config");

    match opts {
        Options::Init { bits, profile } => {
            println!("initializing IPFS node at {:?}", home);

            if config_path.is_file() {
                eprintln!("Error: ipfs configuration file already exists!");
                eprintln!("Reinitializing would override your keys.");
                std::process::exit(1);
            }

            let result = config::initialize(&home, bits, profile);

            match result {
                Ok(_) => {
                    let kp = std::fs::File::open(config_path)
                        .map_err(config::LoadingError::ConfigurationFileOpening)
                        .and_then(config::load)
                        .unwrap();

                    // go-ipfs prints here (in addition to earlier "initializing ..."):
                    //
                    // generating {}-bit RSA keypair...done

                    println!("peer identity: {}", kp.public().into_peer_id());
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
                Err(config::InitializationError::InvalidProfiles(profiles)) => {
                    eprintln!("Error: unsupported profile selection: {:?}", profiles);
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
            if !config_path.is_file() {
                eprintln!("Error: no IPFS repo found in {:?}", home);
                eprintln!("please run: 'ipfs init'");
                std::process::exit(1);
            }
        }
    };

    println!("IPFS_PATH: {:?}", home);

    // TODO: sigterm should initiate graceful shutdown, second time should shutdown right now
    // NOTE: sigkill ... well surely it will stop the process right away

    let mut rt = tokio::runtime::Runtime::new().expect("Failed to create event loop");

    rt.block_on(async move {
        let api_link_file = home.join("api");
        let (addr, server) = serve(home, ());

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
                .map_err(|e| eprintln!("Failed to truncate {:?}: {}", api_link_file, e));
        }
    });
}

fn serve(
    _home: PathBuf,
    _options: (),
) -> (std::net::SocketAddr, impl std::future::Future<Output = ()>) {
    use tokio::stream::StreamExt;
    use warp::Filter;

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);

    // Set up routes
    // /api
    let api = warp::path("api");

    // /api/v0
    let v0 = api.and(warp::path("v0"));

    // /api/v0/shutdown
    let shutdown = warp::post()
        .and(warp::path("shutdown"))
        .and(warp::any().map(move || shutdown_tx.clone()))
        .and_then(shutdown);

    // for some reason js-ipfsd-ctl does a POST here?
    let id = warp::path("id").and_then(id_query);

    // Same here. for some reason this is a post as well
    let api = shutdown
        .or(id)
        // Placeholder paths
        // https://docs.rs/warp/0.2.2/warp/macro.path.html#path-prefixes
        .or(warp::path!("add").and_then(not_implemented))
        .or(warp::path!("bitswap" / ..).and_then(not_implemented))
        .or(warp::path!("block" / ..).and_then(not_implemented))
        .or(warp::path!("bootstrap" / ..).and_then(not_implemented))
        .or(warp::path!("config" / ..).and_then(not_implemented))
        .or(warp::path!("dag" / ..).and_then(not_implemented))
        .or(warp::path!("dht" / ..).and_then(not_implemented))
        .or(warp::path!("get").and_then(not_implemented))
        .or(warp::path!("key" / ..).and_then(not_implemented))
        .or(warp::path!("name" / ..).and_then(not_implemented))
        .or(warp::path!("object" / ..).and_then(not_implemented))
        .or(warp::path!("pin" / ..).and_then(not_implemented))
        .or(warp::path!("ping" / ..).and_then(not_implemented))
        .or(warp::path!("pubsub" / ..).and_then(not_implemented))
        .or(warp::path!("refs" / ..).and_then(not_implemented))
        .or(warp::path!("repo" / ..).and_then(not_implemented))
        .or(warp::path!("stats" / ..).and_then(not_implemented))
        .or(warp::path!("swarm" / "connect")
            .and(warp::query::<v0::swarm::ConnectQuery>())
            .and_then(v0::swarm::connect))
        .or(warp::path!("swarm" / "peers")
            .and(warp::query::<v0::swarm::PeersQuery>())
            .and_then(v0::swarm::peers))
        .or(warp::path!("swarm" / "addrs").and_then(v0::swarm::addrs))
        .or(warp::path!("swarm" / "addrs" / "local")
            .and(warp::query::<v0::swarm::AddrsLocalQuery>())
            .and_then(v0::swarm::addrs_local))
        .or(warp::path!("swarm" / "disconnect")
            .and(warp::query::<v0::swarm::DisconnectQuery>())
            .and_then(v0::swarm::disconnect))
        .or(warp::path!("version")
            .and(warp::query::<v0::version::Query>())
            .and_then(v0::version::version));

    let routes = v0.and(api);
    let routes = routes.with(warp::log("rust-ipfs-http-v0"));

    warp::serve(routes).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async move {
        shutdown_rx.next().await;
    })
}

async fn shutdown(
    mut tx: tokio::sync::mpsc::Sender<()>,
) -> Result<impl warp::Reply, std::convert::Infallible> {
    Ok(match tx.send(()).await {
        Ok(_) => warp::http::StatusCode::OK,
        Err(_) => warp::http::StatusCode::NOT_IMPLEMENTED,
    })
}

async fn not_implemented() -> Result<impl warp::Reply, std::convert::Infallible> {
    Ok(warp::http::StatusCode::NOT_IMPLEMENTED)
}

// NOTE: go-ipfs accepts an -f option for format (unsure if same with Accept: request header),
// unsure on what values -f takes, since `go-ipfs` seems to just print the value back? With plain http
// requests the `f` or `format` is ignored. Perhaps it's a cli functionality. This should return a
// json body, which does get pretty formatted by the cli.
//
// FIXME: Reference has argument `arg: PeerId` which does get processed.
//
// https://docs.ipfs.io/reference/api/http/#api-v0-id
async fn id_query() -> Result<impl warp::Reply, std::convert::Infallible> {
    // the ids are from throwaway go-ipfs init -p test instance
    let response = IdResponse {
        id: "QmdNmxF88uyUzm8T7ps8LnCuZJzPnJvgUJxpKGqAMuxSQE",
        public_key: "CAASpgIwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC+z3lZB1KQxNtahCLOQFFdsUrQ/XT3XLe/09sODTbGGp5mUjylR6hNQijCHFtYY7DcuAKPeyxNbdcOPmyC85gb1a1UB+nT3DiHPlM3AspnVFXDDv1u
kZZ6Fgfs8amaWAWgx5KBRE49GjaG65+wVqtMwoALPa655bpsvaJX5JEeKe8hb8bLNup0O5Tpl3ThQ+ADXLJGmu/tWFI8SdE10xZY6hQG446B0/sL3f4HWqRbrlYrV8Ac2LyU3ZXynQ0yScqO4pXDDXoKZSI44xQNPuWQA9Y9IBWel/cbzTNjWMxapuyjoT9gmFRi52IFAl0RH9X85jFa6FYRkM+h81AiVjD/AgMB
AAE=",
        addresses: vec!["/ip4/127.0.0.1/tcp/8002/ipfs/QmdNmxF88uyUzm8T7ps8LnCuZJzPnJvgUJxpKGqAMuxSQE"],
        agent_version: "rust-ipfs/0.0.1",
        protocol_version: "ipfs/0.1.0",
    };

    Ok(warp::reply::json(&response))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct IdResponse {
    // PeerId
    #[serde(rename = "ID")]
    id: &'static str,
    // looks like Base64
    public_key: &'static str,
    // Multiaddrs
    addresses: Vec<&'static str>,
    // Multiaddr alike <agent_name>/<version>, like rust-ipfs/0.0.1
    agent_version: &'static str,
    // Multiaddr alike ipfs/0.1.0 ... not sure if there are plans to bump this anytime soon
    protocol_version: &'static str,
}
