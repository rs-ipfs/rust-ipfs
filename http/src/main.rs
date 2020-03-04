use serde::Serialize;
use std::path::PathBuf;

fn main() {
    if std::env::var_os("RUST_LOG").is_none() {
        // FIXME: see if tracing could be used as the frontend for log macros
        // FIXME: use log macros here as well
        std::env::set_var("RUST_LOG", "rust-ipfs-http=trace,rust-ipfs=trace");
    }

    print!(
        "Invoked with args: {:?}",
        std::env::args().collect::<Vec<_>>()
    );
    println!();

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

    let home = match home {
        // FIXME: doing the check here opens toctou, should be done when opening the repo, while that
        // is equally bad.
        Some(s) if s.is_dir() => s,
        Some(s) => {
            eprintln!("Error: No IPFS repo found on {:?}", s);
            std::process::exit(1);
        }
        None => {
            eprintln!("IPFS_PATH and HOME unset");
            std::process::exit(1);
        }
    };

    println!("IPFS_PATH: {:?}", home);

    // TODO: sigterm should initiate graceful shutdown, second time should shutdown right now
    // NOTE: sigkill ... well surely it will stop the process right away

    let mut rt = tokio::runtime::Runtime::new().expect("Failed to create event loop");

    rt.block_on(async move {
        let api_link_file = home.join("api");
        let (addr, server) = serve(home, ());

        let api_multiaddr = format!("/ipv4/{}/tcp/{}", addr.ip(), addr.port());

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

    let api = warp::path("api");
    let v0 = api.and(warp::path("v0"));

    let shutdown = warp::post()
        .and(warp::path("shutdown"))
        .and(warp::any().map(move || shutdown_tx.clone()))
        .and_then(shutdown);

    let id = warp::get().and(warp::path("id")).and_then(id_query);

    let routes = v0.and(shutdown.or(id));

    let routes = routes.with(warp::log("rust-ipfs-http-v0"));

    // turns out there is no chance to do graceful shutdown with ephemeral addresses, which is
    // quite odd, but address should be read from a config file
    warp::serve(routes).bind_with_graceful_shutdown(([127, 0, 0, 1], 5099), async move {
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

// NOTE: go-ipfs accepts an -f option for format (unsure if same with Accept: request header),
// unsure on what values -f takes, since `go-ipfs` seems to just print the value back? With plain http
// requests the `f` or `format` is ignored. Perhaps it's a cli functionality. This should return a
// json body, which does get pretty formatted by the cli.
//
// FIXME: Reference has argument `arg: PeerId` which does get processed.
//
// https://docs.ipfs.io/reference/api/http/#api-v0-id
async fn id_query() -> Result<impl warp::Reply, std::convert::Infallible> {
    let response = IdResponse {
        id: "fakeid",
        public_key: "base64",
        addresses: vec!["/ipv4/127.0.0.1/tcp/8002/ipfs/fakeid"],
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
