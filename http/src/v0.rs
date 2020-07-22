use ipfs::{Ipfs, IpfsTypes};
use std::convert::Infallible;
use warp::{query, Filter};

pub mod bitswap;
pub mod block;
pub mod dag;
pub mod id;
pub mod pubsub;
pub mod refs;
pub mod root_files;
pub mod swarm;
pub mod version;

pub mod support;
pub use support::recover_as_message_response;
pub(crate) use support::{with_ipfs, InvalidPeerId, NotImplemented, StringError};

/// Helper to combine the multiple filters together with Filter::or, possibly boxing the types in
/// the process. This greatly helps the build times for `ipfs-http`.
macro_rules! combine {
    ($x:expr, $($y:expr),+) => {
        {
            let filter = boxed_on_debug!($x);
            $(
                let filter = boxed_on_debug!(filter.or($y));
            )+
            filter
        }
    }
}

#[cfg(debug_assertions)]
macro_rules! boxed_on_debug {
    ($x:expr) => {
        $x.boxed()
    };
}

#[cfg(not(debug_assertions))]
macro_rules! boxed_on_debug {
    ($x:expr) => {
        $x
    };
}

pub fn routes<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
    shutdown_tx: tokio::sync::mpsc::Sender<()>,
) -> impl warp::Filter<Extract = impl warp::Reply, Error = Infallible> + Clone {
    let mount = warp::path("api").and(warp::path("v0"));

    let shutdown = warp::post()
        .and(warp::path!("shutdown"))
        .and(warp::any().map(move || shutdown_tx.clone()))
        .and_then(handle_shutdown);

    let api = mount.and(combine!(
        shutdown,
        id::identity(ipfs),
        root_files::add(ipfs),
        bitswap::wantlist(ipfs),
        bitswap::stat(ipfs),
        block::get(ipfs),
        block::put(ipfs),
        block::rm(ipfs),
        block::stat(ipfs),
        warp::path!("bootstrap" / ..).and_then(not_implemented),
        warp::path!("config" / ..).and_then(not_implemented),
        dag::put(ipfs),
        dag::resolve(ipfs),
        warp::path!("dht" / ..).and_then(not_implemented),
        root_files::cat(ipfs),
        root_files::get(ipfs),
        warp::path!("key" / ..).and_then(not_implemented),
        warp::path!("name" / ..).and_then(not_implemented),
        warp::path!("object" / ..).and_then(not_implemented),
        warp::path!("pin" / ..).and_then(not_implemented),
        warp::path!("ping" / ..).and_then(not_implemented),
        pubsub::routes(ipfs),
        refs::local(ipfs),
        refs::refs(ipfs),
        warp::path!("repo" / ..).and_then(not_implemented),
        warp::path!("stats" / ..).and_then(not_implemented),
        swarm::connect(ipfs),
        swarm::peers(ipfs),
        swarm::addrs(ipfs),
        swarm::addrs_local(ipfs),
        swarm::disconnect(ipfs),
        warp::path!("version")
            .and(query::<version::Query>())
            .and_then(version::version)
    ));

    // have a common handler turn the rejections into 400 or 500 with json body
    api.recover(recover_as_message_response)
}

pub(crate) async fn handle_shutdown(
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

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    /// Creates routes for tests, the ipfs will not work as no background task is being spawned.
    async fn testing_routes(
    ) -> impl warp::Filter<Extract = impl warp::Reply, Error = Infallible> + Clone {
        use super::routes;
        use ipfs::{IpfsOptions, UninitializedIpfs};

        let options = IpfsOptions::inmemory_with_generated_keys();

        let (ipfs, fut) = UninitializedIpfs::new(options).await.start().await.unwrap();
        drop(fut);
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
        drop(shutdown_rx);

        routes(&ipfs, shutdown_tx)
    }

    #[tokio::test]
    async fn not_found_as_plaintext() {
        let routes = testing_routes().await;
        let resp = warp::test::request()
            .method("GET")
            .path("/api/v0/id_foobar")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), 404);
        // from go-ipfs
        assert_eq!(resp.body(), "404 page not found");
    }

    #[tokio::test]
    async fn invalid_peer_id_as_messageresponse() {
        let routes = testing_routes().await;
        let resp = warp::test::request()
            .method("GET")
            .path("/api/v0/id?arg=foobar")
            .reply(&routes)
            .await;

        assert_eq!(resp.status(), 400);
        // from go-ipfs
        assert_eq!(
            resp.body(),
            r#"{"Message":"invalid peer id","Code":0,"Type":"error"}"#
        );
    }
}
