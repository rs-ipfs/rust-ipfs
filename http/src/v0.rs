use ipfs::{Ipfs, IpfsTypes};
use std::convert::Infallible;

pub mod bitswap;
pub mod block;
pub mod id;
pub mod pubsub;
pub mod swarm;
pub mod version;

pub mod support;
pub use support::recover_as_message_response;
pub(crate) use support::{with_ipfs, InvalidPeerId, NotImplemented, StringError};

pub fn routes<T>(
    ipfs: &Ipfs<T>,
    shutdown_tx: tokio::sync::mpsc::Sender<()>,
) -> impl warp::Filter<Extract = impl warp::Reply, Error = Infallible> + Clone
where
    T: IpfsTypes,
{
    use warp::{query, Filter};
    // /api/v0/shutdown
    let shutdown = warp::post()
        .and(warp::path!("shutdown"))
        .and(warp::any().map(move || shutdown_tx.clone()))
        .and_then(handle_shutdown);

    let mount = warp::path("api").and(warp::path("v0"));

    let api = mount.and(
        shutdown
            .or(id::identity(ipfs))
            // Placeholder paths
            // https://docs.rs/warp/0.2.2/warp/macro.path.html#path-prefixes
            .or(warp::path!("add").and_then(not_implemented))
            .or(bitswap::wantlist(ipfs))
            .or(bitswap::stat(ipfs))
            .or(block::get(ipfs))
            .or(block::put(ipfs))
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
            .or(pubsub::routes(ipfs))
            .or(warp::path!("refs" / ..).and_then(not_implemented))
            .or(warp::path!("repo" / ..).and_then(not_implemented))
            .or(warp::path!("stats" / ..).and_then(not_implemented))
            .or(swarm::connect(ipfs))
            .or(swarm::peers(ipfs))
            .or(swarm::addrs(ipfs))
            .or(swarm::addrs_local(ipfs))
            .or(swarm::disconnect(ipfs))
            .or(warp::path!("version")
                .and(query::<version::Query>())
                .and_then(version::version)),
    );

    api.recover(recover_as_message_response)
}

pub async fn handle_shutdown(
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

        let options = IpfsOptions::inmemory_with_generated_keys(false);

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
