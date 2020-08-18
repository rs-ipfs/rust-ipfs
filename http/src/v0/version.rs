use serde::{Deserialize, Serialize};

/// The /api/v0/version parses but does not change output according to these values in the query
/// string. Defined in https://docs-beta.ipfs.io/reference/http/api/#api-v0-version. Included here
/// mostly as experimentation on how to do query parameters.
#[derive(Debug, Deserialize)]
pub struct Query {
    number: Option<bool>,
    commit: Option<bool>,
    repo: Option<bool>,
    all: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct Response {
    version: &'static str,
    commit: &'static str,
    // repo is here for go-ipfs and js-ipfs but we do not have full repo at the moment
    // empty string is accepted by the tests
    repo: &'static str,
    #[serde(rename = "ipfs-http-client")]
    ipfs_http_client: &'static str,
    #[serde(rename = "interface-ipfs-core")]
    interface_ipfs_core: &'static str,
}

// https://docs-beta.ipfs.io/reference/http/api/#api-v0-version
// Note: the parameter formatting is only verified, feature looks to be unimplemented for `go-ipfs
// 0.4.23` and handled by cli. This is not compatible with `rust-ipfs-api`.
pub fn version(
    _query: Query,
) -> impl std::future::Future<Output = Result<(impl warp::Reply,), std::convert::Infallible>> {
    let response = Response {
        version: env!("CARGO_PKG_VERSION"), // TODO: move over to rust-ipfs not to worry about syncing version numbers?
        commit: env!("VERGEN_SHA_SHORT"),
        repo: "",
        ipfs_http_client: "",
        interface_ipfs_core: "",
    };

    futures::future::ready(Ok((warp::reply::json(&response),)))
}
