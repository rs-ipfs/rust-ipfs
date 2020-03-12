use super::MessageKind;
use ipfs::{Ipfs, IpfsTypes, PeerId};
use serde::{Deserialize, Serialize};

// NOTE: go-ipfs accepts an -f option for format (unsure if same with Accept: request header),
// unsure on what values -f takes, since `go-ipfs` seems to just print the value back? With plain http
// requests the `f` or `format` is ignored. Perhaps it's a cli functionality. This should return a
// json body, which does get pretty formatted by the cli.
//
// FIXME: Reference has argument `arg: PeerId` which does get processed.
//
// https://docs.ipfs.io/reference/api/http/#api-v0-id
pub async fn identity<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    query: Query,
) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    use multibase::Base::Base64Pad;
    use std::str::FromStr;

    if let Some(peer_id) = query.arg {
        match PeerId::from_str(&peer_id) {
            Ok(_) => {
                // FIXME: probably find this peer, if a match, use identify protocol
                return Ok(Box::new(warp::http::StatusCode::NOT_IMPLEMENTED));
            }
            Err(_) => {
                // FIXME: we need to customize the query deserialization error
                return Ok(Box::new(warp::reply::with_status(
                    MessageKind::Error
                        .with_code(0)
                        .with_message("invalid peer id")
                        .to_json_reply(),
                    warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                )));
            }
        }
    }

    match ipfs.identity().await {
        Ok((public_key, addresses)) => {
            let id = public_key.clone().into_peer_id().to_string();
            let public_key = Base64Pad.encode(public_key.into_protobuf_encoding());

            let response = Response {
                id,
                public_key,
                addresses: addresses.into_iter().map(|addr| addr.to_string()).collect(),
                agent_version: "rust-ipfs/0.1.0",
                protocol_version: "ipfs/0.1.0",
            };

            // TODO: investigate how this could be avoided, perhaps by making the ipfs::Error a
            // Reject
            Ok(Box::new(warp::reply::json(&response)))
        }
        Err(e) => Ok(Box::new(warp::reply::with_status(
            MessageKind::Error
                .with_code(0)
                .with_message(e.to_string())
                .to_json_reply(),
            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        ))),
    }
}

/// Query string of /api/v0/id?arg=peerid&format=notsure
#[derive(Debug, Deserialize)]
pub struct Query {
    // the peer id to query
    arg: Option<String>,
    // this does not seem to be reacted to by go-ipfs
    format: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct Response {
    // PeerId
    #[serde(rename = "ID")]
    id: String,
    // looks like Base64
    public_key: String,
    // Multiaddrs
    addresses: Vec<String>,
    // Multiaddr alike <agent_name>/<version>, like rust-ipfs/0.0.1
    agent_version: &'static str,
    // Multiaddr alike ipfs/0.1.0 ... not sure if there are plans to bump this anytime soon
    protocol_version: &'static str,
}
