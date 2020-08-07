use crate::v0::support::{
    try_only_named_multipart, with_ipfs, HandledErr, MaybeTimeoutExt, StreamResponse, StringError,
    StringSerialized,
};
use bytes::Buf;
use cid::{Cid, Codec, Version};
use futures::stream::{FuturesOrdered, Stream, StreamExt};
use ipfs::error::Error;
use ipfs::{Ipfs, IpfsTypes};
use mime::Mime;

use multihash::Multihash;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use warp::{http::Response, query, reply, Filter, Rejection, Reply};

mod options;
use options::RmOptions;

// This is parsed for both `block/get` and `block/stat`. Should there be any need to add stuff to
// either which isn't relevant in the other, go ahead and split these again.
#[derive(Debug, Deserialize)]
pub struct GetStatOptions {
    arg: String,
    timeout: Option<StringSerialized<humantime::Duration>>,
}

async fn get_query<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    query: GetStatOptions,
) -> Result<impl Reply, Rejection> {
    let cid: Cid = query.arg.parse().map_err(StringError::from)?;
    let data = ipfs
        .get_block(&cid)
        .maybe_timeout(query.timeout.map(StringSerialized::into_inner))
        .await
        .map_err(StringError::from)?
        .map_err(StringError::from)?
        .into_vec();

    let response = Response::builder().body(data);
    Ok(response)
}

pub fn get<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs)
        .and(query::<GetStatOptions>())
        .and_then(get_query)
}

#[derive(Debug, Deserialize)]
pub struct PutQuery {
    format: Option<String>,
    mhtype: Option<String>,
    version: Option<u8>,
}

impl PutQuery {
    fn format(&self) -> Result<Codec, Rejection> {
        Ok(match self.format.as_deref().unwrap_or("dag-pb") {
            "dag-cbor" => Codec::DagCBOR,
            "dag-pb" => Codec::DagProtobuf,
            "dag-json" => Codec::DagJSON,
            "raw" => Codec::Raw,
            _ => return Err(StringError::from("unknown codec").into()),
        })
    }

    fn digest(&self) -> Result<fn(&'_ [u8]) -> Multihash, Rejection> {
        Ok(match self.mhtype.as_deref().unwrap_or("sha2-256") {
            "sha2-256" => multihash::Sha2_256::digest,
            "sha2-512" => multihash::Sha2_512::digest,
            _ => return Err(StringError::from("unknown hash").into()),
        })
    }

    fn version(&self) -> Result<Version, Rejection> {
        Ok(match self.version.unwrap_or(0) {
            0 => Version::V0,
            1 => Version::V1,
            _ => return Err(StringError::from("invalid cid version").into()),
        })
    }
}

pub fn put<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs)
        .and(query::<PutQuery>())
        .and(warp::header::<Mime>("content-type")) // TODO: rejects if missing
        .and(warp::body::stream())
        .and_then(inner_put)
}

async fn inner_put<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    opts: PutQuery,
    mime: Mime,
    body: impl Stream<Item = Result<impl Buf, warp::Error>> + Unpin,
) -> Result<impl Reply, Rejection> {
    let boundary = mime
        .get_param("boundary")
        .map(|v| v.to_string())
        .ok_or_else(|| StringError::from("missing 'boundary' on content-type"))?;

    let data = try_only_named_multipart(&["data", "file"], 1024 * 1024, boundary, body)
        .await
        .map_err(StringError::from)?;

    // FIXME: digest calculation should be done in line with the reception of new blocks, but
    // because of the old multihash version we use, we don't at least yet have access to that api.
    let digest = opts.digest()?(&data);

    // cid generation can fail if we try some other hash or format with cidv0 which only supports
    // SHA2-256 and dag-pb, both are even implicit. could be that these parameters we use here are
    // not supported by go-ipfs or something, as in, the `block/put` should mostly return CidV0.
    // Haven't researched this deeply.
    let cid = Cid::new(opts.version()?, opts.format()?, digest).map_err(StringError::from)?;

    // bad thing about Box<[u8]>: converting to it forces a reallocation; do it now that we know
    // the cid was generated successfully.
    let data = data.into_boxed_slice();

    let size = data.len();
    let key = cid.to_string();

    let block = ipfs::Block { cid, data };

    ipfs.put_block(block).await.map_err(StringError::from)?;

    Ok(reply::json(&serde_json::json!({
        "Key": key,
        "Size": size,
    })))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct RmResponse {
    error: String,
    hash: String,
}

#[derive(Serialize, Deserialize)]
pub struct EmptyResponse;

pub fn rm<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs).and(rm_options()).and_then(rm_query)
}

fn rm_options() -> impl Filter<Extract = (RmOptions,), Error = Rejection> + Clone {
    warp::filters::query::raw().and_then(|q: String| {
        let res = RmOptions::try_from(q.as_str())
            .map_err(StringError::from)
            .map_err(warp::reject::custom);

        futures::future::ready(res)
    })
}

async fn rm_query<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    options: RmOptions,
) -> Result<impl Reply, Rejection> {
    use futures::future::TryFutureExt;

    let RmOptions { args, force, quiet } = options;

    let cids = args
        .into_iter()
        .map(|s| Cid::try_from(s.as_str()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(StringError::from)?;

    let futs: FuturesOrdered<_> = cids
        .into_iter()
        .map(|cid| ipfs.remove_block(cid.clone()).map_err(move |e| (cid, e)))
        .collect();

    let responses = futs
        .collect::<Vec<Result<Cid, (Cid, Error)>>>()
        .await
        .into_iter()
        .map(move |result| match result {
            Ok(cid) => RmResponse {
                hash: cid.to_string(),
                error: "".to_string(),
            },
            Err((cid, e)) => RmResponse {
                hash: cid.to_string(),
                error: if force { "".to_string() } else { e.to_string() },
            },
        })
        .map(|response: RmResponse| serde_json::to_string(&response))
        .map(move |result| match result {
            Ok(mut string) => {
                if quiet {
                    string = "".to_string();
                } else {
                    string.push('\n');
                }
                Ok(string.into_bytes())
            }
            Err(e) => {
                error!("edge serialization failed: {}", e);
                Err(HandledErr)
            }
        });

    let st = futures::stream::iter(responses);
    Ok(StreamResponse(st))
}

pub fn stat<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs)
        .and(query::<GetStatOptions>())
        .and_then(stat_query)
}

async fn stat_query<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    query: GetStatOptions,
) -> Result<impl Reply, Rejection> {
    let cid: Cid = query.arg.parse().map_err(StringError::from)?;
    let block = ipfs
        .get_block(&cid)
        .maybe_timeout(query.timeout.map(StringSerialized::into_inner))
        .await
        .map_err(StringError::from)?
        .map_err(StringError::from)?;

    Ok(reply::json(&serde_json::json!({
        "Key": query.arg,
        "Size": block.data().len(),
    })))
}
