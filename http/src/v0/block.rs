use crate::v0::support::{with_ipfs, HandledErr, StreamResponse, StringError};
use bytes::Buf;
use futures::stream::{FuturesOrdered, Stream, StreamExt, TryStreamExt};
use ipfs::error::Error;
use ipfs::{Ipfs, IpfsTypes};
use libipld::cid::{Cid, Codec, Version};
use mime::Mime;
use mpart_async::server::MultipartStream;
use multihash::Multihash;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use warp::{http::Response, path, query, reply, Filter, Rejection, Reply};

mod options;
use options::RmOptions;

#[derive(Debug, Deserialize)]
pub struct GetQuery {
    arg: String,
}

async fn get_query<T: IpfsTypes>(ipfs: Ipfs<T>, query: GetQuery) -> Result<impl Reply, Rejection> {
    let cid: Cid = query.arg.parse().map_err(StringError::from)?;
    let data = ipfs
        .get_block(&cid)
        .await
        .map_err(StringError::from)?
        .into_vec();

    let response = Response::builder().body(data);
    Ok(response)
}

pub fn get<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("block" / "get")
        .and(with_ipfs(ipfs))
        .and(query::<GetQuery>())
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
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("block" / "put")
        .and(with_ipfs(ipfs))
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

    let buffer = try_only_named_multipart(&["data", "file"], 1024 * 1024, boundary, body).await?;

    // bad thing about Box<[u8]>: converting to it forces an reallocation
    let data = buffer.into_boxed_slice();

    let digest = opts.digest()?(&data);
    let cid = Cid::new(opts.version()?, opts.format()?, digest).map_err(StringError::from)?;

    let size = data.len();
    let key = cid.to_string();

    let block = ipfs::Block { cid, data };

    ipfs.put_block(block).await.map_err(StringError::from)?;

    Ok(reply::json(&serde_json::json!({
        "Key": key,
        "Size": size,
    })))
}

pub async fn try_only_named_multipart<'a>(
    allowed_names: &'a [impl AsRef<str> + 'a],
    size_limit: usize,
    boundary: String,
    st: impl Stream<Item = Result<impl Buf, warp::Error>> + Unpin + 'a,
) -> Result<Vec<u8>, Rejection> {
    use bytes::Bytes;
    let mut stream =
        MultipartStream::new(Bytes::from(boundary), st.map_ok(|mut buf| buf.to_bytes()));

    // store the first good field here; optimally this would just be an Option but couldn't figure
    // out a way to handle the "field matched", "field not matched" cases while supporting empty
    // fields.
    let mut buffer = Vec::new();
    let mut matched = false;

    while let Some(mut field) = stream.try_next().await.map_err(StringError::from)? {
        // [ipfs http api] says we should expect a "data" but examples use "file" as the
        // form field name. newer conformance tests also use former, older latter.
        //
        // [ipfs http api]: https://docs.ipfs.io/reference/http/api/#api-v0-block-put

        let name = field
            .name()
            .map_err(|_| StringError::from("invalid field name"))?;

        let mut target = if allowed_names.iter().any(|s| s.as_ref() == name) {
            Some(&mut buffer)
        } else {
            None
        };

        if matched {
            // per spec: only one block should be uploaded at once
            return Err(StringError::from("multiple blocks (expecting at most one)").into());
        }

        matched = target.is_some();

        loop {
            let next = field.try_next().await.map_err(|e| {
                StringError::from(format!("IO error while reading field bytes: {}", e))
            })?;

            match (next, target.as_mut()) {
                (Some(bytes), Some(target)) => {
                    if target.len() + bytes.len() > size_limit {
                        return Err(StringError::from("block is too large").into());
                    } else if target.is_empty() {
                        target.reserve(size_limit);
                    }
                    target.extend_from_slice(bytes.as_ref());
                }
                (Some(bytes), None) => {
                    // noop: we must fully consume the part before moving on to next.
                    if bytes.is_empty() {
                        // this technically shouldn't be happening any more but erroring
                        // out instead of spinning wildly is much better.
                        return Err(StringError::from("internal error: zero read").into());
                    }
                }
                (None, _) => break,
            }
        }
    }

    if !matched {
        return Err(StringError::from("missing field: \"data\" (or \"file\")").into());
    }

    Ok(buffer)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct RmResponse {
    error: String,
    hash: String,
}

#[derive(Serialize, Deserialize)]
pub struct EmptyResponse {}

pub fn rm<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("block" / "rm")
        .and(with_ipfs(ipfs))
        .and(rm_options())
        .and_then(rm_query)
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
                log::error!("edge serialization failed: {}", e);
                Err(HandledErr)
            }
        });

    let st = futures::stream::iter(responses);
    Ok(StreamResponse(st))
}

#[derive(Debug, Deserialize)]
pub struct StatQuery {
    arg: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct StatResponse {
    key: String,
    size: usize,
}

async fn stat_query<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    query: StatQuery,
) -> Result<impl Reply, Rejection> {
    let cid: Cid = query.arg.parse().map_err(StringError::from)?;
    let block = ipfs.get_block(&cid).await.map_err(StringError::from)?;
    let response = StatResponse {
        key: query.arg,
        size: block.data().len(),
    };
    Ok(reply::json(&response))
}

pub fn stat<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("block" / "stat")
        .and(with_ipfs(ipfs))
        .and(query::<StatQuery>())
        .and_then(stat_query)
}
