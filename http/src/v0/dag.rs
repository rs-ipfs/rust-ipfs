use crate::v0::support::{
    try_only_named_multipart, with_ipfs, MaybeTimeoutExt, NotImplemented, StringError,
    StringSerialized,
};
use cid::{Cid, Codec};
use futures::stream::Stream;
use ipfs::Ipfs;
use mime::Mime;

use serde::Deserialize;
use serde_json::json;
use warp::{path, query, reply, Buf, Filter, Rejection, Reply};

#[derive(Debug, Deserialize)]
pub struct PutQuery {
    format: Option<String>,
    hash: Option<String>,
    #[serde(rename = "input-enc", default)]
    encoding: InputEncoding,
}

#[derive(PartialEq, Eq, Debug, Deserialize)]
pub enum InputEncoding {
    #[serde(rename = "raw")]
    Raw,
    #[serde(rename = "json")]
    Json,
}

impl Default for InputEncoding {
    fn default() -> Self {
        InputEncoding::Json
    }
}

pub fn put(ipfs: &Ipfs) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("dag" / "put")
        .and(with_ipfs(ipfs))
        .and(query::<PutQuery>())
        .and(warp::header::<Mime>("content-type")) // TODO: rejects if missing
        .and(warp::body::stream())
        .and_then(put_query)
}

async fn put_query(
    ipfs: Ipfs,
    query: PutQuery,
    mime: Mime,
    body: impl Stream<Item = Result<impl Buf, warp::Error>> + Unpin,
) -> Result<impl Reply, Rejection> {
    use multihash::{Multihash, Sha2_256, Sha2_512, Sha3_512};

    if query.encoding != InputEncoding::Raw {
        return Err(NotImplemented.into());
    }

    let (format, v0_fmt) = match query.format.as_deref().unwrap_or("dag-cbor") {
        "dag-cbor" => (Codec::DagCBOR, false),
        "dag-pb" => (Codec::DagProtobuf, true),
        "dag-json" => (Codec::DagJSON, false),
        "raw" => (Codec::Raw, false),
        _ => return Err(StringError::from("unknown codec").into()),
    };

    let (hasher, v0_hash) = match query.hash.as_deref().unwrap_or("sha2-256") {
        "sha2-256" => (Sha2_256::digest as fn(&[u8]) -> Multihash, true),
        "sha2-512" => (Sha2_512::digest as fn(&[u8]) -> Multihash, false),
        "sha3-512" => (Sha3_512::digest as fn(&[u8]) -> Multihash, false),
        _ => return Err(StringError::from("unknown hash").into()),
    };

    let boundary = mime
        .get_param("boundary")
        .map(|v| v.to_string())
        .ok_or_else(|| StringError::from("missing 'boundary' on content-type"))?;

    let data = try_only_named_multipart(&["data", "file"], 1024 * 1024, boundary, body)
        .await
        .map_err(StringError::from)?;

    let digest = hasher(&data);

    let cid = if v0_fmt && v0_hash {
        // this is quite ugly way but apparently js-ipfs generates a v0 cid for this combination
        // which is also created by go-ipfs
        Cid::new_v0(digest).expect("cidv0 creation cannot fail for dag-pb and sha2-256")
    } else {
        Cid::new_v1(format, digest)
    };

    let reply = json!({
        "Cid": { "/": cid.to_string() }
    });

    // delay reallocation until cid has been generated
    let data = data.into_boxed_slice();
    let block = ipfs::Block { cid, data };
    ipfs.put_block(block).await.map_err(StringError::from)?;
    Ok(reply::json(&reply))
}

/// Per https://docs-beta.ipfs.io/reference/http/api/#api-v0-block-resolve this endpoint takes in a
/// path and resolves it to the last block (the cid), and to the path inside the final block
/// (rempath).
pub fn resolve(ipfs: &Ipfs) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("dag" / "resolve")
        .and(with_ipfs(ipfs))
        .and(query::<ResolveOptions>())
        .and_then(inner_resolve)
}

#[derive(Debug, Deserialize)]
struct ResolveOptions {
    arg: String,
    timeout: Option<StringSerialized<humantime::Duration>>,
}

async fn inner_resolve(ipfs: Ipfs, opts: ResolveOptions) -> Result<impl Reply, Rejection> {
    use crate::v0::refs::{walk_path, IpfsPath};
    use std::convert::TryFrom;

    let path = IpfsPath::try_from(opts.arg.as_str()).map_err(StringError::from)?;

    let (current, _, remaining) = walk_path(&ipfs, path)
        .maybe_timeout(opts.timeout.map(StringSerialized::into_inner))
        .await
        .map_err(StringError::from)?
        .map_err(StringError::from)?;

    let remaining = {
        let slashes = remaining.len();
        let mut buf =
            String::with_capacity(remaining.iter().map(|s| s.len()).sum::<usize>() + slashes);

        for piece in remaining.into_iter().rev() {
            if !buf.is_empty() {
                buf.push('/');
            }
            buf.push_str(&piece);
        }

        buf
    };

    Ok(reply::json(&json!({
        "Cid": { "/": current.to_string() },
        "RemPath": remaining,
    })))
}
