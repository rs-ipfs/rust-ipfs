use crate::v0::support::{with_ipfs, InvalidMultipartFormData, StringError};
use futures::stream::StreamExt;
use ipfs::{Ipfs, IpfsTypes};
use libipld::cid::{Cid, Codec};
use serde::Deserialize;
use serde_json::json;
use warp::{multipart, path, query, reply, Buf, Filter, Rejection, Reply};

#[derive(Debug, Deserialize)]
pub struct PutQuery {
    format: Option<String>,
    hash: Option<String>,
}

async fn put_query<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    query: PutQuery,
    mut form: multipart::FormData,
) -> Result<impl Reply, Rejection> {

    use multihash::{Multihash, Sha2_256, Sha2_512, Sha3_512};

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
    let mut buf = form
        .next()
        .await
        .ok_or(InvalidMultipartFormData)?
        .map_err(|_| InvalidMultipartFormData)?
        .data()
        .await
        .ok_or(InvalidMultipartFormData)?
        .map_err(|_| InvalidMultipartFormData)?;
    let data = buf.to_bytes().as_ref().to_vec().into_boxed_slice();
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
    let block = ipfs::Block { cid, data };
    ipfs.put_block(block).await.map_err(StringError::from)?;
    Ok(reply::json(&reply))
}

pub fn put<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    path!("dag" / "put")
        .and(with_ipfs(ipfs))
        .and(query::<PutQuery>())
        .and(multipart::form())
        .and_then(put_query)
}
