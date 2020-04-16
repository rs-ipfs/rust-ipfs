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
    let format = match query.format.as_deref().unwrap_or("dag-cbor") {
        "dag-cbor" => Codec::DagCBOR,
        "dag-pb" => Codec::DagProtobuf,
        "dag-json" => Codec::DagJSON,
        "raw" => Codec::Raw,
        _ => return Err(StringError::from("unknown codec").into()),
    };
    let hasher = match query.hash.as_deref().unwrap_or("sha2-256") {
        "sha2-256" => multihash::Sha2_256::digest,
        "sha2-512" => multihash::Sha2_512::digest,
        "sha3-512" => multihash::Sha3_512::digest,
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
    let cid = Cid::new_v1(format, digest);
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
