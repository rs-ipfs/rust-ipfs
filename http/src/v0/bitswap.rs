use crate::v0::support::{with_ipfs, InvalidPeerId, StringError};
use ipfs::{BitswapStats, Ipfs, IpfsTypes};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use warp::{query, reply, Filter, Rejection, Reply};

#[derive(Debug, Deserialize)]
pub struct WantlistQuery {
    peer: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct WantlistResponse {
    keys: Vec<Value>,
}

async fn wantlist_query<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    query: WantlistQuery,
) -> Result<impl Reply, Rejection> {
    let peer_id = if let Some(peer_id) = query.peer {
        let peer_id = peer_id.parse().map_err(|_| InvalidPeerId)?;
        Some(peer_id)
    } else {
        None
    };
    let cids = ipfs
        .bitswap_wantlist(peer_id)
        .await
        .map_err(StringError::from)?;
    let keys = cids
        .into_iter()
        .map(|(cid, _)| json!({"/": cid.to_string()}))
        .collect();
    let response = WantlistResponse { keys };
    Ok(reply::json(&response))
}

pub fn wantlist<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs)
        .and(query::<WantlistQuery>())
        .and_then(wantlist_query)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct StatResponse {
    blocks_received: u64,
    blocks_sent: u64,
    data_received: u64,
    data_sent: u64,
    dup_blks_received: u64,
    dup_data_received: u64,
    messages_received: u64,
    peers: Vec<String>,
    provide_buf_len: i32,
    wantlist: Vec<Value>,
}

impl From<BitswapStats> for StatResponse {
    fn from(stats: BitswapStats) -> Self {
        let wantlist = stats
            .wantlist
            .into_iter()
            .map(|(cid, _)| json!({"/": cid.to_string()}))
            .collect();
        let peers = stats
            .peers
            .into_iter()
            .map(|peer_id| peer_id.to_string())
            .collect();
        Self {
            blocks_received: stats.blocks_received,
            blocks_sent: stats.blocks_sent,
            data_received: stats.data_received,
            data_sent: stats.data_sent,
            dup_blks_received: stats.dup_blks_received,
            dup_data_received: stats.dup_data_received,
            peers,
            wantlist,
            messages_received: 0,
            provide_buf_len: 0,
        }
    }
}

async fn stat_query<T: IpfsTypes>(ipfs: Ipfs<T>) -> Result<impl Reply, Rejection> {
    let stats: StatResponse = ipfs
        .bitswap_stats()
        .await
        .map_err(StringError::from)?
        .into();
    Ok(reply::json(&stats))
}

pub fn stat<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs).and_then(stat_query)
}
