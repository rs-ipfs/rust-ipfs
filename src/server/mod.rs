#![allow(dead_code)]
// use ipfs::{Block, Ipfs, IpfsOptions, RepoTypes, SwarmTypes, IpfsTypes};
use super::{Block, Ipfs, IpfsTypes, Cid};
use warp::{filters::BoxedFilter, Filter, Rejection, Reply, http::Response};
use std::sync::{Arc, Mutex};
use futures::compat::Compat;
use futures::{Future, TryFutureExt};
use super::ipld::Ipld;

pub type IpfsService<T> = Arc<Mutex<Ipfs<T>>>; 

fn load_block<T: IpfsTypes>(item: &Cid, service: IpfsService<T>)
    -> impl Future<Output=Result<Block, Rejection>>
{
    service
        .lock()
        .unwrap()
        .get_block(item)
        .map_err(|_err| warp::reject::not_found())
}

pub fn serve_ipfs<T: IpfsTypes>(ipfs_service: IpfsService<T>)
    -> BoxedFilter<(impl Reply,)> {
    warp::path::param::<Cid>()
        .and(warp::path::end())
        .and(warp::any().map(move || ipfs_service.clone()))
        // just fetch and return the block
        .and_then(|c, s| Compat::new(Box::pin(load_block(&c, s))))
        .map(|block: Block| Response::builder()
            .status(200)
            .header("ETag", block.cid().to_string())
            .body::<String>(block.into())
        )
        .boxed()
}

pub fn load_file<T: IpfsTypes>(ipfs_service: IpfsService<T>)
    -> BoxedFilter<(impl Reply,)>
{
    let convert_block = |b: &Block| Ipld::from(b);

    warp::any()
        .map(move || ipfs_service.clone())
        .and(warp::path::param::<Cid>()) // CiD
        .and(warp::path::tail()) // everything after
        .and_then(move |service: IpfsService<T>, item: Cid, tail: warp::path::Tail | {
            let load_inner = async move || {
                // TODO: recursive support
                let path = Ipld::String(tail.as_str().to_owned());
                let block = await!(load_block(&item, service.clone()))?;

                match convert_block(&block) {
                    Ok(Ipld::Object(hp)) => {
                        if let Some(Ipld::Array(ref links)) = hp.get(&"Links".to_owned()) {
                            if let Some(Ipld::Object(entry)) = links
                                .iter()
                                .find(|e| {
                                    if let Ipld::Object(hp) = e {
                                        if Some(&path) == hp.get(&"name".to_owned()) {
                                            return true
                                        }
                                    }
                                    false
                                })
                            {
                                if let Some(Ipld::Cid(cid)) = entry.get(&"cid".to_owned()) {

                                    return await!(load_block(&cid, service.clone()))
                                        .map_err(|_err| warp::reject::not_found())
                                }
                            }
                        }
                        Err(warp::reject::not_found())
                    },
                    _ => Err(warp::reject::not_found())
                }
            };
            Compat::new(Box::pin(load_inner()))
        })
        .map(|block: Block| Response::builder()
            .status(200)
            .header("x-ben", "yo")
            .body::<String>(block.into())
        )
        .boxed()
}
