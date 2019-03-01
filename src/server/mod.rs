#![allow(dead_code)]
// use ipfs::{Block, Ipfs, IpfsOptions, RepoTypes, SwarmTypes, IpfsTypes};
use super::{Block,Ipfs, IpfsTypes, Cid};
use std::str::FromStr;
use warp::{Filter, Rejection};
use std::sync::{Arc, Mutex};
use futures::compat::Compat;
use futures::TryFutureExt;

pub type IpfsService<T> = Arc<Mutex<Ipfs<T>>>; 

/// A newtype to enforce our maximum allowed seconds.
struct Seconds(u64);

impl FromStr for Seconds {
    type Err = ();
    fn from_str(src: &str) -> Result<Self, Self::Err> {
        src.parse::<u64>().map_err(|_| ()).and_then(|num| {
            if num <= 5 {
                Ok(Seconds(num))
            } else {
                Err(())
            }
        })
    }
}

pub fn gen_routes<T: IpfsTypes>(ipfs_service: IpfsService<T>)
    -> impl Filter<Extract = (String, ), Error = Rejection>
{

    let ipfs_service = warp::any().map(move || ipfs_service.clone());

    let ipfs = warp::path("ipfs");
    let ipfs_item = ipfs.and(warp::path::param::<Cid>());

    warp::get2()
        .and(ipfs_item)
        .and(ipfs_service)
        .and_then(move |item: Cid, service: IpfsService<T> | {
            Compat::new(Box::pin(service
                    .lock()
                    .unwrap()
                    .get_block(&item)
                    .map_err(|_err| warp::reject::not_found())
            ))
        }).map(|block: Block| block.into())
        

    // // Match `/:u32`...
    // warp::path::param()
    //     // and_then create a `Future` that will simply wait N seconds...
    //     .and_then(|Seconds(seconds), _ipfs| {
    //         Delay::new(Instant::now() + Duration::from_secs(seconds))
    //             // return the number of seconds again...
    //             .map(move |()| seconds)
    //             // An error from `Delay` means a big problem with the server...
    //             .map_err(|timer_err| {
    //                 eprintln!("timer error: {}", timer_err);
    //                 warp::reject::custom(timer_err)
    //             })
    //     })
    //     .map(|seconds| format!("I waited {} seconds!", seconds))
}
