#![feature(async_await, await_macro, futures_api)]
use ipfs::{Ipfs, IpfsOptions, RepoTypes, SwarmTypes, IpfsTypes};
use ipfs::server::{gen_routes, IpfsService};
use ipfs::{tokio_run, tokio_spawn};
use futures::compat::Compat01As03;
use std::sync::{Arc, Mutex};
use warp;

#[derive(Clone)]
struct Types;

unsafe impl Send for Types {}
unsafe impl Sync for Types {}

impl RepoTypes for Types {
    type TBlockStore = ipfs::repo::mem::MemBlockStore;
    type TDataStore = ipfs::repo::mem::MemDataStore;
}

impl SwarmTypes for Types {
    type TStrategy = ipfs::bitswap::strategy::AltruisticStrategy<Self>;
}

impl IpfsTypes for Types {}

fn main() {
    let options = IpfsOptions::new();
    env_logger::Builder::new().parse(&options.ipfs_log).init();
    let mut ipfs = Ipfs::<Types>::new(options);


    tokio_run(async move {
        tokio_spawn(ipfs.start_daemon());
        await!(ipfs.init_repo()).unwrap();
        await!(ipfs.open_repo()).unwrap();
        // Set the address to run our socket on.

        let ipfs_service : IpfsService<Types> = Arc::new(Mutex::new(ipfs));

        let addr = ([127, 0, 0, 1], 8081);
        let routes = gen_routes(ipfs_service);
        println!("Listening on {:?}", addr);
        await!(Compat01As03::new(warp::serve(routes).bind(addr)));
    });
}