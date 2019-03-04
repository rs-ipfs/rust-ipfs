#![feature(async_await, await_macro, futures_api)]
use ipfs::{Ipfs, IpfsOptions, Types};
use ipfs::server::{gen_routes, IpfsService};
use ipfs::{tokio_run, tokio_spawn};
use futures::compat::Compat01As03;
use std::sync::{Arc, Mutex};
use warp;

fn main() {
    let options = IpfsOptions::<Types>::default();
    env_logger::Builder::new().parse(&options.ipfs_log).init();
    let mut ipfs = Ipfs::<Types>::new(options);


    tokio_run(async move {
        let fut = ipfs.start_daemon().unwrap();
        tokio_spawn(fut);
        await!(ipfs.init_repo()).unwrap();
        await!(ipfs.open_repo()).unwrap();
        // Set the address to run our socket on.

        let ipfs_service : IpfsService<Types> = Arc::new(Mutex::new(ipfs));

        let addr = ([127, 0, 0, 1], 8081);
        let routes = gen_routes(ipfs_service);
        println!("Listening on {:?}", addr);
        await!(Compat01As03::new(warp::serve(routes).bind(addr))).unwrap();
    });
}