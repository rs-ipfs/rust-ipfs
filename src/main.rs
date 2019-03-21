#![feature(async_await, await_macro, futures_api)]
use ipfs::{serve, IpfsService, Ipfs, IpfsOptions, Types};
use ipfs::{tokio_run, tokio_spawn};


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

        let ipfs_service = IpfsService::new(ipfs);

        let addr = ([0, 0, 0, 0], 8081);
        println!("Listening on {:?}", addr);
        await!(serve(ipfs_service, addr)).unwrap();
    });
}