use async_std::{
    future::{pending, timeout},
    task,
};
use futures::future::{select, Either, FutureExt};
use futures::future::{AbortHandle, Abortable};
use libipld::Cid;

use std::{
    convert::TryFrom,
    future::Future,
    thread,
    time::{Duration, Instant},
};

fn bounded_retry<Fun, Fut, F, T>(
    n_times: usize,
    sleep_between: Duration,
    mut future: Fun,
    check: F,
) -> Result<usize, Duration>
where
    Fun: FnMut() -> Fut,
    Fut: Future<Output = T>,
    F: Fn(T) -> bool,
{
    let started = Instant::now();
    for n in 0..n_times {
        if check(futures::executor::block_on(future())) {
            return Ok(n);
        }
        thread::sleep(sleep_between);
    }

    Err(started.elapsed())
}

/// Check if canceling a Cid affects the wantlist.
#[async_std::test]
async fn wantlist_cancellation() {
    // start a single node
    let opts = ipfs::IpfsOptions::inmemory_with_generated_keys(false);
    let (ipfs, ipfs_fut) = ipfs::UninitializedIpfs::new(opts)
        .await
        .start()
        .await
        .unwrap();
    let _fut_task = task::spawn(ipfs_fut);

    // execute a get_block request
    let cid = Cid::try_from("QmSoLPppuBtQSGwKDZT2M73ULpjvfd3aZ6ha4oFGL1KaGa").unwrap();

    // start a get_request future
    let ipfs_clone = ipfs.clone();
    let cid_clone = cid.clone();
    let (abort_handle1, abort_reg) = AbortHandle::new_pair();
    let abortable_req = Abortable::new(
        async move { ipfs_clone.get_block(&cid_clone).await },
        abort_reg,
    );
    let _get_request1 = task::spawn(abortable_req);

    // verify that the requested Cid is in the wantlist
    let wantlist_populated = bounded_retry(
        3,
        Duration::from_millis(200),
        || ipfs.bitswap_wantlist(None),
        |ret| ret.unwrap().get(0).map(|x| &x.0) == Some(&cid),
    );

    assert!(
        wantlist_populated.is_ok(),
        "the wantlist is still empty after the request was issued"
    );

    // fire up an additional get request, this time within the same async task...
    let ipfs_clone = ipfs.clone();
    let cid_clone = cid.clone();
    let get_request2 = ipfs_clone.get_block(&cid_clone);
    let get_timeout = timeout(Duration::from_millis(200), pending::<()>());
    let get_request2 = match select(get_timeout.boxed(), get_request2.boxed()).await {
        Either::Left((_, fut)) => fut,
        Either::Right(_) => unreachable!(),
    };

    // ...and an additional one within the same task, for good measure
    let ipfs_clone = ipfs.clone();
    let cid_clone = cid.clone();
    let get_request3 = ipfs_clone.get_block(&cid_clone);
    let get_timeout = timeout(Duration::from_millis(200), pending::<()>());
    let get_request3 = match select(get_timeout.boxed(), get_request3.boxed()).await {
        Either::Left((_, fut)) => fut,
        Either::Right(_) => unreachable!(),
    };

    // cancel the first requested Cid
    abort_handle1.abort();

    // verify that the requested Cid is still in the wantlist
    let wantlist_partially_cleared1 = bounded_retry(
        3,
        Duration::from_millis(200),
        || ipfs.bitswap_wantlist(None),
        |ret| ret.unwrap().len() == 1,
    );

    assert!(
        wantlist_partially_cleared1.is_ok(),
        "the wantlist is empty despite there still being 2 live get requests"
    );

    // cancel the second requested Cid
    drop(get_request2);

    // verify that the requested Cid is STILL in the wantlist
    let wantlist_partially_cleared2 = bounded_retry(
        3,
        Duration::from_millis(200),
        || ipfs.bitswap_wantlist(None),
        |ret| ret.unwrap().len() == 1,
    );

    assert!(
        wantlist_partially_cleared2.is_ok(),
        "the wantlist is empty despite there still being a live get request"
    );

    // cancel the second requested Cid
    drop(get_request3);

    // verify that the requested Cid is no longer in the wantlist
    let wantlist_cleared = bounded_retry(
        3,
        Duration::from_millis(200),
        || ipfs.bitswap_wantlist(None),
        |ret| ret.unwrap().is_empty(),
    );

    assert!(
        wantlist_cleared.is_ok(),
        "a block was not removed from the wantlist after all its subscriptions had died"
    );
}
