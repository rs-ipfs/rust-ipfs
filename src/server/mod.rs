#![allow(dead_code)]
// use ipfs::{Block, Ipfs, IpfsOptions, RepoTypes, SwarmTypes, IpfsTypes};
use super::{Block, Ipfs, IpfsTypes, Cid};
use warp::{filters::BoxedFilter, Filter, Rejection, Reply};
use warp::http::{Response, status::StatusCode, header::CONTENT_TYPE};
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

fn serve_block(block: Block, path: String) -> Response<Vec<u8>> {
    
    let mut builder = Response::builder();

    builder.status(200)
        .header("ETag", block.cid().to_string());

    if path.ends_with(".html") {
        builder.header(CONTENT_TYPE, "text/html");
    }
    
    if let Ok(Ipld::Object(hp)) = Ipld::from(&block) {
        println!("Could parse for {:}", path);
        if let Some(Ipld::Bytes(d)) = hp.get(&"Data".to_owned()) {
            println!("only serving data");
            return builder.body(d.to_vec()).expect("Body never fails on first call")
        }
    }

    let (_cid, data) = block.into_inner();

    builder.body(data).expect("Body never fails on first call")
}

fn moved(cid: &Cid) -> Response<String> {
    let url = format!("/ipfs/{}", cid.to_string());
    println!("moved to: {}", url);

    Response::builder()
        .status(StatusCode::MOVED_PERMANENTLY)
        .header("Location", url.clone())
        .header(CONTENT_TYPE, "text/html")
        .body(format!("<a href=\"{}\">This page has moved permanently</a>", url))
            .expect("Body never fails on first attempt")
}

fn find_item<'d>(ipld: &'d Ipld, path: String) -> Option<&'d Cid> {
    let wrapped_path = Ipld::String(path);
    if let Ipld::Object(hp) = ipld {
        if let Some(Ipld::Array(ref links)) = hp.get(&"Links".to_owned()) {
            if let Some(Ipld::Object(entry)) = links
                .iter()
                .find(|e| {
                    if let Ipld::Object(hp) = e {
                        if Some(&wrapped_path) == hp.get(&"Name".to_owned()) {
                            return true
                        }
                    }
                    false
                })
            {
                if let Some(Ipld::Cid(cid)) = entry.get(&"Hash".to_owned()) {
                    return Some(cid)
                }
            }
        }
    }
    None
}

// pub fn serve_ipfs<T: IpfsTypes>(ipfs_service: IpfsService<T>)
//     -> BoxedFilter<(impl Reply,)> {
//     warp::path::param::<Cid>()
//         .and(warp::path::end())
//         .and(warp::any().map(move || ipfs_service.clone()))
//         // just fetch and return the block
//         .and_then(|c, s| Compat::new(Box::pin(load_block(&c, s))))
//         .map(|b| serve_block(b, "index.html".to_owned()))
//         .boxed()
// }

pub fn serve_ipfs<T: IpfsTypes>(ipfs_service: IpfsService<T>)
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
                let mut full_path = tail.as_str().split("/");
                let mut cid = item;
                loop {
                    let path : String = full_path.next().unwrap_or("index.html").to_owned();
                    let block = await!(load_block(&cid, service.clone()))?;

                    if let Ok(ref ipld) = convert_block(&block) {
                        if let Some(file_id) = find_item(ipld, path.clone()) {
                            cid = file_id.clone();
                            continue
                        }
                    }
                    if full_path.next().is_some() {
                        // no more lookups, but still some path?
                        return Err(warp::reject::not_found())
                    } else {
                        return Ok(serve_block(block, path))
                    }
                }
            };
            Compat::new(Box::pin(load_inner()))
        })
        .boxed()
}
