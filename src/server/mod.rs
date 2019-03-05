#![allow(dead_code)]
use super::{Block, Ipfs, IpfsTypes, Cid};
use warp::{filters::BoxedFilter, Filter, Rejection, Reply};
use warp::http::{Response, status::StatusCode, header::CONTENT_TYPE};
use std::sync::{Arc, Mutex};
use futures::compat::Compat;
use futures::{Future, TryFutureExt};
use super::ipld::Ipld;
use super::unixfs::unixfs::Data as UnixfsData;
use protobuf;

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

fn serve_block(block: Block, path: String, etag: String) -> Response<Vec<u8>> {
    
    let mut builder = Response::builder();

    builder.status(200)
        .header("ETag", etag);

    if path.ends_with(".html") {
        builder.header(CONTENT_TYPE, "text/html");
    }

    if let Ok(Ipld::Object(hp)) = Ipld::from(&block) {
        if let Some(Ipld::Bytes(d)) = hp.get(&"Data".to_owned()) {
            let data = protobuf::parse_from_bytes::<UnixfsData>(d)
                .map(|mut u| u.take_Data())
                .unwrap_or_else(|_| d.to_vec());
            return builder
                .body(data)
                .expect("Body never fails on first call")
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
                let mut full_path = tail.as_str().split("/");
                let mut cid = item.clone();
                let mut prev_filename: String = "index.html".to_owned();
                loop {
                    let block = await!(load_block(&cid, service.clone()))?;
                    let path = full_path
                        .next()
                        .map(|s|s.to_owned())
                        .unwrap_or_else(|| "index.html".to_owned());

                    if let Ok(ref ipld) = convert_block(&block) {
                        if let Some(file_id) = find_item(ipld, path.clone()) {
                            cid = file_id.clone();
                            prev_filename = path;
                            continue
                        }
                    }
                    if full_path.next().is_some() {
                        // no more lookups, but still some path -> fail
                        return Err(warp::reject::not_found())
                    } else {
                        return Ok(serve_block(block, prev_filename, item.to_string()))
                    }
                }
            };
            Compat::new(Box::pin(load_inner()))
        })
        .boxed()
}
