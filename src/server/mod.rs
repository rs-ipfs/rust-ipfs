#![allow(dead_code)]
use super::{Block, Ipfs, IpfsTypes, Cid};
use warp::{self, filters::BoxedFilter, Filter, Rejection, Reply};
use warp::http::{Response, status::StatusCode, HeaderMap, header::CONTENT_TYPE};
use std::sync::{Arc, Mutex};
use futures::compat::{Compat, Compat01As03};
use futures::{Future, TryFutureExt};
use super::ipld::Ipld;
use super::unixfs::unixfs::{Data_DataType, Data as UnixfsData};
use protobuf;
use cid::ToCid;
use std::net::SocketAddr;

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
        .header("ETag", etag)
        .header("x-ipfs-cid", block.cid().to_string());

    if path.ends_with(".html") {
        builder.header(CONTENT_TYPE, "text/html");
    }

    builder
        .body(extract_block(block))
        .expect("Body never fails on first call")
}

fn moved(cid: &Cid) -> Response<String> {
    let url = format!("/ipfs/{}", cid.to_string());

    Response::builder()
        .status(StatusCode::MOVED_PERMANENTLY)
        .header("Location", url.clone())
        .header(CONTENT_TYPE, "text/html")
        .body(format!("<a href=\"{}\">This page has moved permanently</a>", url))
            .expect("Body never fails on first attempt")
}

fn extract_block(block: Block) -> Vec<u8> {
    match Ipld::from(&block) {
        Ok(ref i) => extract_file_data(i),
        _ => None
    }.unwrap_or_else(move || {
        let (_cid, data) = block.into_inner();
        data
    }
}

fn extract_file_data<'d>(ipld: &'d Ipld) -> Option<Vec<u8>> {
    match ipld {
        Ipld::Bytes(b) => {
            protobuf::parse_from_bytes::<UnixfsData>(&b)
                .map(|mut u| u.take_Data()).ok()
        },
        Ipld::Object(hp) => match hp.get(&"Data".to_owned()) {
                Some(i) => extract_file_data(i),
                _ => None
            },
        _ => None
    }
}

async fn fetch_file<'d, T: IpfsTypes>(ipfs_service: IpfsService<T>, ipld: &'d Ipld)
    -> Result<Vec<u8>, Rejection>
{
    if let Ipld::Object(hp) = ipld {
        if let Some(Ipld::Bytes(d)) = hp.get(&"Data".to_owned()) {
            if let Ok(item) = protobuf::parse_from_bytes::<UnixfsData>(d) {
                if item.get_Type() == Data_DataType::File && item.get_blocksizes().len() > 0
                {
                    if let Some(Ipld::Array(links)) = hp.get(&"Links".to_owned()) {
                        let name = &"".to_owned();
                        let mut buffer : Vec<u8> = vec![];
                        // TODO: can we somehow return this iterator to hyper
                        //       and have it chunk-transfer the data?
                        for cid in links
                            .iter()
                            .filter_map(move |x|
                                if let Ipld::Object(l) = x {
                                    match (l.get(&"Name".to_owned()), l.get(&"Hash".to_owned())) {
                                        (Some(Ipld::String(n)),
                                            Some(Ipld::Cid(cid))) if n == name => Some(cid),
                                        _=> None
                                    }
                                } else {
                                    None
                                }
                        ) {
                            let block = await!(load_block(cid, ipfs_service.clone()))?;
                            let mut data = extract_block(block);
                            buffer.append(&mut data)
                        }

                        return Ok(buffer)
                    }
                }

                return Ok(item.get_Data().into())
            } else {
                return Ok(d.to_vec())
            }
        }
    };

    Err(warp::reject::not_found())
}

fn find_item<'d>(ipld: &'d Ipld, path: String) -> Option<&'d Cid> {
    let wrapped_path = Ipld::String(path);
    if let Ipld::Object(hp) = ipld {
        if let Some(Ipld::Array(ref links)) = hp.get(&"Links".to_owned()) {
            for e in links.iter() {
                if let Ipld::Object(hp) = e {
                    if  hp.get(&"Name".to_owned()) != Some(&wrapped_path) {
                        continue // object has a different name
                    }
                    if let Some(Ipld::Cid(cid)) = hp.get(&"Hash".to_owned()) {
                        return Some(cid)
                    }
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
        .and(warp::header::headers_cloned())
        .and_then(move |
            service: IpfsService<T>,
            item: Cid,
            tail: warp::path::Tail,
            header: HeaderMap
        | {
            let load_inner = async move || {
                if let Some(val) = header.get("If-None-Match") {
                    if let Ok(etag) = val.as_bytes().to_cid() {
                        if etag == item {
                            return Ok(Response::builder()
                                .status(StatusCode::NOT_MODIFIED)
                                .body(Vec::default()).expect("Body call doesn't fail first time"))
                        }
                    }
                }
                let mut full_path = tail.as_str().split("/");
                let mut cid = item.clone();
                let mut prev_filename: String = "index.html".to_owned();

                // iterate through the path, finding the deepest entry
                let last = loop {
                    let block = await!(load_block(&cid, service.clone()))?;
                    let path = full_path
                        .next()
                        .map(|s|s.to_owned())
                        .unwrap_or_else(|| "index.html".to_owned());
                    let ipld = convert_block(&block);

                    match ipld {
                        Ok(ipld) => {
                            if let Some(file_id) = find_item(&ipld, path.clone()) {
                                cid = file_id.clone();
                                prev_filename = path;
                                continue
                            } else if full_path.next().is_some() {
                                // no more lookups, but still some path -> fail
                                return Err(warp::reject::not_found())
                            } else {
                                // no more entries, this is the last we want
                                break ipld
                            }
                        }
                        _ => {
                            if full_path.next().is_some() {
                                // no more lookups, but still some path -> fail
                                return Err(warp::reject::not_found())
                            } else {
                                return Ok(serve_block(block, prev_filename, item.to_string()))
                            }
                        }
                    }
                };

                await!(fetch_file(service, &last)).map(move |data| {

                    let mut builder = Response::builder();

                    builder.status(200)
                        .header("ETag", item.to_string())
                        .header("x-ipfs-cid", cid.to_string());

                    if prev_filename.ends_with(".html") {
                        builder.header(CONTENT_TYPE, "text/html");
                    }

                    Ok(builder.body(data).expect("Body never fails on first call"))
                })?
            };
            Compat::new(Box::pin(load_inner()))
        })
        .boxed()
}


pub fn serve<T: IpfsTypes>(service: IpfsService<T>, addr: impl Into<SocketAddr> + 'static)
    -> impl Future<Output = Result<(), ()>> + 'static
{
    let routes = warp::path("ipfs")
        .and(serve_ipfs(service));
    Compat01As03::new(warp::serve(routes).bind(addr))
}