#![allow(dead_code)]
use super::{Block, BlockLoader, Ipfs, IpfsTypes, Cid};
use warp::{self, filters::BoxedFilter, Filter, Rejection, Reply};
use warp::http::{Response, status::StatusCode, HeaderMap, header::CONTENT_TYPE};
use std::sync::{Arc, Mutex};
use futures::compat::{Compat, Compat01As03};
use crate::error::Error;
use futures::{self, Future};
use super::ipld::Ipld;
use cid::ToCid;
use std::net::SocketAddr;
use std::pin::Pin;
use super::unixfs::{extract_block, fetch_file};

#[derive(Clone)]
pub struct IpfsService<T: IpfsTypes>(Arc<Mutex<Ipfs<T>>>);

impl<T: IpfsTypes> IpfsService<T> {
    pub fn new(ipfs: Ipfs<T>) -> Self {
        IpfsService(Arc::new(Mutex::new(ipfs)))
    }
}

impl From<Error> for Rejection {
    #[inline]
    fn from(_error: Error) -> Rejection {
        // TODO: actually understand the error and return something smarter
        warp::reject::not_found()
    }
}

impl<T> BlockLoader for IpfsService<T> where T: IpfsTypes {
    existential type Fut: Future<Output=Result<Block, Error>>;

    fn load_block(&self, item: &Cid) -> Self::Fut {
        self.0
            .lock()
            .unwrap()
            .get_block(item)
    }
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

pub fn moved(cid: &Cid) -> impl warp::Reply {
    let url = format!("/ipfs/{}", cid.to_string());

    Response::builder()
        .status(StatusCode::TEMPORARY_REDIRECT)
        .header("Location", url.clone())
        .header(CONTENT_TYPE, "text/html")
        .body(format!("<a href=\"{}\">This page has moved permanently</a>", url))
            .expect("Body never fails on first attempt")
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

pub fn ipfs_responder<'d, T: IpfsTypes>(
    service: IpfsService<T>,
    item: Cid,
    tail: warp::path::Tail,
    header: HeaderMap
) -> Compat<Pin<Box<impl Future<Output=Result<impl Reply + 'd, Rejection>>>>>
{
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
        let mut prev_filename = INDEX.to_owned() ;

        // iterate through the path, finding the deepest entry
        let last = loop {
            let block = await!(service.load_block(&cid))?;
            let path = {
                if let Some(s) = full_path.next() {
                    if s.len() > 0 {
                        s
                    } else {
                        INDEX
                    }
                } else {
                    INDEX
                }
            }.to_owned();
            let ipld = Ipld::from(&block);

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

        await!(fetch_file(&last, service.clone())).map(move |data| {

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
}

const INDEX: &'static str = "index.html";

pub fn serve_ipfs<T: IpfsTypes>(ipfs_service: IpfsService<T>)
    -> BoxedFilter<(impl Reply,)>
{

    warp::any()
        .map(move || ipfs_service.clone())
        .and(warp::path::param::<Cid>()) // CiD
        .and(warp::path::tail()) // everything after
        .and(warp::header::headers_cloned())
        .and_then(ipfs_responder)
        .boxed()
}


pub fn serve<T: IpfsTypes>(service: IpfsService<T>, addr: impl Into<SocketAddr> + 'static)
    -> impl Future<Output = Result<(), ()>> + 'static
{
    let routes = warp::path("ipfs")
        .and(serve_ipfs(service));
    Compat01As03::new(warp::serve(routes).bind(addr))
}