use crate::v0::support::option_parsing::ParseError;
use crate::v0::support::{with_ipfs, StringError, StringSerialized};
use ipfs::{Cid, Ipfs, IpfsTypes, PinKind, PinMode};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::str::FromStr;
use warp::{Filter, Rejection, Reply};

mod add;

/// `pin/add` per https://docs.ipfs.io/reference/http/api/#api-v0-pin-add or the
/// interface-ipfs-http test suite.
pub fn add<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs)
        .and(add::add_request())
        .and_then(add::add_inner)
}

#[derive(Debug)]
struct ListRequest {
    // FIXME: should be Vec<IpfsPath>
    arg: Vec<Cid>,
    filter: PinFilter,
    // FIXME: not sure if this is used
    quiet: bool,
    // FIXME copypaste
    stream: bool,
    timeout: Option<humantime::Duration>,
}

impl<'a> TryFrom<&'a str> for ListRequest {
    type Error = ParseError<'a>;

    fn try_from(q: &'a str) -> Result<Self, Self::Error> {
        use ParseError::*;

        // TODO: similar arg with duplicates question as always

        let parse = url::form_urlencoded::parse(q.as_bytes());
        let mut args = Vec::new();
        let mut filter = None;
        let mut quiet = None;
        let mut stream = None;
        let mut timeout = None;

        for (key, value) in parse {
            let target =
                match &*key {
                    "arg" => {
                        // FIXME: this should be IpfsPath
                        args.push(
                            Cid::try_from(&*value)
                                .map_err(|e| ParseError::InvalidCid("arg".into(), e))?,
                        );
                        continue;
                    }
                    "type" => {
                        if filter.is_none() {
                            // not parsing this the whole way as there might be hope to have this
                            // function removed in the future.
                            filter =
                                Some(value.parse::<PinFilter>().map_err(|e| {
                                    ParseError::InvalidValue("type".into(), e.into())
                                })?);
                            continue;
                        } else {
                            return Err(DuplicateField(key));
                        }
                    }
                    "timeout" => {
                        if timeout.is_none() {
                            timeout =
                                Some(value.parse().map_err(|e| {
                                    ParseError::InvalidDuration("timeout".into(), e)
                                })?);
                            continue;
                        } else {
                            return Err(DuplicateField(key));
                        }
                    }
                    "quiet" => &mut quiet,
                    "stream" => &mut stream,
                    _ => {
                        // ignore unknown fields
                        continue;
                    }
                };

            if target.is_none() {
                match value.parse::<bool>() {
                    Ok(value) => *target = Some(value),
                    Err(_) => return Err(InvalidBoolean(key, value)),
                }
            } else {
                return Err(DuplicateField(key));
            }
        }

        // special case compared to others: it's ok not to have any args, or any filter

        Ok(ListRequest {
            arg: args,
            filter: filter.unwrap_or_default(),
            quiet: quiet.unwrap_or(false),
            // this default was mentioned in the pin/ls api
            stream: quiet.unwrap_or(true),
            timeout,
        })
    }
}

#[derive(Debug)]
enum PinFilter {
    Direct,
    Indirect,
    Recursive,
    All,
}

impl FromStr for PinFilter {
    // the not understood input
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use PinFilter::*;
        Ok(match s {
            "all" => All,
            "direct" => Direct,
            "recursive" => Recursive,
            "indirect" => Indirect,
            other => return Err(other.to_owned()),
        })
    }
}

impl Default for PinFilter {
    fn default() -> Self {
        PinFilter::All
    }
}

impl PinFilter {
    fn to_mode(&self) -> Option<PinMode> {
        use PinFilter::*;
        match self {
            Direct => Some(PinMode::Direct),
            Indirect => Some(PinMode::Indirect),
            Recursive => Some(PinMode::Recursive),
            All => None,
        }
    }
}

/// `pin/ls` as per https://docs.ipfs.io/reference/http/api/#api-v0-pin-ls
pub fn list<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs).and(list_options()).and_then(list_inner)
}

fn list_options() -> impl Filter<Extract = (ListRequest,), Error = Rejection> + Clone {
    warp::filters::query::raw().and_then(|q: String| {
        let res = ListRequest::try_from(q.as_str())
            .map_err(StringError::from)
            .map_err(warp::reject::custom);

        futures::future::ready(res)
    })
}

async fn list_inner<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    req: ListRequest,
) -> Result<impl Reply, Rejection> {
    use futures::stream::{StreamExt, TryStreamExt};

    #[derive(serde::Serialize)]
    struct Good {
        #[serde(rename = "Cid")]
        cid: StringSerialized<Cid>,
        #[serde(rename = "Type")]
        mode: Cow<'static, str>,
    }

    impl From<(Cid, Cow<'static, str>)> for Good {
        fn from((cid, mode): (Cid, Cow<'static, str>)) -> Good {
            Good {
                cid: StringSerialized(cid),
                mode,
            }
        }
    }

    if req.arg.is_empty() {
        let st = ipfs.list_pins(req.filter.to_mode()).await;

        if req.stream {
            let st = st.map_ok(|(cid, mode)| {
                Good::from((
                    cid,
                    Cow::Borrowed(match mode {
                        PinMode::Direct => "direct",
                        PinMode::Indirect => "indirect",
                        PinMode::Recursive => "recursive",
                    }),
                ))
            });

            Ok(format_json_newline(st))
        } else {
            // TODO: the non stream variant looks like the http docs one:
            // { "Keys": { "cid": { "Type": "indirect" } } }
            // See https://github.com/rs-ipfs/rust-ipfs/issues/351
            Err(crate::v0::NotImplemented.into())
        }
    } else {
        // the variant where we are not actually listing anything but more about finding the
        // specific cids

        let requirement = req.filter.to_mode();

        let details = ipfs
            .query_pins(req.arg, requirement)
            .await
            .map_err(StringError::from)?;

        if req.stream {
            let st = futures::stream::iter(details)
                .map(Ok::<_, std::convert::Infallible>) // only done trying to match the types
                .map_ok(|(cid, kind)| {
                    Good::from((
                        cid,
                        match kind {
                            PinKind::Recursive(_) | PinKind::RecursiveIntention => {
                                "recursive".into()
                            }
                            PinKind::Direct => "direct".into(),
                            PinKind::IndirectFrom(cid) => {
                                format!("indirect through {}", cid).into()
                            }
                        },
                    ))
                });

            Ok(format_json_newline(st))
        } else {
            // TODO: same as above
            Err(crate::v0::NotImplemented.into())
        }
    }
}

fn format_json_newline<St, T, E>(st: St) -> warp::http::Response<warp::hyper::Body>
where
    St: futures::stream::Stream<Item = Result<T, E>> + Send + 'static,
    T: Serialize + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    use bytes::{BufMut, BytesMut};
    use futures::stream::StreamExt;

    let mut buffer = BytesMut::with_capacity(256);
    let st = st.map(move |res| match res {
        Ok(good) => {
            serde_json::to_writer((&mut buffer).writer(), &good)
                .expect("no component should fail serialization");

            buffer.put_u8(b'\n');

            Ok::<_, std::convert::Infallible>(buffer.split().freeze())
        }
        Err(e) => {
            // FIXME: this is non-standard once again
            serde_json::to_writer(
                (&mut buffer).writer(),
                &serde_json::json!({ "Err": e.to_string() }),
            )
            .unwrap();
            buffer.put_u8(b'\n');

            // the stream should end on first error
            Ok(buffer.split().freeze())
        }
    });

    crate::v0::support::StreamResponse(st).into_response()
}

#[derive(Debug, Serialize)]
struct ListResponse<'a> {
    #[serde(rename = "Keys")]
    keys: HashMap<StringSerialized<Cid>, TypeDecorator<'a>>,
}

#[derive(Debug, Serialize)]
struct TypeDecorator<'a> {
    #[serde(rename = "Type")]
    kind: Cow<'a, str>,
}

#[derive(Debug, Deserialize)]
struct RemoveRequest {
    // FIXME: go-ipfs supports multiple pin removals on single request however for us it does not
    // seem like a possibility at least for now. Not tested by conformance tests as far as I can
    // see.
    arg: StringSerialized<Cid>,
    // Not mentioned in the API docs but this is accepted
    // Defaults to true which will remove both recursive and direct.
    recursive: Option<bool>,
}

pub fn rm<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs)
        .and(warp::query::<RemoveRequest>())
        .and_then(rm_inner)
}

async fn rm_inner<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    req: RemoveRequest,
) -> Result<impl Reply, Rejection> {
    ipfs.remove_pin(req.arg.as_ref(), req.recursive.unwrap_or(true))
        .await
        .map_err(StringError::from)?;

    Ok(warp::reply::json(&RemoveResponse {
        pins: vec![req.arg],
    }))
}

#[derive(Debug, Serialize)]
struct RemoveResponse {
    #[serde(rename = "Pins")]
    pins: Vec<StringSerialized<Cid>>,
}
