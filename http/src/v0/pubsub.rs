use warp::Filter;
use warp::hyper::body::Bytes;
use serde::{Serialize, Deserialize};
use futures::stream::TryStream;
use tokio::sync::Mutex;
use tokio::sync::broadcast;

use std::fmt;
use std::collections::HashMap;
use std::sync::Arc;
use ipfs::{Ipfs, IpfsTypes};
use super::support::{with_ipfs, StringError};

#[derive(Default)]
pub struct Pubsub {
    subscriptions: Mutex<HashMap<String, broadcast::Sender<Result<PreformattedJsonMessage, StreamError>>>>,
}

pub fn routes<T: IpfsTypes>(ipfs: &Ipfs<T>) -> impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    peers(ipfs)
        .or(list_subscriptions(ipfs))
        .or(publish(ipfs))
        .or(subscribe(ipfs, Default::default()))
}

pub fn peers<T: IpfsTypes>(ipfs: &Ipfs<T>) -> impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("peers")
        .and(warp::get().or(warp::post()))
        .unify()
        .and(with_ipfs(ipfs))
        .and(warp::query::<OptionalTopicParameter>().map(|tp: OptionalTopicParameter| tp.topic))
        .and_then(inner_peers)
}

async fn inner_peers<T: IpfsTypes>(ipfs: Ipfs<T>, topic: Option<String>) -> Result<impl warp::Reply, warp::Rejection>{
    let peers = ipfs.pubsub_peers(topic.as_ref().map(String::as_str)).await
        .map_err(|e| warp::reject::custom(StringError::from(e)))?;

    Ok(warp::reply::json(&StringListResponse { strings: peers.into_iter().map(|id| id.to_string()).collect() }))
}

pub fn list_subscriptions<T: IpfsTypes>(ipfs: &Ipfs<T>) -> impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("ls")
        .and(warp::get().or(warp::post()))
        .unify()
        .and(with_ipfs(ipfs))
        .and_then(inner_ls)
}

async fn inner_ls<T: IpfsTypes>(ipfs: Ipfs<T>) -> Result<impl warp::Reply, warp::Rejection> {
    let topics = ipfs.pubsub_subscribed()
        .await
        .map_err(|e| warp::reject::custom(StringError::from(e)))?;

    Ok(warp::reply::json(&StringListResponse { strings: topics }))
}

pub fn publish<T: IpfsTypes>(ipfs: &Ipfs<T>) -> impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("pub")
        .and(warp::post())
        .and(with_ipfs(ipfs))
        .and(publish_args(b"arg"))
        .and_then(inner_publish)
}

async fn inner_publish<T: IpfsTypes>(ipfs: Ipfs<T>, PublishArgs { topic, message }: PublishArgs) -> Result<impl warp::Reply, warp::Rejection> {
    ipfs.pubsub_publish(&topic, &message.into_inner())
        .await
        .map_err(|e| warp::reject::custom(StringError::from(e)))?;
    Ok(warp::reply::reply())
}


pub fn subscribe<T: IpfsTypes>(ipfs: &Ipfs<T>, pubsub: Arc<Pubsub>) -> impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("sub")
        .and(warp::get().or(warp::post()))
        .unify()
        .and(with_ipfs(ipfs))
        .and(warp::any().map(move || pubsub.clone()))
        .and(warp::query::<TopicParameter>())
        .and_then(|ipfs, pubsub, TopicParameter { topic }| async move {
            Ok::<_, warp::Rejection>(StreamResponse(inner_subscribe(ipfs, pubsub, topic).await))
        })
}

async fn inner_subscribe<T: IpfsTypes>(ipfs: Ipfs<T>, pubsub: Arc<Pubsub>, topic: String) -> impl TryStream<Ok = PreformattedJsonMessage, Error = StreamError> {
    use std::collections::hash_map::Entry;
    use futures::stream::{StreamExt, TryStreamExt};
    // accessing this through mutex bets on "most accesses would need write access" as in most
    // requests would be asking for new subscriptions, which would require RwLock upgrading
    // from write, which is not supported operation either.
    let mut guard = pubsub.subscriptions.lock().await;

    let rx = match guard.entry(topic) {
        Entry::Occupied(oe) => {
            // the easiest case: just join in, even if there are no other subscribers at the
            // moment
            oe.get().subscribe()
        }
        Entry::Vacant(ve) => {

            let topic = ve.key().clone();

            // the returned stream needs to be set up to be shoveled in a background task
            let mut shoveled = ipfs.pubsub_subscribe(&topic).await
                .expect("new subscriptions shouldn't fail while holding the lock");

            // using broadcast channel should allow us have N concurrent subscribes and
            // preformatted json should give us good enough performance
            let (tx, rx) = broadcast::channel::<Result<PreformattedJsonMessage, StreamError>>(4);

            // this will be used to create more subscriptions
            ve.insert(tx.clone());

            let pubsub = Arc::clone(&pubsub);

            // FIXME: handling this all efficiently in single task would require getting a
            // stream of "all streams" from ipfs::p2p::Behaviour ... perhaps one could be added
            // alongside the current "spread per topic" somehow?
            tokio::spawn(async move {
                loop {
                    loop {
                        let next = match shoveled.next().await {
                            Some(next) => preformat(next),
                            // stop shoveling
                            None => break,
                        };

                        if tx.send(next).is_err() {
                            // currently no more subscribers
                            break;
                        }
                    }

                    let mut guard = pubsub.subscriptions.lock().await;

                    // as this can take a long time to acquire the mutex, we might get a new
                    // subscriber in the between

                    if let Entry::Occupied(oe) = guard.entry(topic.clone()) {
                        if oe.get().receiver_count() > 0 {
                            log::trace!("got a new subscriber between removing");
                            // a new subscriber has appeared since our previous send failure.
                            // it is possible it gets dropped while we hold the lock, but then
                            // we will eventually exit the next await ... FIXME: perhaps there
                            // should be a timeout?
                            continue;
                        }
                        // really no more subscribers, unsubscribe and terminate the shoveling
                        // task for this stream. racing this will have to happen through mutex.
                        oe.remove();
                        return;
                    } else {
                        unreachable!("only way to remove subscriptions from
                            ipfs-http::v0::pubsub::Pubsub is through tasks exiting");
                    }
                }
            });

            rx
        }
    };

    rx.into_stream().map(|res| res.map_err(|_| StreamError::Recv).and_then(|res| res))
}

#[derive(Debug, Clone)]
enum StreamError {
    Serialization,
    Recv
}

impl fmt::Display for StreamError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StreamError::Serialization => write!(fmt, "failed to serialize received message"),
            StreamError::Recv => write!(fmt, "consuming the stream too slowly")
        }
    }
}

impl std::error::Error for StreamError {}

#[derive(Debug, Serialize)]
struct PubsubHttpApiMessage {
    from: String,
    data: String,
    seqno: String,
    #[serde(rename = "topicIDs")]
    topics: Vec<String>,
}

impl<T> From<T> for PubsubHttpApiMessage
    where T: AsRef<ipfs::PubsubMessage>
{
    fn from(msg: T) -> Self {
        use multibase::Base::Base64Pad;
        let msg = msg.as_ref();

        let from = Base64Pad.encode(msg.source.as_bytes());
        let data = Base64Pad.encode(&msg.data);
        let seqno = Base64Pad.encode(&msg.sequence_number);
        let topics = msg.topics.clone();

        PubsubHttpApiMessage {
            from,
            data,
            seqno,
            topics
        }
    }
}

/// Bytes backed preformatted json + newline for subscription response stream.
#[derive(Clone)]
struct PreformattedJsonMessage(Bytes);

impl From<Bytes> for PreformattedJsonMessage {
    fn from(b: Bytes) -> Self {
        Self(b)
    }
}

impl Into<Bytes> for PreformattedJsonMessage {
    fn into(self) -> Bytes {
        self.0
    }
}

/// Formats the given pubsub message into json and a newline, as is the subscription format.
fn preformat(msg: impl AsRef<ipfs::PubsubMessage>) -> Result<PreformattedJsonMessage, StreamError> {
    serde_json::to_vec(&PubsubHttpApiMessage::from(msg))
        .map(|mut vec| { vec.push(b'\n'); vec })
        .map(Bytes::from)
        .map(PreformattedJsonMessage::from)
        .map_err(|e| {
            log::error!("failed to serialize PubsubMessage: {}", e);
            StreamError::Serialization
        })
}

struct StreamResponse<S>(S);

impl<S> warp::Reply for StreamResponse<S>
    where S: futures::stream::TryStream + Send + Sync + 'static,
          S::Ok: Into<Bytes>,
          S::Error: std::error::Error + Send + Sync + 'static
{
    fn into_response(self) -> warp::reply::Response {
        use warp::hyper::Body;
        use futures::stream::TryStreamExt;
        use warp::http::header::{HeaderValue, TRAILER, CONTENT_TYPE};

        // while it may seem like the S::Error is handled somehow it currently just means the
        // response will stop. hopefully later it can be used to become trailer headers.
        let mut resp = warp::reply::Response::new(Body::wrap_stream(self.0.into_stream()));
        let headers = resp.headers_mut();

        // FIXME: unable to send this header with warp/hyper right now
        headers.insert(TRAILER, HeaderValue::from_static("X-Stream-Error"));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert("X-Chunked-Output", HeaderValue::from_static("1"));

        resp
    }
}

#[derive(Debug, Deserialize)]
struct TopicParameter {
    #[serde(rename = "arg")]
    topic: String,
}

#[derive(Debug, Deserialize)]
struct OptionalTopicParameter {
    #[serde(rename = "arg")]
    topic: Option<String>,
}

// GET|POST /pubsub/peers or /pubsub/peers/?arg=topic
//fn peers(topic: Option<String>) {}

// GET|POST /pubsub/pub
//  - two args in query: topic and query: ?arg=topic&arg=msg
//    - not sure if msgs can be newline delimited
//  - topic in query, newline delimited msgs in body: ?arg=topic
//    - still unsure on the content splitting, how does it work with "any bytes"
//fn publish(topic: String, msg: Vec<u8>) {}

// GET|POST /pubsub/sub?arg=topic
//  - persistent subscription
//  - needs header Trailer: X-Stream-Error
//    - we cannot end stream with error with hyper and warp at the moment
//fn subscribe(topic: String) {}

// GET|[POST??] /pubsub/ls lists the ongoing local subscriptions
//fn ls() {}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct StringListResponse {
    strings: Vec<String>
}

#[derive(Debug)]
struct PublishArgs {
    topic: String,
    message: QueryOrBody,
}

#[derive(Debug)]
enum QueryOrBody {
    Query(Vec<u8>),
    #[allow(dead_code)]
    Body(Vec<u8>),
}

impl QueryOrBody {
    fn into_inner(self) -> Vec<u8> {
        match self {
            Self::Query(x) | Self::Body(x) => x,
        }
    }
}

/// `parameter_name` is bytes slice because there is no percent decoding done for that component.
fn publish_args(parameter_name: &'static [u8]) -> impl warp::Filter<Extract = (PublishArgs, ), Error = warp::Rejection> + Copy {
    warp::filters::query::raw()
        .and_then(move |s: String| {
            let ret = if s.is_empty() {
                Err(warp::reject::custom(RequiredArgumentMissing(b"topic")))
            } else {
                // sadly we can't use url::form_urlencoded::parse here as it will do lossy
                // conversion to utf8 without us being able to recover the raw bytes, which are
                // used by js-ipfs/ipfs-http-client to encode raw Buffers:
                // https://github.com/ipfs/js-ipfs/blob/master/packages/ipfs-http-client/src/pubsub/publish.js
                let mut args = QueryAsRawPartsParser { input: s.as_bytes() }
                    .filter(|&(k, _)| k == parameter_name)
                    .map(|t| t.1);

                let first = args.next()
                    // can't be missing
                    .ok_or_else(|| warp::reject::custom(RequiredArgumentMissing(b"arg")))
                    // decode into Result<String, warp::Rejection>
                    .and_then(|raw_first| percent_encoding::percent_decode(raw_first)
                        .decode_utf8()
                        .map(|cow| cow.into_owned())
                        .map_err(|_| warp::reject::custom(NonUtf8Topic))
                    );

                first.map(move |first| {
                    // continue to second arg, which may or may not be present
                    let second = args.next()
                        .map(|slice| percent_encoding::percent_decode(slice).collect::<Vec<_>>())
                        .map(QueryOrBody::Query);

                    (first, second)
                })
            };

            futures::future::ready(ret)
        })
        .and_then(|(topic, opt_arg) : (String, Option<QueryOrBody>)| {
            let ret = if let Some(message) = opt_arg {
                Ok(PublishArgs { topic, message })
            } else {
                // this branch should check for multipart body, however the js-http client is not
                // using that so we can leave it probably for now. Looks like warp doesn't support
                // multipart bodies without Content-Length so `go-ipfs` is not supported at this
                // time.
                Err(warp::reject::custom(RequiredArgumentMissing(b"data")))
            };
            futures::future::ready(ret)
        })
}

#[derive(Debug)]
struct NonUtf8Topic;
impl warp::reject::Reject for NonUtf8Topic {}

#[derive(Debug)]
struct RequiredArgumentMissing(&'static [u8]);
impl warp::reject::Reject for RequiredArgumentMissing {}

struct QueryAsRawPartsParser<'a> { input: &'a [u8] }

// This has been monkey'd from https://github.com/servo/rust-url/blob/cce2d32015419b38f00c210430ecd3059105a7f2/src/form_urlencoded.rs
impl<'a> Iterator for QueryAsRawPartsParser<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.input.is_empty() {
                return None
            }

            let mut split2 = self.input.splitn(2, |&b| b == b'&');

            let sequence = split2.next().expect("splitn will always return first");
            self.input = split2.next().unwrap_or(&[][..]);

            if sequence.is_empty() {
                continue;
            }

            let mut split2 = sequence.splitn(2, |&b| b == b'=');
            let name = split2.next().unwrap();
            let value = split2.next().unwrap_or(&[][..]);
            // original implementation calls percent_decode for both arguments into Cow<str>
            return Some((name, value));
        }
    }
}
