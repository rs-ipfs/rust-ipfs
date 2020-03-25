//! /api/v0/pubsub module.
//!
//! /api/v0/pubsub/sub?arg=topic allows multiple clients to subscribe to the single topic, with
//! semantics of getting the messages received on that topic from request onwards. This is
//! implemented with [`tokio::sync::broadcast`] which supports these semantics.
//!
//! # Panics
//!
//! The subscription functionality *assumes* that there are no other users for
//! `ipfs::Ipfs::pubsub_subscribe` and thus will panic if an subscription was made outside of this
//! locking mechanism.

use futures::stream::TryStream;
use futures::stream::TryStreamExt;
use serde::{Deserialize, Serialize};

use tokio::stream::StreamExt;
use tokio::sync::{broadcast, Mutex};
use tokio::time::timeout;

use ipfs::{Ipfs, IpfsTypes};

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use warp::hyper::body::Bytes;
use warp::Filter;

use super::support::{with_ipfs, NonUtf8Topic, RequiredArgumentMissing, StringError};

#[derive(Default)]
pub struct Pubsub {
    subscriptions:
        Mutex<HashMap<String, broadcast::Sender<Result<PreformattedJsonMessage, StreamError>>>>,
}

/// Creates a filter composing pubsub/{peers,ls,pub,sub}.
pub fn routes<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path("pubsub").and(
        peers(ipfs)
            .or(list_subscriptions(ipfs))
            .or(publish(ipfs))
            .or(subscribe(ipfs, Default::default())),
    )
}

/// Handling of https://docs-beta.ipfs.io/reference/http/api/#api-v0-pubsub-peers
pub fn peers<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("peers")
        .and(warp::get().or(warp::post()))
        .unify()
        .and(with_ipfs(ipfs))
        .and(warp::query::<OptionalTopicParameter>().map(|tp: OptionalTopicParameter| tp.topic))
        .and_then(inner_peers)
}

async fn inner_peers<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    topic: Option<String>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let peers = ipfs
        .pubsub_peers(topic.as_deref())
        .await
        .map_err(|e| warp::reject::custom(StringError::from(e)))?;

    Ok(warp::reply::json(&StringListResponse {
        strings: peers.into_iter().map(|id| id.to_string()).collect(),
    }))
}

/// Handling of https://docs-beta.ipfs.io/reference/http/api/#api-v0-pubsub-ls
pub fn list_subscriptions<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("ls")
        .and(warp::get().or(warp::post()))
        .unify()
        .and(with_ipfs(ipfs))
        .and_then(inner_ls)
}

async fn inner_ls<T: IpfsTypes>(ipfs: Ipfs<T>) -> Result<impl warp::Reply, warp::Rejection> {
    let topics = ipfs
        .pubsub_subscribed()
        .await
        .map_err(|e| warp::reject::custom(StringError::from(e)))?;

    Ok(warp::reply::json(&StringListResponse { strings: topics }))
}

/// Handling of https://docs-beta.ipfs.io/reference/http/api/#api-v0-pubsub-pub
pub fn publish<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("pub")
        .and(warp::post())
        .and(with_ipfs(ipfs))
        .and(publish_args(b"arg"))
        .and_then(inner_publish)
}

async fn inner_publish<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    PublishArgs { topic, message }: PublishArgs,
) -> Result<impl warp::Reply, warp::Rejection> {
    // FIXME: perhaps these should be taken by value as they are always moved?
    ipfs.pubsub_publish(&topic, &message.into_inner())
        .await
        .map_err(|e| warp::reject::custom(StringError::from(e)))?;
    Ok(warp::reply::reply())
}

/// Handling of https://docs-beta.ipfs.io/reference/http/api/#api-v0-pubsub-sub
///
/// # Panics
///
/// Note the module documentation.
pub fn subscribe<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
    pubsub: Arc<Pubsub>,
) -> impl warp::Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
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

async fn inner_subscribe<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    pubsub: Arc<Pubsub>,
    topic: String,
) -> impl TryStream<Ok = PreformattedJsonMessage, Error = StreamError> {
    // accessing this through mutex bets on "most accesses would need write access" as in most
    // requests would be asking for new subscriptions, which would require RwLock upgrading
    // from write, which is not supported operation either.
    let mut guard = pubsub.subscriptions.lock().await;

    let rx = match guard.entry(topic) {
        Entry::Occupied(oe) => {
            // the easiest case: just join in, even if there are no other subscribers at the
            // moment
            log::debug!("joining in existing subscription of {:?}", oe.key());
            oe.get().subscribe()
        }
        Entry::Vacant(ve) => {
            let topic = ve.key().clone();

            // the returned stream needs to be set up to be shoveled in a background task
            let shoveled = ipfs
                .pubsub_subscribe(&topic)
                .await
                .expect("new subscriptions shouldn't fail while holding the lock");

            // using broadcast channel should allow us have N concurrent subscribes and
            // preformatted json should give us good enough performance. this channel can last over
            // multiple subscriptions and unsubscriptions
            let (tx, rx) = broadcast::channel::<Result<PreformattedJsonMessage, StreamError>>(4);

            // this will be used to create more subscriptions
            ve.insert(tx.clone());

            let pubsub = Arc::clone(&pubsub);

            // FIXME: handling this all efficiently in single task would require getting a
            // stream of "all streams" from ipfs::p2p::Behaviour ... perhaps one could be added
            // alongside the current "spread per topic" somehow?
            tokio::spawn(shovel(ipfs, pubsub, topic, shoveled, tx));

            rx
        }
    };

    // map recv errors into the StreamError and flatten
    rx.into_stream()
        .map(|res| res.map_err(|_| StreamError::Recv).and_then(|res| res))
}

/// Shovel task takes items from the [`SubscriptionStream`], formats them and passes them on to
/// response streams. Uses timeouts to attempt dropping subscriptions which no longer have
/// responses reading from them and resubscribes streams which get new requests.
async fn shovel<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    pubsub: Arc<Pubsub>,
    topic: String,
    mut shoveled: ipfs::SubscriptionStream,
    tx: broadcast::Sender<Result<PreformattedJsonMessage, StreamError>>,
) {
    log::trace!(
        "started background task for shoveling messages of {:?}",
        topic
    );

    // related conformance test waits for 100ms
    let check_every = Duration::from_millis(50);

    loop {
        // has the underlying stream been stopped by directly calling
        // `Ipfs::pubsub_unsubscribe`
        let mut unsubscribed = true;
        loop {
            let next = match timeout(check_every, shoveled.next()).await {
                Ok(Some(next)) => preformat(next),
                Ok(None) => break,
                Err(_) => {
                    if tx.receiver_count() == 0 {
                        log::debug!("timed out shoveling with zero receivers");
                        break;
                    }

                    // nice thing about this timeout is that it reduces resubscription
                    // traffic, bad thing is that it is still work but then again it's
                    // done once per topic so it's not too much work.
                    continue;
                }
            };

            if tx.send(next.clone()).is_err() {
                // currently no more subscribers
                unsubscribed = false;
                break;
            }
        }

        let mut guard = pubsub.subscriptions.lock().await;

        // as this can take a long time to acquire the mutex, we might get a new
        // subscriber in the between

        if let Entry::Occupied(oe) = guard.entry(topic.clone()) {
            if oe.get().receiver_count() > 0 {
                if unsubscribed {
                    // this is tricky, se should obtain a new shoveled by resubscribing
                    // and reusing the existing broadcast::channel. this will fail if
                    // we introduce other Ipfs::pubsub_subscribe using code which does
                    // not use the `Pubsub` thing.
                    log::debug!(
                        "resubscribing with the existing broadcast channel to {:?}",
                        topic
                    );
                    shoveled = ipfs
                        .pubsub_subscribe(&topic)
                        .await
                        .expect("new subscriptions shouldn't fail while holding the lock");
                } else {
                    log::trace!(
                        "got a new subscriber to existing broadcast channel on {:?}",
                        topic
                    );
                }
                // a new subscriber has appeared since our previous send failure.
                continue;
            }
            // really no more subscribers, unsubscribe and terminate the shoveling
            // task for this stream.
            log::debug!("unsubscribing from {:?}", topic);
            oe.remove();
            return;
        } else {
            unreachable!(
                "only way to remove subscriptions from
                ipfs-http::v0::pubsub::Pubsub is through shoveling tasks exiting"
            );
        }
    }
}

/// The two cases which can stop a pubsub/sub response generation.
// Any error from the stream wrapped in warp::hyper::Body will currently stop the request.
#[derive(Debug, Clone)]
enum StreamError {
    /// Something went bad with the `serde_json`
    Serialization,
    /// Response is not keeping up with the stream (slow client)
    Recv,
}

impl fmt::Display for StreamError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StreamError::Serialization => write!(fmt, "failed to serialize received message"),
            StreamError::Recv => write!(fmt, "consuming the stream too slowly"),
        }
    }
}

impl std::error::Error for StreamError {}

/// Another representation for ipfs::PubsubMessage, but with the Base64Pad encoded fields.
#[derive(Debug, Serialize)]
struct PubsubHttpApiMessage {
    // Base64Pad encoded PeerId
    from: String,
    // Base64Pad encoded Vec<u8>
    data: String,
    // Base64Pad encoded sequence number (go-ipfs sends incrementing, rust-libp2p has random
    // values)
    seqno: String,
    // Plain text topic names
    #[serde(rename = "topicIDs")]
    topics: Vec<String>,
}

impl<'a, T> From<&'a T> for PubsubHttpApiMessage
where
    T: AsRef<ipfs::PubsubMessage>,
{
    fn from(msg: &'a T) -> Self {
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
            topics,
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

// This direction is required by warp::hyper::Body
impl Into<Bytes> for PreformattedJsonMessage {
    fn into(self) -> Bytes {
        self.0
    }
}

/// Formats the given pubsub message into json and a newline, as is the subscription format.
fn preformat(msg: impl AsRef<ipfs::PubsubMessage>) -> Result<PreformattedJsonMessage, StreamError> {
    serde_json::to_vec(&PubsubHttpApiMessage::from(&msg))
        .map(|mut vec| {
            vec.push(b'\n');
            vec
        })
        .map(Bytes::from)
        .map(PreformattedJsonMessage::from)
        .map_err(|e| {
            log::error!("failed to serialize {:?}: {}", msg.as_ref(), e);
            StreamError::Serialization
        })
}

struct StreamResponse<S>(S);

impl<S> warp::Reply for StreamResponse<S>
where
    S: futures::stream::TryStream + Send + Sync + 'static,
    S::Ok: Into<Bytes>,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    fn into_response(self) -> warp::reply::Response {
        use warp::http::header::{HeaderValue, CONTENT_TYPE, TRAILER};
        use warp::hyper::Body;

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

/// The  "arg" for `pubsub/sub`
#[derive(Debug, Deserialize)]
struct TopicParameter {
    #[serde(rename = "arg")]
    topic: String,
}

/// The optional "arg" for `pubsub/peers`
#[derive(Debug, Deserialize)]
struct OptionalTopicParameter {
    #[serde(rename = "arg")]
    topic: Option<String>,
}

/// Generic response which should be moved to ipfs_http::v0
#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct StringListResponse {
    strings: Vec<String>,
}

/// `pubsub/pub` is used by `go-ipfs` by including the topic in the query string and using body for
/// the message. `js-ipfs-http-client` uses query parameters for both. Currently only supports the
/// `js-ipfs-http-client` as `go-ipfs` doesn't send `Content-Length` with the body.
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

/// `parameter_name` is byte slice because there is no percent decoding done for that component.
fn publish_args(
    parameter_name: &'static [u8],
) -> impl warp::Filter<Extract = (PublishArgs,), Error = warp::Rejection> + Copy {
    warp::filters::query::raw()
        .and_then(move |s: String| {
            let ret = if s.is_empty() {
                Err(warp::reject::custom(RequiredArgumentMissing(b"topic")))
            } else {
                // sadly we can't use url::form_urlencoded::parse here as it will do lossy
                // conversion to utf8 without us being able to recover the raw bytes, which are
                // used by js-ipfs/ipfs-http-client to encode raw Buffers:
                // https://github.com/ipfs/js-ipfs/blob/master/packages/ipfs-http-client/src/pubsub/publish.js
                let mut args = QueryAsRawPartsParser {
                    input: s.as_bytes(),
                }
                .filter(|&(k, _)| k == parameter_name)
                .map(|t| t.1);

                let first = args
                    .next()
                    // can't be missing
                    .ok_or_else(|| warp::reject::custom(RequiredArgumentMissing(b"arg")))
                    // decode into Result<String, warp::Rejection>
                    .and_then(|raw_first| {
                        percent_encoding::percent_decode(raw_first)
                            .decode_utf8()
                            .map(|cow| cow.into_owned())
                            .map_err(|_| warp::reject::custom(NonUtf8Topic))
                    });

                first.map(move |first| {
                    // continue to second arg, which may or may not be present
                    let second = args
                        .next()
                        .map(|slice| percent_encoding::percent_decode(slice).collect::<Vec<_>>())
                        .map(QueryOrBody::Query);

                    (first, second)
                })
            };

            futures::future::ready(ret)
        })
        .and_then(|(topic, opt_arg): (String, Option<QueryOrBody>)| {
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

struct QueryAsRawPartsParser<'a> {
    input: &'a [u8],
}

// This has been monkey'd from https://github.com/servo/rust-url/blob/cce2d32015419b38f00c210430ecd3059105a7f2/src/form_urlencoded.rs
impl<'a> Iterator for QueryAsRawPartsParser<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.input.is_empty() {
                return None;
            }

            let mut split2 = self.input.splitn(2, |&b| b == b'&');

            let sequence = split2.next().expect("splitn will always return first");
            self.input = split2.next().unwrap_or(&[][..]);

            if sequence.is_empty() {
                continue;
            }

            let mut split2 = sequence.splitn(2, |&b| b == b'=');
            let name = split2.next().expect("splitn will always return first");
            let value = split2.next().unwrap_or(&[][..]);
            // original implementation calls percent_decode for both arguments into lossy Cow<str>
            return Some((name, value));
        }
    }
}
