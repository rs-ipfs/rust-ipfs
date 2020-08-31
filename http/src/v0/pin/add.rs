use crate::v0::support::option_parsing::ParseError;
use crate::v0::support::{StringError, StringSerialized};
use futures::future::try_join_all;
use ipfs::{Cid, Ipfs, IpfsTypes};
use serde::Serialize;
use std::convert::TryFrom;
use warp::{reply, Filter, Rejection, Reply};

#[derive(Debug)]
pub struct AddRequest {
    args: Vec<Cid>,
    recursive: bool,
    progress: bool,
    // TODO: timeout, probably with rollback semantics?
}

#[derive(Serialize)]
struct AddResponse {
    #[serde(rename = "Pins")]
    pins: Vec<StringSerialized<Cid>>,
    // FIXME: go-ipfs doesn't respond with this
    //progress: u8,
}

pub async fn add_inner<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    request: AddRequest,
) -> Result<impl Reply, Rejection> {
    if request.progress {
        // FIXME: there doesn't appear to be a test for this
        return Err(crate::v0::support::NotImplemented.into());
    }

    let cids: Vec<Cid> = request.args;

    let recursive = request.recursive;

    let dispatched_pins = cids.into_iter().map(|x| async {
        ipfs.insert_pin(&x, recursive)
            .await
            .map(move |_| StringSerialized(x))
    });

    // could be unordered :)
    let completed = try_join_all(dispatched_pins)
        .await
        .map_err(StringError::from)?;

    Ok(reply::json(&AddResponse {
        pins: completed,
        //progress: 100,
    }))
}

impl<'a> TryFrom<&'a str> for AddRequest {
    type Error = ParseError<'a>;

    fn try_from(q: &'a str) -> Result<Self, Self::Error> {
        use ParseError::*;

        let mut args = Vec::new();
        let mut recursive = None;
        let mut progress = None;

        for (key, value) in url::form_urlencoded::parse(q.as_bytes()) {
            let target = match &*key {
                "arg" => {
                    args.push(Cid::try_from(&*value).map_err(|e| InvalidCid("arg".into(), e))?);
                    continue;
                }
                "recursive" => &mut recursive,
                "progress" => &mut progress,
                _ => {
                    // ignore unknown
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

        if args.is_empty() {
            return Err(MissingArg);
        }

        Ok(AddRequest {
            args,
            recursive: recursive.unwrap_or(false),
            progress: progress.unwrap_or(false),
        })
    }
}

/// Filter to perform custom `warp::query::<AddRequest>`. This needs to be copypasted around as
/// HRTB is not quite usable yet.
pub fn add_request() -> impl Filter<Extract = (AddRequest,), Error = Rejection> + Clone {
    warp::filters::query::raw().and_then(|q: String| {
        let res = AddRequest::try_from(q.as_str())
            .map_err(StringError::from)
            .map_err(warp::reject::custom);

        futures::future::ready(res)
    })
}
