use super::AddArgs;
use crate::v0::support::StringError;
use bytes::{Buf, Bytes};
use cid::Cid;
use futures::stream::{Stream, TryStreamExt};
use ipfs::{Ipfs, IpfsTypes};
use mime::Mime;
use mpart_async::server::MultipartStream;
use serde::Serialize;
use std::borrow::Cow;
use std::fmt;
use warp::{Rejection, Reply};

pub(super) async fn add_inner<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    _opts: AddArgs,
    content_type: Mime,
    body: impl Stream<Item = Result<impl Buf, warp::Error>> + Unpin,
) -> Result<impl Reply, Rejection> {
    // FIXME: this should be without adder at least
    use ipfs::unixfs::ll::file::adder::FileAdder;

    let boundary = content_type
        .get_param("boundary")
        .map(|v| v.to_string())
        .ok_or_else(|| StringError::from("missing 'boundary' on content-type"))?;

    let mut stream =
        MultipartStream::new(Bytes::from(boundary), body.map_ok(|mut buf| buf.to_bytes()));

    // this should be a while loop but clippy will warn if this is a while loop which will only get
    // executed once.
    if let Some(mut field) = stream
        .try_next()
        .await
        .map_err(|e| StringError::from(format!("IO error: {}", e)))?
    {
        let field_name = field
            .name()
            .map_err(|e| StringError::from(format!("unparseable headers: {}", e)))?;

        if field_name != "file" {
            return Err(StringError::from(format!("unsupported field: {}", field_name)).into());
        }

        let filename = field
            .filename()
            .map_err(|e| StringError::from(format!("unparseable filename: {}", e)))?
            .to_string();

        let mut adder = FileAdder::default();
        let mut total = 0u64;

        loop {
            let next = field
                .try_next()
                .await
                .map_err(|e| StringError::from(format!("IO error: {}", e)))?;

            match next {
                Some(next) => {
                    let mut read = 0usize;
                    while read < next.len() {
                        let (iter, used) = adder.push(&next.slice(read..));
                        read += used;

                        let maybe_tuple = import_all(&ipfs, iter).await.map_err(|e| {
                            StringError::from(format!("Failed to save blocks: {}", e))
                        })?;

                        total += maybe_tuple.map(|t| t.1).unwrap_or(0);
                    }
                }
                None => break,
            }
        }

        let (root, subtotal) = import_all(&ipfs, adder.finish())
            .await
            .map_err(|e| StringError::from(format!("Failed to save blocks: {}", e)))?
            .expect("I think there should always be something from finish -- except if the link block has just been compressed?");

        total += subtotal;

        let root = root.to_string();

        let filename: Cow<'_, str> = if filename.is_empty() {
            // cid needs to be repeated if no filename was given
            Cow::Borrowed(&root)
        } else {
            Cow::Owned(filename)
        };

        return Ok(warp::reply::json(&Response::Added {
            name: filename,
            hash: Cow::Borrowed(&root),
            size: Quoted(total),
        }));
    }

    Err(StringError::from("not implemented").into())
}

async fn import_all(
    ipfs: &Ipfs<impl IpfsTypes>,
    iter: impl Iterator<Item = (Cid, Vec<u8>)>,
) -> Result<Option<(Cid, u64)>, ipfs::Error> {
    use ipfs::Block;
    // TODO: use FuturesUnordered
    let mut last: Option<Cid> = None;
    let mut total = 0u64;

    for (cid, data) in iter {
        total += data.len() as u64;
        let block = Block {
            cid,
            data: data.into_boxed_slice(),
        };

        let cid = ipfs.put_block(block).await?;

        last = Some(cid);
    }

    Ok(last.map(|cid| (cid, total)))
}

/// The possible response messages from /add.
#[derive(Debug, Serialize)]
#[serde(untagged)] // rename_all="..." doesn't seem to work at this level
enum Response<'a> {
    /// When progress=true query parameter has been given, this will be output every N bytes, or
    /// perhaps every chunk.
    #[allow(unused)] // unused == not implemented yet
    Progress {
        /// Probably the name of the file being added or empty if none was provided.
        name: Cow<'a, str>,
        /// Bytes processed since last progress; for a file, all progress reports must add up to
        /// the total file size.
        bytes: u64,
    },
    /// Output for every input item.
    #[serde(rename_all = "PascalCase")]
    Added {
        /// The resulting Cid as a string.
        hash: Cow<'a, str>,
        /// Name of the file added from filename or the resulting Cid.
        name: Cow<'a, str>,
        /// Stringified version of the total size in bytes.
        size: Quoted<u64>,
    },
}

#[derive(Debug)]
struct Quoted<D>(pub D);

impl<D: fmt::Display> serde::Serialize for Quoted<D> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use crate::v0::root_files::add;

    #[tokio::test]
    async fn add_single_block_file() {
        let ipfs = testing_ipfs().await;

        // this is from interface-ipfs-core, pretty much simplest add a buffer test case
        // but the body content is from the pubsub test case I copied this from
        let response = warp::test::request()
            .path("/add")
            .header(
                "content-type",
                "multipart/form-data; boundary=-----------------------------Z0oYi6XyTm7_x2L4ty8JL",
            )
            .body(
                &b"-------------------------------Z0oYi6XyTm7_x2L4ty8JL\r\n\
                    Content-Disposition: form-data; name=\"file\"; filename=\"testfile.txt\"\r\n\
                    Content-Type: application/octet-stream\r\n\
                    \r\n\
                    Plz add me!\n\
                    \r\n-------------------------------Z0oYi6XyTm7_x2L4ty8JL--\r\n"[..],
            )
            .reply(&add(&ipfs))
            .await;

        let body = std::str::from_utf8(response.body()).unwrap();

        assert_eq!(
            body,
            r#"{"Hash":"Qma4hjFTnCasJ8PVp3mZbZK5g2vGDT4LByLJ7m8ciyRFZP","Name":"testfile.txt","Size":"20"}"#
        );
    }

    async fn testing_ipfs() -> ipfs::Ipfs<ipfs::TestTypes> {
        let options = ipfs::IpfsOptions::inmemory_with_generated_keys(
            false,
            Some("/ipfs/lan/kad/1.0.0".into()),
        );
        let (ipfs, fut) = ipfs::UninitializedIpfs::new(options)
            .await
            .start()
            .await
            .unwrap();

        tokio::spawn(fut);
        ipfs
    }
}
