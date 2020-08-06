use super::AddArgs;
use crate::v0::support::StringError;
use bytes::{
    buf::{BufExt, BufMutExt},
    Buf, BufMut, Bytes, BytesMut,
};
use cid::Cid;
use futures::stream::{Stream, StreamExt, TryStreamExt};
use ipfs::unixfs::ll::{
    dir::builder::{BufferingTreeBuilder, TreeBuildingFailed, TreeConstructionFailed, TreeNode},
    file::adder::FileAdder,
};
use ipfs::{Block, Ipfs, IpfsTypes};
use mime::Mime;
use mpart_async::server::{MultipartError, MultipartStream};
use serde::Serialize;
use std::borrow::Cow;
use std::fmt;
use warp::{Rejection, Reply};

pub(super) async fn add_inner<T: IpfsTypes>(
    ipfs: Ipfs<T>,
    _opts: AddArgs,
    content_type: Mime,
    body: impl Stream<Item = Result<impl Buf, warp::Error>> + Send + Unpin + 'static,
) -> Result<impl Reply, Rejection> {
    let boundary = content_type
        .get_param("boundary")
        .map(|v| v.to_string())
        .ok_or_else(|| StringError::from("missing 'boundary' on content-type"))?;

    let st = MultipartStream::new(Bytes::from(boundary), body.map_ok(|mut buf| buf.to_bytes()));

    let st = add_stream(ipfs, st);

    // map the errors into json objects at least as we cannot return them as trailers (yet)

    let st = st.map(|res| match res {
        passthrough @ Ok(_) | passthrough @ Err(AddError::ResponseSerialization(_)) => {
            // there is nothing we should do or could do for these; the assumption is that hyper
            // will send the bytes and stop on serialization error and log it. the response
            // *should* be closed on the error.
            passthrough
        }
        Err(something_else) => {
            let msg = crate::v0::support::MessageResponseBuilder::default()
                .with_message(format!("{}", something_else));
            let bytes: Bytes = serde_json::to_vec(&msg)
                .expect("serializing here should not have failed")
                .into();
            let crlf = Bytes::from(&b"\r\n"[..]);
            // note that here we are assuming that the stream ends on error
            Ok(bytes.chain(crlf).to_bytes())
        }
    });

    let body = crate::v0::support::StreamResponse(st);

    Ok(body)
}

#[derive(Debug)]
enum AddError {
    Parsing(MultipartError),
    Header(MultipartError),
    InvalidFilename(std::str::Utf8Error),
    UnsupportedField(String),
    UnsupportedContentType(String),
    ResponseSerialization(serde_json::Error),
    Persisting(ipfs::Error),
    TreeGathering(TreeBuildingFailed),
    TreeBuilding(TreeConstructionFailed),
}

impl From<MultipartError> for AddError {
    fn from(e: MultipartError) -> AddError {
        AddError::Parsing(e)
    }
}

impl fmt::Display for AddError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use AddError::*;
        match self {
            Parsing(me) => write!(fmt, "invalid request body: {}", me),
            Header(me) => write!(fmt, "invalid multipart header(s): {}", me),
            InvalidFilename(e) => write!(fmt, "invalid multipart filename: {:?}", e),
            UnsupportedField(name) => write!(fmt, "unsupported field name: {:?}", name),
            UnsupportedContentType(t) => write!(fmt, "unsupported content-type: {:?} (supported: application/{{octet-stream,x-directory}})", t),
            ResponseSerialization(e) => write!(fmt, "progress serialization failed: {}", e),
            Persisting(e) => write!(fmt, "put_block failed: {}", e),
            TreeGathering(g) => write!(fmt, "invalid directory tree: {}", g),
            TreeBuilding(b) => write!(fmt, "constructed invalid directory tree: {}", b),
        }
    }
}

impl std::error::Error for AddError {}

fn add_stream<St, E>(
    ipfs: Ipfs<impl IpfsTypes>,
    mut fields: MultipartStream<St, E>,
) -> impl Stream<Item = Result<Bytes, AddError>> + Send + 'static
where
    St: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
    E: Into<anyhow::Error> + Send + 'static,
{
    async_stream::try_stream! {
        // TODO: wrap-in-directory option
        let mut tree = BufferingTreeBuilder::default();
        let mut buffer = BytesMut::new();

        while let Some(mut field) = fields
            .try_next()
            .await?
        {

            let field_name = field.name().map_err(AddError::Header)?;
            let filename = field.filename().map_err(AddError::Header)?;
            let filename = percent_encoding::percent_decode_str(filename)
                .decode_utf8()
                .map(|cow| cow.into_owned())
                .map_err(AddError::InvalidFilename)?;

            let content_type = field.content_type().map_err(AddError::Header)?;

            let next = match content_type {
                "application/octet-stream" => {

                    // files are file{,-1,-2,-3,..}
                    let _ = if field_name != "file" && !field_name.starts_with("file-") {
                        Err(AddError::UnsupportedField(field_name.to_string()))
                    } else {
                        Ok(())
                    }?;

                    let mut adder = FileAdder::default();
                    let mut total = 0u64;

                    loop {
                        let next = field
                            .try_next()
                            .await
                            .map_err(AddError::Parsing)?;

                        match next {
                            Some(next) => {
                                let mut read = 0usize;
                                while read < next.len() {
                                    let (iter, used) = adder.push(&next.slice(read..));
                                    read += used;

                                    let maybe_tuple = import_all(&ipfs, iter).await.map_err(AddError::Persisting)?;

                                    total += maybe_tuple.map(|t| t.1).unwrap_or(0);
                                }
                            }
                            None => break,
                        }
                    }

                    let (root, subtotal) = import_all(&ipfs, adder.finish())
                        .await
                        .map_err(AddError::Persisting)?
                        .expect("I think there should always be something from finish -- except if the link block has just been compressed?");

                    total += subtotal;

                    tracing::trace!("completed processing file of {} bytes: {:?}", total, filename);

                    // using the filename as the path since we can tolerate a single empty named file
                    // however the second one will cause issues
                    tree.put_file(&filename, root.clone(), total)
                        .map_err(AddError::TreeGathering)?;

                    let filename: Cow<'_, str> = if filename.is_empty() {
                        // cid needs to be repeated if no filename was given; in which case there
                        // should not be anything to build as tree either.
                        Cow::Owned(root.to_string())
                    } else {
                        Cow::Owned(filename)
                    };

                    serde_json::to_writer((&mut buffer).writer(), &Response::Added {
                        name: filename,
                        hash: Quoted(&root),
                        size: Quoted(total),
                    }).map_err(AddError::ResponseSerialization)?;

                    buffer.put(&b"\r\n"[..]);

                    Ok(buffer.split().freeze())
                },
                "application/x-directory" => {
                    // dirs are dir{,-1,-2,-3,..}
                    let _ = if field_name != "dir" && !field_name.starts_with("dir-") {
                        Err(AddError::UnsupportedField(field_name.to_string()))
                    } else {
                        Ok(())
                    }?;

                    // we need to fully consume this part, even though there shouldn't be anything
                    // except for the already parsed *but* ignored headers
                    while field.try_next().await.map_err(AddError::Parsing)?.is_some() {}

                    // while we don't at the moment parse the mtime, mtime-nsec headers and mode
                    // those should be reflected in the metadata. this will still add an empty
                    // directory which is good thing.
                    tree.set_metadata(&filename, ipfs::unixfs::ll::Metadata::default())
                        .map_err(AddError::TreeGathering)?;
                    continue;
                }
                unsupported => {
                    Err(AddError::UnsupportedContentType(unsupported.to_string()))
                }
            }?;

            yield next;
        }

        let mut full_path = String::new();
        let mut block_buffer = Vec::new();

        let mut iter = tree.build(&mut full_path, &mut block_buffer);

        while let Some(res) = iter.next_borrowed() {
            let TreeNode { path, cid, total_size, block } = res.map_err(AddError::TreeBuilding)?;

            // shame we need to allocate once again here..
            ipfs.put_block(Block { cid: cid.to_owned(), data: block.into() }).await.map_err(AddError::Persisting)?;

            serde_json::to_writer((&mut buffer).writer(), &Response::Added {
                name: Cow::Borrowed(path),
                hash: Quoted(cid),
                size: Quoted(total_size),
            }).map_err(AddError::ResponseSerialization)?;

            buffer.put(&b"\r\n"[..]);

            yield buffer.split().freeze();
        }
    }
}

async fn import_all(
    ipfs: &Ipfs<impl IpfsTypes>,
    iter: impl Iterator<Item = (Cid, Vec<u8>)>,
) -> Result<Option<(Cid, u64)>, ipfs::Error> {
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
        hash: Quoted<&'a Cid>,
        /// Name of the file added from filename or the resulting Cid.
        name: Cow<'a, str>,
        /// Stringified version of the total cumulative size in bytes.
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
        let ipfs = tokio_ipfs().await;

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
            "{\"Hash\":\"Qma4hjFTnCasJ8PVp3mZbZK5g2vGDT4LByLJ7m8ciyRFZP\",\"Name\":\"testfile.txt\",\"Size\":\"20\"}\r\n"
        );
    }

    async fn tokio_ipfs() -> ipfs::Ipfs<ipfs::TestTypes> {
        let options = ipfs::IpfsOptions::inmemory_with_generated_keys();
        let (ipfs, fut) = ipfs::UninitializedIpfs::new(options, None)
            .await
            .start()
            .await
            .unwrap();

        tokio::spawn(fut);
        ipfs
    }
}
