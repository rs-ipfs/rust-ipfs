use crate::v0::support::{
    with_ipfs, MaybeTimeoutExt, StreamResponse, StringError, StringSerialized,
};
use async_stream::try_stream;
use bytes::Bytes;
use futures::stream::TryStream;
use ipfs::unixfs::ll::walk::{self, ContinuedWalk, Walker};
use ipfs::unixfs::{ll::file::FileReadFailed, TraversalFailed};
use ipfs::{dag::ResolveError, Block, Ipfs, IpfsPath, IpfsTypes};
use serde::Deserialize;
use std::fmt;
use std::path::Path;
use warp::{query, Filter, Rejection, Reply};

mod tar_helper;
use tar_helper::TarHelper;

mod add;

#[derive(Debug, Deserialize)]
pub struct AddArgs {
    // unknown meaning; ignoring it doesn't fail any tests
    #[serde(default, rename = "stream-channels")]
    stream_channels: bool,
    // progress reports totaling to the input file size
    #[serde(default)]
    progress: bool,
    /// When true, a new directory is created to hold more than 1 root level directories.
    #[serde(default, rename = "wrap-with-directory")]
    wrap_with_directory: bool,
}

pub fn add<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs)
        .and(query::<AddArgs>())
        .and(warp::header::<mime::Mime>("content-type")) // TODO: rejects if missing
        .and(warp::body::stream())
        .and_then(add::add_inner)
}

#[derive(Debug, Deserialize)]
pub struct CatArgs {
    arg: StringSerialized<IpfsPath>,
    offset: Option<u64>,
    length: Option<u64>,
    timeout: Option<StringSerialized<humantime::Duration>>,
}

pub fn cat<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs).and(query::<CatArgs>()).and_then(cat_inner)
}

async fn cat_inner<T: IpfsTypes>(ipfs: Ipfs<T>, args: CatArgs) -> Result<impl Reply, Rejection> {
    let path = args.arg.into_inner();

    let range = match (args.offset, args.length) {
        (Some(start), Some(len)) => Some(start..(start + len)),
        (Some(_start), None) => return Err(crate::v0::support::NotImplemented.into()),
        (None, Some(len)) => Some(0..len),
        (None, None) => None,
    };

    // TODO: timeout for the whole stream!
    let ret = ipfs::unixfs::cat(ipfs, path, range)
        .maybe_timeout(args.timeout.map(StringSerialized::into_inner))
        .await
        .map_err(StringError::from)?;

    let stream = match ret {
        Ok(stream) => stream,
        Err(TraversalFailed::Resolving(err @ ResolveError::NotFound(..))) => {
            // this is checked in the tests
            return Err(StringError::from(err).into());
        }
        Err(TraversalFailed::Walking(_, FileReadFailed::UnexpectedType(ut)))
            if ut.is_directory() =>
        {
            return Err(StringError::from("this dag node is a directory").into())
        }
        Err(e) => return Err(StringError::from(e).into()),
    };

    Ok(StreamResponse(stream))
}

#[derive(Deserialize)]
struct GetArgs {
    arg: StringSerialized<IpfsPath>,
    timeout: Option<StringSerialized<humantime::Duration>>,
}

pub fn get<T: IpfsTypes>(
    ipfs: &Ipfs<T>,
) -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    with_ipfs(ipfs).and(query::<GetArgs>()).and_then(get_inner)
}

async fn get_inner<T: IpfsTypes>(ipfs: Ipfs<T>, args: GetArgs) -> Result<impl Reply, Rejection> {
    use futures::stream::TryStreamExt;

    let path = args.arg.into_inner();

    // FIXME: this timeout is only for the first step, should be for the whole walk!
    let block = resolve_dagpb(&ipfs, path)
        .maybe_timeout(args.timeout.map(StringSerialized::into_inner))
        .await
        .map_err(StringError::from)?
        .map_err(StringError::from)?;

    Ok(StreamResponse(walk(ipfs, block).into_stream()))
}

async fn resolve_dagpb<T: IpfsTypes>(ipfs: &Ipfs<T>, path: IpfsPath) -> Result<Block, StringError> {
    let (resolved, _) = ipfs
        .dag()
        .resolve(path, true)
        .await
        .map_err(StringError::from)?;

    resolved.into_unixfs_block().map_err(StringError::from)
}

fn walk<Types: IpfsTypes>(
    ipfs: Ipfs<Types>,
    Block {
        cid: root,
        data: first_block_data,
    }: Block,
) -> impl TryStream<Ok = Bytes, Error = GetError> + 'static {
    let mut cache = None;
    let mut tar_helper = TarHelper::with_capacity(16 * 1024);

    // the HTTP api uses the final Cid name as the root name in the generated tar
    // archive.
    let name = root.to_string();
    let mut walker = Walker::new(root, name);

    let mut buffer = Some(first_block_data);

    try_stream! {
        while walker.should_continue() {
            let data = match buffer.take() {
                Some(first) => first,
                None => {
                    let (next, _) = walker.pending_links();
                    let Block { data, .. } = ipfs.get_block(next).await?;
                    data
                }
            };

            match walker.next(&data, &mut cache)? {
                ContinuedWalk::Bucket(..) => {}
                ContinuedWalk::File(segment, _, path, metadata, size) => {
                    if segment.is_first() {
                        for bytes in tar_helper.apply_file(path, metadata, size)?.iter_mut() {
                            if let Some(bytes) = bytes.take() {
                                yield bytes;
                            }
                        }
                    }

                    // even if the largest of files can have 256 kB blocks and about the same
                    // amount of content, try to consume it in small parts not to grow the buffers
                    // too much.

                    let mut n = 0usize;
                    let slice = segment.as_ref();
                    let total = slice.len();

                    while n < total {
                        let next = tar_helper.buffer_file_contents(&slice[n..]);
                        n += next.len();
                        yield next;
                    }

                    if segment.is_last() {
                        if let Some(zeroes) = tar_helper.pad(size) {
                            yield zeroes;
                        }
                    }
                },
                ContinuedWalk::Directory(_, path, metadata) | ContinuedWalk::RootDirectory(_, path, metadata) => {
                    for bytes in tar_helper.apply_directory(path, metadata)?.iter_mut() {
                        if let Some(bytes) = bytes.take() {
                            yield bytes;
                        }
                    }
                },
                ContinuedWalk::Symlink(bytes, _, path, metadata) => {
                    // converting a symlink is the most tricky part
                    let target = std::str::from_utf8(bytes).map_err(|_| GetError::NonUtf8Symlink)?;
                    let target = Path::new(target);

                    for bytes in tar_helper.apply_symlink(path, target, metadata)?.iter_mut() {
                        if let Some(bytes) = bytes.take() {
                            yield bytes;
                        }
                    }
                },
            };
        }
    }
}

#[derive(Debug)]
enum GetError {
    NonUtf8Symlink,
    InvalidFileName(Vec<u8>),
    InvalidLinkName(Vec<u8>),
    Walk(walk::Error),
    Loading(ipfs::Error),
}

impl From<ipfs::Error> for GetError {
    fn from(e: ipfs::Error) -> Self {
        GetError::Loading(e)
    }
}

impl From<walk::Error> for GetError {
    fn from(e: walk::Error) -> Self {
        GetError::Walk(e)
    }
}

impl fmt::Display for GetError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use GetError::*;
        match self {
            NonUtf8Symlink => write!(fmt, "symlink target could not be converted to utf-8"),
            Walk(e) => write!(fmt, "{}", e),
            Loading(e) => write!(fmt, "loading failed: {}", e),
            InvalidFileName(x) => write!(fmt, "filename cannot be put inside tar: {:?}", x),
            InvalidLinkName(x) => write!(fmt, "symlink name cannot be put inside tar: {:?}", x),
        }
    }
}

impl std::error::Error for GetError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            GetError::Walk(e) => Some(e),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use cid::Cid;
    use futures::stream::{FuturesOrdered, TryStreamExt};
    use hex_literal::hex;
    use ipfs::{Block, Ipfs, IpfsTypes, Node};
    use multihash::Sha2_256;
    use std::convert::TryFrom;
    use std::path::PathBuf;

    // Entry we'll use in expectations
    #[derive(Debug, PartialEq, Eq)]
    enum Entry {
        Dir(PathBuf),
        File(PathBuf, u64, Vec<u8>),
        Symlink(PathBuf, PathBuf),
    }

    impl<'a, R: std::io::Read> TryFrom<tar::Entry<'a, R>> for Entry {
        type Error = std::io::Error;

        fn try_from(mut entry: tar::Entry<'a, R>) -> Result<Self, Self::Error> {
            let header = entry.header();

            let entry = match header.entry_type() {
                tar::EntryType::Directory => Entry::Dir(entry.path()?.into()),
                tar::EntryType::Regular => {
                    let path = entry.path()?.into();
                    let size = header.size()?;

                    // writing to file is the only supported way to get the contents
                    let tempdir = tempfile::tempdir()?;
                    let temp_file = tempdir.path().join("temporary_file_for_testing.txt");
                    entry.unpack(&temp_file)?;

                    let bytes = std::fs::read(&temp_file);

                    // regardless of read success let's prefer deleting the file
                    std::fs::remove_file(&temp_file)?;

                    // and only later check if the read succeeded
                    Entry::File(path, size, bytes?)
                }
                tar::EntryType::Symlink => Entry::Symlink(
                    entry.path()?.into(),
                    entry.link_name()?.as_deref().unwrap().into(),
                ),
                x => unreachable!("{:?}", x),
            };

            Ok(entry)
        }
    }

    #[tokio::test]
    async fn very_long_file_and_symlink_names() {
        let ipfs = Node::new("test_node").await;

        let blocks: &[&[u8]] = &[
            // the root, QmdKuCuXDuVTsnGpzPgZEuJmiCEn6LZhGHHHwWPQH28DeD
            &hex!("122f0a22122043217b633fbc72e90938d6dc43a34fcf8fe64e5c0d4f5d4b92a691e5a010c40912063337736f6d6518d90912aa020a2212208b346f875acc01ee2d277d2eadc00d42420a7a441a84247d5eb00e76e43e1fb8128102394634326133353338313761366265386231646561303130666138353663326664646236653131366338346561376132393766346637353163613462336461333637646435633638356566313931376232366334333961663765333032626538396163326432663936313132663363386631303262663766373531613836323738316630396536306536313831646333353565343466373664356162366662346463363265346438373532336538356663613765323965386463386333646438646336376133366239623761336134373031366138323761646336353839386635663930666161623632313861643139396234383161363031623433393261355f18280a1d0805121480000000000000000000000000800000000000002822308002"),
            &hex!("12310a221220e2f1caec3161a8950e02ebcdfb3a9879a132041c48fc76fd003b7bcde1f68f08120845436e657374656418fd080a270805121e1000000000000000000000000000000000000000000000000000000000002822308002"),
            &hex!("122e0a221220c710935f96b0b2fb670af9792570c560152ce4a60e7e6995605ebaccb84427371205324164697218bc080a0f080512060400000000002822308002"),
            &hex!("12340a22122098e02ca8786af6b4895b66cfe5af6755b0ab0374c9c44b6ae62a6441cd399254120b324668696572617263687918f5070a0f080512068000000000002822308002"),
            &hex!("12310a2212203a25dbf4a767b451a4c2601807f623f5e86e3758ba26a465c0a8675885537c55120833466c6f6e67657218af070a110805120880000000000000002822308002"),
            &hex!("122e0a221220c16baf1b090a996a990e49136a205aad4e86da6279b20a7fabaf8b087f209f4812054634616e6418d5060a280805121f100000000000000000000000000000000000000000000000000000000000002822308002"),
            &hex!("12310a2212206c28a669b54bb96b80a821162d1050e6c883f72020214c3a2b80afec9a27861e120833466c6f6e676572188f060a110805120880000000000000002822308002"),
            &hex!("122e0a221220f4819507773b17e79ad51a0a56a1e2bfb58b880d8b81fc440fd932c29a6fce8e12054634616e6418b5050a280805121f100000000000000000000000000000000000000000000000000000000000002822308002"),
            &hex!("12310a22122049ead3b060f85b80149d3bee5210b705618e13c40e4884f5a3153bf2f0aee535120833466c6f6e67657218ef040a110805120880000000000000002822308002"),
            &hex!("12ab020a22122068080292b22f2ed1958079a9bcfdfa9affac9a092c141bda94d496af4b1712f8128102394634326133353338313761366265386231646561303130666138353663326664646236653131366338346561376132393766346637353163613462336461333637646435633638356566313931376232366334333961663765333032626538396163326432663936313132663363386631303262663766373531613836323738316630396536306536313831646333353565343466373664356162366662346463363265346438373532336538356663613765323965386463386333646438646336376133366239623761336134373031366138323761646336353839386635663930666161623632313861643139396234383161363031623433393261355f18a2020a1d0805121480000000000000000000000000000000000000002822308002"),
            &hex!("0a9f020804129a022e2e2f2e2e2f2e2e2f2e2e2f2e2e2f2e2e2f2e2e2f2e2e2f2e2e2f34326133353338313761366265386231646561303130666138353663326664646236653131366338346561376132393766346637353163613462336461333637646435633638356566313931376232366334333961663765333032626538396163326432663936313132663363386631303262663766373531613836323738316630396536306536313831646333353565343466373664356162366662346463363265346438373532336538356663613765323965386463386333646438646336376133366239623761336134373031366138323761646336353839386635663930666161623632313861643139396234383161363031623433393261355f"),
            &hex!("0a260802122077656c6c2068656c6c6f207468657265206c6f6e672066696c656e616d65730a1820"),
        ];

        drop(put_all_blocks(&ipfs, &blocks).await.unwrap());

        let filter = super::get(&ipfs);

        let response = warp::test::request()
            .method("POST")
            .path("/get?arg=QmdKuCuXDuVTsnGpzPgZEuJmiCEn6LZhGHHHwWPQH28DeD")
            .reply(&filter)
            .await;

        assert_eq!(response.status(), 200);

        let found = get_archive_entries(response.body());

        let long_filename = "42a353817a6be8b1dea010fa856c2fddb6e116c84ea7a297f4f751ca4b3da367dd5c685ef1917b26c439af7e302be89ac2d2f96112f3c8f102bf7f751a862781f09e60e6181dc355e44f76d5ab6fb4dc62e4d87523e85fca7e29e8dc8c3dd8dc67a36b9b7a3a47016a827adc65898f5f90faab6218ad199b481a601b4392a5_";

        let expected = vec![
            Entry::Dir("QmdKuCuXDuVTsnGpzPgZEuJmiCEn6LZhGHHHwWPQH28DeD".into()),
            Entry::Dir("QmdKuCuXDuVTsnGpzPgZEuJmiCEn6LZhGHHHwWPQH28DeD/some".into()),
            Entry::Dir("QmdKuCuXDuVTsnGpzPgZEuJmiCEn6LZhGHHHwWPQH28DeD/some/nested".into()),
            Entry::Dir("QmdKuCuXDuVTsnGpzPgZEuJmiCEn6LZhGHHHwWPQH28DeD/some/nested/dir".into()),
            Entry::Dir("QmdKuCuXDuVTsnGpzPgZEuJmiCEn6LZhGHHHwWPQH28DeD/some/nested/dir/hierarchy".into()),
            Entry::Dir("QmdKuCuXDuVTsnGpzPgZEuJmiCEn6LZhGHHHwWPQH28DeD/some/nested/dir/hierarchy/longer".into()),
            Entry::Dir("QmdKuCuXDuVTsnGpzPgZEuJmiCEn6LZhGHHHwWPQH28DeD/some/nested/dir/hierarchy/longer/and".into()),
            Entry::Dir("QmdKuCuXDuVTsnGpzPgZEuJmiCEn6LZhGHHHwWPQH28DeD/some/nested/dir/hierarchy/longer/and/longer".into()),
            Entry::Dir("QmdKuCuXDuVTsnGpzPgZEuJmiCEn6LZhGHHHwWPQH28DeD/some/nested/dir/hierarchy/longer/and/longer/and".into()),
            Entry::Dir("QmdKuCuXDuVTsnGpzPgZEuJmiCEn6LZhGHHHwWPQH28DeD/some/nested/dir/hierarchy/longer/and/longer/and/longer".into()),
            Entry::Symlink(
                format!("QmdKuCuXDuVTsnGpzPgZEuJmiCEn6LZhGHHHwWPQH28DeD/some/nested/dir/hierarchy/longer/and/longer/and/longer/{}", long_filename).into(),
                format!("../../../../../../../../../{}", long_filename).into()),
            Entry::File(format!("QmdKuCuXDuVTsnGpzPgZEuJmiCEn6LZhGHHHwWPQH28DeD/{}", long_filename).into(), 32, b"well hello there long filenames\n".to_vec()),
        ];

        assert_eq!(found, expected);
    }

    #[tokio::test]
    async fn get_multiblock_file() {
        let ipfs = Node::new("test_node").await;

        let blocks: &[&[u8]] = &[
            // the root, QmRJHYTNvC3hmd9gJQARxLR1QMEincccBV53bBw524yyq6
            &hex!("12280a221220fef9fe1804942b35e19e145a03f9c9d5ca9c997dda0a9416f3f515a52f1b3ce11200180a12280a221220dfb94b75acb208fd4873d84872af58bd65c731770a7d4c0deeb4088e87390bfe1200180a12280a221220054497ae4e89812c83276a48e3e679013a788b7c0eb02712df15095c02d6cd2c1200180a12280a221220cc332ceb37dea7d3d7c00d1393117638d3ed963575836c6d44a24951e444cf5d120018090a0c080218072002200220022001"),
            // first bytes: fo
            &hex!("0a0808021202666f1802"),
            // ob
            &hex!("0a08080212026f621802"),
            // ar
            &hex!("0a080802120261721802"),
            // \n
            &hex!("0a07080212010a1801"),
        ];

        drop(put_all_blocks(&ipfs, &blocks).await.unwrap());

        let filter = super::get(&ipfs);

        let response = warp::test::request()
            .method("POST")
            .path("/get?arg=QmRJHYTNvC3hmd9gJQARxLR1QMEincccBV53bBw524yyq6")
            .reply(&filter)
            .await;

        assert_eq!(response.status(), 200);

        let found = get_archive_entries(response.body());

        let expected = vec![Entry::File(
            "QmRJHYTNvC3hmd9gJQARxLR1QMEincccBV53bBw524yyq6".into(),
            7,
            b"foobar\n".to_vec(),
        )];

        assert_eq!(found, expected);
    }

    fn get_archive_entries(bytes: impl AsRef<[u8]>) -> Vec<Entry> {
        let mut cursor = std::io::Cursor::new(bytes.as_ref());

        let mut archive = tar::Archive::new(&mut cursor);
        archive
            .entries()
            .and_then(|entries| {
                entries
                    .map(|res| res.and_then(Entry::try_from))
                    .collect::<Result<Vec<Entry>, _>>()
            })
            .unwrap()
    }

    fn put_all_blocks<'a, T: IpfsTypes>(
        ipfs: &'a Ipfs<T>,
        blocks: &'a [&'a [u8]],
    ) -> impl std::future::Future<Output = Result<Vec<Cid>, ipfs::Error>> + 'a {
        let mut inorder = FuturesOrdered::new();
        for block in blocks {
            inorder.push(put_block(&ipfs, block));
        }

        inorder.try_collect::<Vec<_>>()
    }

    fn put_block<'a, T: IpfsTypes>(
        ipfs: &'a Ipfs<T>,
        block: &'a [u8],
    ) -> impl std::future::Future<Output = Result<Cid, ipfs::Error>> + 'a {
        let cid = Cid::new_v0(Sha2_256::digest(block)).unwrap();

        let block = Block {
            cid,
            data: block.into(),
        };

        ipfs.put_block(block)
    }
}
