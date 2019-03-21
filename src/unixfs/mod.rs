use crate::block::Block;
use crate::error::Error;
use crate::ipld::{Ipld, IpldDag,  formats::pb::PbNode};
use crate::path::IpfsPath;
use crate::repo::{BlockLoader, RepoTypes};
use core::future::Future;
use futures::compat::*;
use std::collections::HashMap;
use std::convert::TryInto;
use std::path::PathBuf;

pub mod unixfs;
use unixfs::{Data_DataType, Data as UnixfsData};

pub struct File {
    data: Vec<u8>,
}

impl File {
    pub fn new(path: PathBuf) -> impl Future<Output=Result<Self, Error>> {
        async move {
            let file = await!(tokio::fs::File::open(path).compat())?;
            let (_, data) = await!(tokio::io::read_to_end(file, Vec::new()).compat())?;
            Ok(File {
                data
            })
        }
    }

    pub fn get_unixfs_v1<T: RepoTypes>(dag: &IpldDag<T>, path: IpfsPath) ->
    impl Future<Output=Result<Self, Error>> {
        let future = dag.get(path);
        async move {
            let ipld = await!(future)?;
            let pb_node: PbNode = match ipld.try_into() {
                Ok(pb_node) => pb_node,
                Err(_) => bail!("invalid dag_pb node"),
            };
            Ok(File {
                data: pb_node.data,
            })
        }
    }

    pub fn put_unixfs_v1<T: RepoTypes>(&self, dag: &IpldDag<T>) ->
    impl Future<Output=Result<IpfsPath, Error>>
    {
        let links: Vec<Ipld> = vec![];
        let mut pb_node = HashMap::<&str, Ipld>::new();
        pb_node.insert("Data", self.data.clone().into());
        pb_node.insert("Links", links.into());
        let ipld = pb_node.into();
        dag.put(ipld, cid::Codec::DagProtobuf)
    }
}

impl From<Vec<u8>> for File {
    fn from(data: Vec<u8>) -> Self {
        File {
            data,
        }
    }
}

impl From<&str> for File {
    fn from(string: &str) -> Self {
        File {
            data: string.as_bytes().to_vec()
        }
    }
}

impl Into<String> for File {
    fn into(self) -> String {
        String::from_utf8_lossy(&self.data).to_string()
    }
}


pub(crate) fn extract_block(block: Block) -> Vec<u8> {
    match Ipld::from(&block) {
        Ok(ref i) => extract_file_data(i),
        _ => None
    }.unwrap_or_else(move || {
        let (_cid, data) = block.into_inner();
        data
    }
}

pub(crate) fn extract_file_data<'d>(ipld: &'d Ipld) -> Option<Vec<u8>> {
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

pub(crate) async fn fetch_file<'d, B: 'd + BlockLoader>(ipld: &'d Ipld, loader: B)
    -> Result<Vec<u8>, Error>
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
                        for path in links
                            .iter()
                            .filter_map(move |x|
                                if let Ipld::Object(l) = x {
                                    match (l.get(&"Name".to_owned()), l.get(&"Hash".to_owned())) {
                                        (Some(Ipld::String(n)),
                                            Some(Ipld::Link(path))) if n == name => Some(path),
                                        _=> None
                                    }
                                } else {
                                    None
                                }
                        ) {
                            let cid = path.cid()
                                .ok_or(Error::from(::std::io::Error::new(::std::io::ErrorKind::NotFound, "")))?;
                            let block = await!(loader.load_block(cid))?;
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

    Err(Error::from(::std::io::Error::new(::std::io::ErrorKind::NotFound, "")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::Cid;
    use crate::repo::tests::create_mock_repo;
    use crate::future::tokio_run;
    
    #[test]
    fn test_file_cid() {
        let repo = create_mock_repo();
        let dag = IpldDag::new(repo);
        let file = File::from("\u{8}\u{2}\u{12}\u{12}Here is some data\n\u{18}\u{12}");
        let cid = Cid::from("QmSy5pnHk1EnvE5dmJSyFKG5unXLGjPpBuJJCBQkBTvBaW").unwrap();

        tokio_run(async move {
            let path = await!(file.put_unixfs_v1(&dag)).unwrap();
            assert_eq!(cid.to_string(), path.root().cid().unwrap().to_string());
        });
    }
}
