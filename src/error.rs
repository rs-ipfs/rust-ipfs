pub trait IpfsError: std::fmt::Display + std::fmt::Debug + std::error::Error + Send {}

#[derive(Debug)]
pub struct Error(Box<IpfsError>);

impl std::error::Error for Error {
    fn description(&self) -> &str {
        self.0.description()
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl IpfsError for crate::bitswap::BitswapError {}
impl IpfsError for crate::ipld::IpldError {}
impl IpfsError for cbor::CborError {}
impl IpfsError for cid::Error {}
impl IpfsError for libp2p::core::upgrade::ReadOneError {}
impl IpfsError for protobuf::ProtobufError {}
impl IpfsError for rocksdb::Error {}
impl IpfsError for std::io::Error {}

impl<T: IpfsError + 'static> From<T> for Error {
    fn from(err: T) -> Self {
        Error(Box::new(err))
    }
}
