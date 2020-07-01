use thiserror::Error;

#[derive(Debug, Error)]
pub enum BitswapError {
    #[error("Error while reading from socket: {0}")]
    ReadError(#[from] libp2p_core::upgrade::ReadOneError),
    #[error("Error while decoding bitswap message: {0}")]
    ProtobufError(#[from] prost::DecodeError),
    #[error("Error while parsing cid: {0}")]
    Cid(#[from] cid::Error),
}
