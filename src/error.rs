use crate::config::KeyMaterialLoadingFailure;
use futures::channel::{mpsc, oneshot};
use libipld::error::BlockError;
use std::io;
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Cid(#[from] libipld::cid::Error),
    #[error("{0}")]
    Io(Arc<io::Error>),
    #[error("{0}")]
    Canceled(#[from] oneshot::Canceled),
    #[error("{0}")]
    Send(#[from] mpsc::SendError),
    #[error("{0}")]
    Init(#[from] Arc<KeyMaterialLoadingFailure>),
    #[error("{0}")]
    Connect(String),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(Arc::new(err))
    }
}

impl From<KeyMaterialLoadingFailure> for Error {
    fn from(err: KeyMaterialLoadingFailure) -> Self {
        Error::Init(Arc::new(err))
    }
}

impl From<Error> for BlockError {
    fn from(err: Error) -> Self {
        match err {
            Error::Cid(err) => BlockError::Cid(err),
            err => {
                let err = io::Error::new(io::ErrorKind::Other, err);
                BlockError::Io(err)
            }
        }
    }
}
