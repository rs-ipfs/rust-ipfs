use crate::path::SubPath;
use libipld::ipld::Ipld;

#[derive(Debug)]
pub enum IpfsPathError {
    InvalidPath(String),
    ResolveError { ipld: Ipld, path: SubPath },
    ExpectedIpldPath,
}

impl std::error::Error for IpfsPathError {
    fn description(&self) -> &str {
        match *self {
            IpfsPathError::InvalidPath(_) => "invalid path",
            IpfsPathError::ResolveError { .. } => "error resolving path",
            IpfsPathError::ExpectedIpldPath => "expected ipld path",
        }
    }
}

impl std::fmt::Display for IpfsPathError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            IpfsPathError::InvalidPath(ref path) => write!(f, "Invalid path {:?}", path),
            IpfsPathError::ResolveError { ref path, .. } => {
                write!(f, "Can't resolve {}", path.to_string())
            }
            IpfsPathError::ExpectedIpldPath => write!(f, "Expected ipld path but found ipns path"),
        }
    }
}
