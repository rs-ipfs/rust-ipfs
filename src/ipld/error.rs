use cid::Codec;

#[derive(Debug)]
pub enum IpldError {
    UnsupportedCodec(Codec),
}

impl std::error::Error for IpldError {
    fn description(&self) -> &str {
        match *self {
            IpldError::UnsupportedCodec(_) => "unsupported codec",
        }
    }
}

impl std::fmt::Display for IpldError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            IpldError::UnsupportedCodec(ref codec) => {
                write!(f, "Unsupported codec {:?}", codec)
            }
        }
    }
}
