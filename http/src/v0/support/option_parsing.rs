use std::borrow::Cow;
use std::fmt;

#[derive(Debug)]
pub enum ParseError<'a> {
    DuplicateField(Cow<'a, str>),
    MissingArg,
    MissingField(Cow<'a, str>),
    InvalidNumber(Cow<'a, str>, Cow<'a, str>),
    InvalidBoolean(Cow<'a, str>, Cow<'a, str>),
    InvalidDuration(Cow<'a, str>, humantime::DurationError),
    InvalidCid(Cow<'a, str>, cid::Error),
    InvalidValue(Cow<'a, str>, Cow<'a, str>),
}

impl<'a> fmt::Display for ParseError<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use ParseError::*;
        match *self {
            DuplicateField(ref s) => write!(fmt, "field {:?} was duplicated", *s),
            MissingArg => write!(fmt, "required field \"arg\" missing"),
            MissingField(ref field) => write!(fmt, "required field {:?} missing", field),
            InvalidNumber(ref k, ref v) => write!(fmt, "field {:?} invalid number: {:?}", *k, *v),
            InvalidBoolean(ref k, ref v) => write!(fmt, "field {:?} invalid boolean: {:?}", *k, *v),
            InvalidDuration(ref field, ref e) => {
                write!(fmt, "field {:?} invalid duration: {}", field, e)
            }
            InvalidCid(ref field, ref e) => write!(fmt, "field {:?} invalid cid: {}", field, e),
            InvalidValue(ref field, ref value) => {
                write!(fmt, "field {:?} invalid value: {:?}", field, value)
            }
        }
    }
}

impl<'a> std::error::Error for ParseError<'a> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        use ParseError::*;
        match self {
            InvalidDuration(_, e) => Some(e),
            InvalidCid(_, e) => Some(e),
            _ => None,
        }
    }
}
