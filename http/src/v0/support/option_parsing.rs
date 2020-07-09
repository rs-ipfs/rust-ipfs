use std::borrow::Cow;
use std::fmt;

#[derive(Debug)]
pub enum ParseError<'a> {
    DuplicateField(Cow<'a, str>),
    MissingArg,
    InvalidNumber(Cow<'a, str>, Cow<'a, str>),
    InvalidBoolean(Cow<'a, str>, Cow<'a, str>),
    InvalidDuration(Cow<'a, str>, humantime::DurationError),
}

impl<'a> fmt::Display for ParseError<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use ParseError::*;
        match *self {
            DuplicateField(ref s) => write!(fmt, "field {:?} was duplicated", *s),
            MissingArg => write!(fmt, "required field \"arg\" missing"),
            InvalidNumber(ref k, ref v) => write!(fmt, "field {:?} invalid number: {:?}", *k, *v),
            InvalidBoolean(ref k, ref v) => write!(fmt, "field {:?} invalid boolean: {:?}", *k, *v),
            InvalidDuration(ref field, ref e) => {
                write!(fmt, "field {:?} invalid duration: {:?}", field, e)
            }
        }
    }
}

impl<'a> std::error::Error for ParseError<'a> {}
