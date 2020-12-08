use cid::Cid;
use std::fmt;

/// `EdgeFormatter` handles different kinds of formats that can be via the `format` query
/// parameter. "Edge" is the unit produced by the stream of walking iplds links.
#[derive(Debug)]
pub enum EdgeFormatter {
    /// The default: only the destination
    Destination,
    /// The default when `edges=true` on the query
    Arrow,
    /// Custom format string
    FormatString(Vec<FormattedPart>),
}

/// Different parts of the format string
#[derive(Debug, PartialEq, Eq)]
pub enum FormattedPart {
    Static(String),
    Source,
    Destination,
    LinkName,
}

/// Failures to create new `EdgeFormatter`
#[derive(Debug)]
pub enum InvalidFormat<'a> {
    /// This needs to be errored on as the combination would make no sense
    EdgesWithCustomFormat,
    /// The format string was invalid
    Format(FormatError<'a>),
}

/// Failures to parse the `str` in `/api/v0/refs?format=str`.
#[derive(Debug)]
pub enum FormatError<'a> {
    UnsupportedTag(&'a str),
    UnterminatedTag(usize),
}

impl EdgeFormatter {
    pub fn from_options(edges: bool, format: Option<&str>) -> Result<Self, InvalidFormat> {
        if edges && format.is_some() {
            return Err(InvalidFormat::EdgesWithCustomFormat);
        }

        if edges {
            Ok(EdgeFormatter::Arrow)
        } else if let Some(formatstr) = format {
            Ok(EdgeFormatter::FormatString(parse_format(formatstr)?))
        } else {
            Ok(EdgeFormatter::Destination)
        }
    }

    /// Produces a `String` for the `Ref` property of returned `Edge` values, according to the
    /// configured formatting. `link_name` is always `None` except for `dag-pb` nodes.
    pub fn format(&self, src: Cid, dst: Cid, link_name: Option<String>) -> String {
        match *self {
            EdgeFormatter::Destination => dst.to_string(),
            EdgeFormatter::Arrow => format!("{} -> {}", src, dst),
            EdgeFormatter::FormatString(ref parts) => {
                let mut out = String::new();
                for part in parts {
                    part.format(&mut out, &src, &dst, link_name.as_deref());
                }
                out.shrink_to_fit();
                out
            }
        }
    }
}

impl FormattedPart {
    fn format(&self, out: &mut String, src: &Cid, dst: &Cid, linkname: Option<&str>) {
        use fmt::Write;
        use FormattedPart::*;
        match *self {
            Static(ref s) => out.push_str(s),
            Source => write!(out, "{}", src).expect("String writing shouldn't fail"),
            Destination => write!(out, "{}", dst).expect("String writing shouldn't fail"),
            LinkName => {
                if let Some(s) = linkname {
                    out.push_str(s)
                }
            }
        }
    }
}

impl<'a> fmt::Display for InvalidFormat<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use InvalidFormat::*;

        match *self {
            EdgesWithCustomFormat => write!(fmt, "using format argument with edges is not allowed"),
            Format(ref e) => write!(fmt, "invalid format string: {}", e),
        }
    }
}

impl<'a> std::error::Error for InvalidFormat<'a> {}

impl<'a> From<FormatError<'a>> for InvalidFormat<'a> {
    fn from(e: FormatError<'a>) -> Self {
        InvalidFormat::Format(e)
    }
}

impl<'a> fmt::Display for FormatError<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use FormatError::*;
        match *self {
            UnsupportedTag(tag) => write!(fmt, "unsupported tag: {:?}", tag),
            UnterminatedTag(index) => write!(fmt, "unterminated tag at index: {}", index),
        }
    }
}

impl<'a> std::error::Error for FormatError<'a> {}

fn parse_format(s: &str) -> Result<Vec<FormattedPart>, FormatError> {
    use std::mem;

    let mut buffer = String::new();
    let mut ret = Vec::new();
    let mut chars = s.char_indices();

    loop {
        match chars.next() {
            Some((index, '<')) => {
                if !buffer.is_empty() {
                    ret.push(FormattedPart::Static(mem::take(&mut buffer)));
                }

                let remaining = chars.as_str();
                let end = remaining
                    .find('>')
                    .ok_or(FormatError::UnterminatedTag(index))?;

                // the use of string indices here is ok as the angle brackets are ascii and
                // cannot be in the middle of multibyte char boundaries
                let inside = &remaining[0..end];

                // TODO: this might need to be case insensitive
                let part = match inside {
                    "src" => FormattedPart::Source,
                    "dst" => FormattedPart::Destination,
                    "linkname" => FormattedPart::LinkName,
                    tag => return Err(FormatError::UnsupportedTag(tag)),
                };

                ret.push(part);

                // the one here is to ignore the '>', which cannot be a codepoint boundary
                chars = remaining[end + 1..].char_indices();
            }
            Some((_, ch)) => buffer.push(ch),
            None => {
                if !buffer.is_empty() {
                    ret.push(FormattedPart::Static(buffer));
                }
                return Ok(ret);
            }
        }
    }
}

#[test]
fn parse_good_formats() {
    use FormattedPart::*;

    let examples = &[
        (
            "<linkname>: <src> -> <dst>",
            vec![
                LinkName,
                Static(": ".into()),
                Source,
                Static(" -> ".into()),
                Destination,
            ],
        ),
        ("-<linkname>", vec![Static("-".into()), LinkName]),
        ("<linkname>-", vec![LinkName, Static("-".into())]),
    ];

    for (input, expected) in examples {
        assert_eq!(&parse_format(input).unwrap(), expected);
    }
}
