use crate::v0::support::option_parsing::ParseError;
use std::convert::TryFrom;

#[derive(Debug)]
pub struct RefsOptions {
    /// This can start with /ipfs/ but doesn't have to, can continue with paths, if a link cannot
    /// be found it's an json error from go-ipfs
    pub arg: Vec<String>,
    /// This can be used to format the output string into the `{ "Ref": "here" .. }`
    pub format: Option<String>,
    /// This cannot be used with `format`, prepends "source -> " to the `Ref` response
    pub edges: bool,
    /// Not sure if this is tested by conformance testing but I'd assume this destinatinos on their
    /// first linking.
    pub unique: bool,
    pub recursive: bool,
    // `int` in the docs apparently is platform specific
    // go-ipfs only honors this when `recursive` is true.
    // go-ipfs treats -2 as -1 when `recursive` is true.
    // go-ipfs doesn't use the json return value if this value is too large or non-int
    pub max_depth: Option<i64>,
    /// Pretty much any other duration, but since we are not using serde, we can just have it
    /// directly.
    pub timeout: Option<humantime::Duration>,
}

impl RefsOptions {
    pub fn max_depth(&self) -> Option<u64> {
        if self.recursive {
            match self.max_depth {
                // zero means do nothing
                Some(x) if x >= 0 => Some(x as u64),
                _ => None,
            }
        } else {
            // only immediate links after the path
            Some(1)
        }
    }
}

impl<'a> TryFrom<&'a str> for RefsOptions {
    type Error = ParseError<'a>;

    fn try_from(q: &'a str) -> Result<Self, Self::Error> {
        use ParseError::*;

        // TODO: check how go-ipfs handles duplicate parameters for non-Vec fields
        //
        // this manual deserialization is required because `serde_urlencoded` (used by
        // warp::query) does not support multiple instances of the same field, nor does
        // `serde_qs` (it would support arg[]=...). supporting this in `serde_urlencoded` is
        // out of scope; not sure of `serde_qs`.
        let parse = url::form_urlencoded::parse(q.as_bytes());

        let mut args = Vec::new();
        let mut format = None;
        let mut timeout: Option<humantime::Duration> = None;
        let mut edges = None;
        let mut unique = None;
        let mut recursive = None;
        let mut max_depth = None;

        for (key, value) in parse {
            let target = match &*key {
                "arg" => {
                    args.push(value.into_owned());
                    continue;
                }
                "format" => {
                    if format.is_none() {
                        // not parsing this the whole way as there might be hope to have this
                        // function removed in the future.
                        format = Some(value.into_owned());
                        continue;
                    } else {
                        return Err(DuplicateField(key));
                    }
                }
                "timeout" => {
                    if timeout.is_none() {
                        timeout = Some(
                            value
                                .parse()
                                .map_err(|e| ParseError::InvalidDuration("timeout".into(), e))?,
                        );
                        continue;
                    } else {
                        return Err(DuplicateField(key));
                    }
                }
                "max-depth" => {
                    if max_depth.is_none() {
                        max_depth = match value.parse::<i64>() {
                            Ok(max_depth) => Some(max_depth),
                            Err(_) => return Err(InvalidNumber(key, value)),
                        };
                        continue;
                    } else {
                        return Err(DuplicateField(key));
                    }
                }
                "edges" => &mut edges,
                "unique" => &mut unique,
                "recursive" => &mut recursive,
                _ => {
                    // ignore unknown fields
                    continue;
                }
            };

            // common bool field handling
            if target.is_none() {
                match value.parse::<bool>() {
                    Ok(value) => *target = Some(value),
                    Err(_) => return Err(InvalidBoolean(key, value)),
                }
            } else {
                return Err(DuplicateField(key));
            }
        }

        if args.is_empty() {
            return Err(MissingArg);
        }

        Ok(RefsOptions {
            arg: args,
            format,
            edges: edges.unwrap_or(false),
            unique: unique.unwrap_or(false),
            recursive: recursive.unwrap_or(false),
            max_depth,
            timeout,
        })
    }
}
