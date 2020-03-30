use crate::v0::support::option_parsing::ParseError;
use std::convert::TryFrom;

#[derive(Debug)]
pub struct RmOptions {
    // base58 encoded multihashes of block(s) to remove
    pub args: Vec<String>,
    // Ignore nonexistent blocks
    pub force: bool,
    // Write minimal output
    pub quiet: bool,
}

impl<'a> TryFrom<&'a str> for RmOptions {
    type Error = ParseError<'a>;

    fn try_from(q: &'a str) -> Result<Self, Self::Error> {
        use ParseError::*;

        let parse = url::form_urlencoded::parse(q.as_bytes());

        let mut args = Vec::new();
        let mut force = None;
        let mut quiet = None;

        for (key, value) in parse {
            let target = match &*key {
                "arg" => {
                    args.push(value.into_owned());
                    continue;
                }
                "force" => &mut force,
                "quiet" => &mut quiet,
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

        Ok(RmOptions {
            args,
            force: force.unwrap_or(false),
            quiet: quiet.unwrap_or(false),
        })
    }
}
