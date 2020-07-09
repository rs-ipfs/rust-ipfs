use std::fmt::{self, Display};
use std::str::FromStr;

#[derive(Clone, Copy)]
pub struct StringSerialized<T>(pub T);

impl<T> StringSerialized<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> From<T> for StringSerialized<T> {
    fn from(t: T) -> Self {
        StringSerialized(t)
    }
}

impl<T: fmt::Debug> fmt::Debug for StringSerialized<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(fmt)
    }
}

impl<T: fmt::Display> fmt::Display for StringSerialized<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(fmt)
    }
}

impl<T: Display> serde::Serialize for StringSerialized<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de, T: FromStr> serde::Deserialize<'de> for StringSerialized<T>
where
    <T as FromStr>::Err: Display,
    T: Sized,
{
    fn deserialize<D>(deserializer: D) -> Result<StringSerialized<T>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map(StringSerialized)
            .map_err(serde::de::Error::custom)
    }
}
