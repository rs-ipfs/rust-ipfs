use std::fmt::{self, Display};
use std::ops::{Deref, DerefMut};
use std::str::FromStr;

/// A wrapper for anything that implements FromStr to make it serde::Deserialize. Will turn
/// Display to serde::Serialize. Probably should be used with Cid, PeerId and such.
///
/// Monkeyd from: https://github.com/serde-rs/serde/issues/1316
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StringSerialized<T>(pub T);

impl<T> StringSerialized<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> AsRef<T> for StringSerialized<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T> From<T> for StringSerialized<T> {
    fn from(t: T) -> Self {
        StringSerialized(t)
    }
}

impl<T> Deref for StringSerialized<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> DerefMut for StringSerialized<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
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
