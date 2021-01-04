//! DAG-JSON codec.

use crate::ipld::{BlockError, Ipld};
use cid::Cid;

use core::convert::TryFrom;
use serde::de::Error as SerdeError;
use serde::{de, ser, Deserialize, Serialize};
use serde_json::ser::Serializer;
use serde_json::Error;
use std::collections::BTreeMap;
use std::fmt;

/// Json codec.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DagJsonCodec;

impl DagJsonCodec {
    pub fn encode(ipld: &Ipld) -> Result<Box<[u8]>, BlockError> {
        json_encode(ipld).map_err(|e| BlockError::CodecError(e.into()))
    }

    pub fn decode(data: &[u8]) -> Result<Ipld, BlockError> {
        json_decode(data).map_err(|e| BlockError::CodecError(e.into()))
    }
}

const LINK_KEY: &str = "/";

pub fn json_encode(ipld: &Ipld) -> Result<Box<[u8]>, Error> {
    let mut writer = Vec::with_capacity(128);
    let mut ser = Serializer::new(&mut writer);
    serialize(&ipld, &mut ser)?;
    Ok(writer.into_boxed_slice())
}

pub fn json_decode(data: &[u8]) -> Result<Ipld, Error> {
    let mut de = serde_json::Deserializer::from_slice(&data);
    Ok(deserialize(&mut de)?)
}

fn serialize<S>(ipld: &Ipld, ser: S) -> Result<S::Ok, S::Error>
where
    S: ser::Serializer,
{
    match &ipld {
        Ipld::Null => ser.serialize_none(),
        Ipld::Bool(bool) => ser.serialize_bool(*bool),
        Ipld::Integer(i128) => ser.serialize_i128(*i128),
        Ipld::Float(f64) => ser.serialize_f64(*f64),
        Ipld::String(string) => ser.serialize_str(&string),
        Ipld::Bytes(bytes) => ser.serialize_bytes(&bytes),
        Ipld::List(list) => {
            let wrapped = list.iter().map(|ipld| Wrapper(ipld));
            ser.collect_seq(wrapped)
        }
        Ipld::Map(map) => {
            let wrapped = map.iter().map(|(key, ipld)| (key, Wrapper(ipld)));
            ser.collect_map(wrapped)
        }
        Ipld::Link(link) => {
            let value = base64::encode(&link.to_bytes());
            let mut map = BTreeMap::new();
            map.insert("/", value);

            ser.collect_map(map)
        }
    }
}

fn deserialize<'de, D>(deserializer: D) -> Result<Ipld, D::Error>
where
    D: de::Deserializer<'de>,
{
    deserializer.deserialize_any(JSONVisitor)
}

// Needed for `collect_seq` and `collect_map` in Seserializer
struct Wrapper<'a>(&'a Ipld);
impl<'a> Serialize for Wrapper<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serialize(&self.0, serializer)
    }
}

// serde deserializer visitor that is used by Deseraliazer to decode
// json into IPLD.
struct JSONVisitor;
impl<'de> de::Visitor<'de> for JSONVisitor {
    type Value = Ipld;

    fn expecting(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.write_str("any valid JSON value")
    }

    #[inline]
    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_string(String::from(value))
    }

    #[inline]
    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Ipld::String(value))
    }
    #[inline]
    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_byte_buf(v.to_owned())
    }

    #[inline]
    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Ipld::Bytes(v))
    }

    #[inline]
    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Ipld::Integer(v.into()))
    }

    #[inline]
    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Ipld::Integer(v.into()))
    }

    #[inline]
    fn visit_i128<E>(self, v: i128) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Ipld::Integer(v))
    }

    #[inline]
    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Ipld::Bool(v))
    }

    #[inline]
    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_unit()
    }

    #[inline]
    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Ipld::Null)
    }

    #[inline]
    fn visit_seq<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
    where
        V: de::SeqAccess<'de>,
    {
        let mut vec: Vec<WrapperOwned> = Vec::new();

        while let Some(elem) = visitor.next_element()? {
            vec.push(elem);
        }

        let unwrapped = vec.into_iter().map(|WrapperOwned(ipld)| ipld).collect();
        Ok(Ipld::List(unwrapped))
    }

    #[inline]
    fn visit_map<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
    where
        V: de::MapAccess<'de>,
    {
        let mut values: Vec<(String, WrapperOwned)> = Vec::new();

        while let Some((key, value)) = visitor.next_entry()? {
            values.push((key, value));
        }

        // JSON Object represents IPLD Link if it is `{ "/": "...." }` therefor
        // we valiadet if that is the case here.
        if let Some((key, WrapperOwned(Ipld::String(value)))) = values.first() {
            if key == LINK_KEY && values.len() == 1 {
                let link = base64::decode(value).map_err(SerdeError::custom)?;
                let cid = Cid::try_from(link).map_err(SerdeError::custom)?;
                return Ok(Ipld::Link(cid));
            }
        }

        let unwrapped = values
            .into_iter()
            .map(|(key, WrapperOwned(value))| (key, value));
        Ok(Ipld::Map(unwrapped.collect()))
    }

    #[inline]
    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Ipld::Float(v))
    }
}

// Needed for `visit_seq` and `visit_map` in Deserializer
/// We cannot directly implement `serde::Deserializer` for `Ipld` as it is a remote type.
/// Instead wrap it into a newtype struct and implement `serde::Deserialize` for that one.
/// All the deserializer does is calling the `deserialize()` function we defined which returns
/// an unwrapped `Ipld` instance. Wrap that `Ipld` instance in `Wrapper` and return it.
/// Users of this wrapper will then unwrap it again so that they can return the expected `Ipld`
/// instance.
struct WrapperOwned(Ipld);
impl<'de> Deserialize<'de> for WrapperOwned {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let deserialized = deserialize(deserializer);
        // Better version of Ok(Wrapper(deserialized.unwrap()))
        deserialized.map(Self)
    }
}
