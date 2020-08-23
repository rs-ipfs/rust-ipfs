// Modified automatically generated rust module for 'merkledag.proto' file
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_imports)]
#![allow(unknown_lints)]
#![allow(clippy::all)]
#![cfg_attr(rustfmt, rustfmt_skip)]
use super::*;
use quick_protobuf::sizeofs::*;
use quick_protobuf::{BytesReader, MessageRead, MessageWrite, Result, Writer, WriterBackend};
use std::borrow::Cow;
use core::convert::TryFrom;
use std::io::Write;
use core::ops::Deref;
use core::ops::DerefMut;

#[derive(Debug, Default, PartialEq, Clone)]
pub struct PBLink<'a> {
    pub Hash: Option<Cow<'a, [u8]>>,
    pub Name: Option<Cow<'a, str>>,
    pub Tsize: Option<u64>,
}
impl<'a> MessageRead<'a> for PBLink<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(10) => msg.Hash = Some(r.read_bytes(bytes).map(Cow::Borrowed)?),
                Ok(18) => msg.Name = Some(r.read_string(bytes).map(Cow::Borrowed)?),
                Ok(24) => msg.Tsize = Some(r.read_uint64(bytes)?),
                Ok(t) => {
                    r.read_unknown(bytes, t)?;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}
impl<'a> MessageWrite for PBLink<'a> {
    fn get_size(&self) -> usize {
        0 + self.Hash.as_ref().map_or(0, |m| 1 + sizeof_len((m).len()))
            + self.Name.as_ref().map_or(0, |m| 1 + sizeof_len((m).len()))
            + self
                .Tsize
                .as_ref()
                .map_or(0, |m| 1 + sizeof_varint(*(m) as u64))
    }
    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        if let Some(ref s) = self.Hash {
            w.write_with_tag(10, |w| w.write_bytes(&**s))?;
        }
        if let Some(ref s) = self.Name {
            w.write_with_tag(18, |w| w.write_string(&**s))?;
        }
        if let Some(ref s) = self.Tsize {
            w.write_with_tag(24, |w| w.write_uint64(*s))?;
        }
        Ok(())
    }
}
#[derive(Debug, Default, PartialEq, Clone)]
pub struct PBNode<'a> {
    pub Links: Vec<PBLink<'a>>,
    pub Data: Option<Cow<'a, [u8]>>,
}
impl<'a> MessageRead<'a> for PBNode<'a> {
    fn from_reader(r: &mut BytesReader, bytes: &'a [u8]) -> Result<Self> {
        let mut msg = Self::default();
        while !r.is_eof() {
            match r.next_tag(bytes) {
                Ok(18) => msg.Links.push(r.read_message::<PBLink<'a>>(bytes)?),
                Ok(10) => msg.Data = Some(r.read_bytes(bytes).map(Cow::Borrowed)?),
                Ok(t) => {
                    r.read_unknown(bytes, t)?;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(msg)
    }
}
impl<'a> MessageWrite for PBNode<'a> {
    fn get_size(&self) -> usize {
        0 + self
            .Links
            .iter()
            .map(|s| 1 + sizeof_len((s).get_size()))
            .sum::<usize>()
            + self.Data.as_ref().map_or(0, |m| 1 + sizeof_len((m).len()))
    }
    fn write_message<W: WriterBackend>(&self, w: &mut Writer<W>) -> Result<()> {
        for s in &self.Links {
            w.write_with_tag(18, |w| w.write_message(s))?;
        }
        if let Some(ref s) = self.Data {
            w.write_with_tag(10, |w| w.write_bytes(&**s))?;
        }
        Ok(())
    }
}
