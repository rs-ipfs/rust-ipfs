//! DAG-CBOR codec.

use crate::ipld::{BlockError, Ipld, IpldError};
use byteorder::{BigEndian, ByteOrder};
use cid::Cid;
use std::{
    collections::BTreeMap,
    convert::TryFrom,
    io::{Read, Write},
};
use thiserror::Error;

/// CBOR codec.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct DagCborCodec;

impl DagCborCodec {
    pub fn encode(ipld: &Ipld) -> Result<Box<[u8]>, CborError> {
        let mut bytes = Vec::new();
        ipld.write_cbor(&mut bytes)?;
        Ok(bytes.into_boxed_slice())
    }

    pub fn decode(mut data: &[u8]) -> Result<Ipld, CborError> {
        Ipld::read_cbor(&mut data)
    }
}

/// CBOR error.
#[derive(Debug, Error)]
pub enum CborError {
    /// Number larger than u64.
    #[error("Number larger than u64.")]
    NumberOutOfRange,
    /// Length larger than usize or too small, for example zero length cid field.
    #[error("Length out of range.")]
    LengthOutOfRange,
    /// Unexpected cbor code.
    #[error("Unexpected cbor code.")]
    UnexpectedCode,
    /// Unknown cbor tag.
    #[error("Unkown cbor tag.")]
    UnknownTag,
    /// Unexpected key.
    #[error("Wrong key.")]
    UnexpectedKey,
    /// Unexpected eof.
    #[error("Unexpected end of file.")]
    UnexpectedEof,
    /// Io error.
    #[error("{0}")]
    Io(#[from] std::io::Error),
    /// Utf8 error.
    #[error("{0}")]
    Utf8(#[from] std::str::Utf8Error),
    /// The byte before Cid was not multibase identity prefix.
    #[error("Invalid Cid prefix: {0}")]
    InvalidCidPrefix(u8),
    /// Cid error.
    #[error("{0}")]
    Cid(#[from] cid::Error),
    /// Ipld error.
    #[error("{0}")]
    Ipld(#[from] IpldError),
}

impl From<CborError> for BlockError {
    fn from(err: CborError) -> Self {
        Self::CodecError(err.into())
    }
}

/// CBOR result.
pub type CborResult<T> = Result<T, CborError>;

#[inline]
pub fn write_null<W: Write>(w: &mut W) -> CborResult<()> {
    w.write_all(&[0xf6])?;
    Ok(())
}

#[inline]
pub fn write_u8<W: Write>(w: &mut W, major: u8, value: u8) -> CborResult<()> {
    if value <= 0x17 {
        let buf = [major << 5 | value];
        w.write_all(&buf)?;
    } else {
        let buf = [major << 5 | 24, value];
        w.write_all(&buf)?;
    }
    Ok(())
}

#[inline]
pub fn write_u16<W: Write>(w: &mut W, major: u8, value: u16) -> CborResult<()> {
    if value <= u16::from(u8::max_value()) {
        write_u8(w, major, value as u8)?;
    } else {
        let mut buf = [major << 5 | 25, 0, 0];
        BigEndian::write_u16(&mut buf[1..], value);
        w.write_all(&buf)?;
    }
    Ok(())
}

#[inline]
pub fn write_u32<W: Write>(w: &mut W, major: u8, value: u32) -> CborResult<()> {
    if value <= u32::from(u16::max_value()) {
        write_u16(w, major, value as u16)?;
    } else {
        let mut buf = [major << 5 | 26, 0, 0, 0, 0];
        BigEndian::write_u32(&mut buf[1..], value);
        w.write_all(&buf)?;
    }
    Ok(())
}

#[inline]
pub fn write_u64<W: Write>(w: &mut W, major: u8, value: u64) -> CborResult<()> {
    if value <= u64::from(u32::max_value()) {
        write_u32(w, major, value as u32)?;
    } else {
        let mut buf = [major << 5 | 27, 0, 0, 0, 0, 0, 0, 0, 0];
        BigEndian::write_u64(&mut buf[1..], value);
        w.write_all(&buf)?;
    }
    Ok(())
}

#[inline]
pub fn write_tag<W: Write>(w: &mut W, tag: u64) -> CborResult<()> {
    write_u64(w, 6, tag)
}

pub trait WriteCbor {
    fn write_cbor<W: Write>(&self, w: &mut W) -> CborResult<()>;
}

impl WriteCbor for bool {
    #[inline]
    fn write_cbor<W: Write>(&self, w: &mut W) -> CborResult<()> {
        let buf = if *self { [0xf5] } else { [0xf4] };
        w.write_all(&buf)?;
        Ok(())
    }
}

impl WriteCbor for u8 {
    #[inline]
    fn write_cbor<W: Write>(&self, w: &mut W) -> CborResult<()> {
        write_u8(w, 0, *self)
    }
}

impl WriteCbor for u16 {
    #[inline]
    fn write_cbor<W: Write>(&self, w: &mut W) -> CborResult<()> {
        write_u16(w, 0, *self)
    }
}

impl WriteCbor for u32 {
    #[inline]
    fn write_cbor<W: Write>(&self, w: &mut W) -> CborResult<()> {
        write_u32(w, 0, *self)
    }
}

impl WriteCbor for u64 {
    #[inline]
    fn write_cbor<W: Write>(&self, w: &mut W) -> CborResult<()> {
        write_u64(w, 0, *self)
    }
}

impl WriteCbor for i8 {
    #[inline]
    fn write_cbor<W: Write>(&self, w: &mut W) -> CborResult<()> {
        write_u8(w, 1, -(*self + 1) as u8)
    }
}

impl WriteCbor for i16 {
    #[inline]
    fn write_cbor<W: Write>(&self, w: &mut W) -> CborResult<()> {
        write_u16(w, 1, -(*self + 1) as u16)
    }
}

impl WriteCbor for i32 {
    #[inline]
    fn write_cbor<W: Write>(&self, w: &mut W) -> CborResult<()> {
        write_u32(w, 1, -(*self + 1) as u32)
    }
}

impl WriteCbor for i64 {
    #[inline]
    fn write_cbor<W: Write>(&self, w: &mut W) -> CborResult<()> {
        write_u64(w, 1, -(*self + 1) as u64)
    }
}

impl WriteCbor for f32 {
    #[inline]
    #[allow(clippy::float_cmp)]
    fn write_cbor<W: Write>(&self, w: &mut W) -> CborResult<()> {
        if self.is_infinite() {
            if self.is_sign_positive() {
                w.write_all(&[0xf9, 0x7c, 0x00])?;
            } else {
                w.write_all(&[0xf9, 0xfc, 0x00])?;
            }
        } else if self.is_nan() {
            w.write_all(&[0xf9, 0x7e, 0x00])?;
        } else {
            let mut buf = [0xfa, 0, 0, 0, 0];
            BigEndian::write_f32(&mut buf[1..], *self);
            w.write_all(&buf)?;
        }
        Ok(())
    }
}

impl WriteCbor for f64 {
    #[inline]
    #[allow(clippy::float_cmp)]
    fn write_cbor<W: Write>(&self, w: &mut W) -> CborResult<()> {
        if !self.is_finite() || f64::from(*self as f32) == *self {
            let value = *self as f32;
            value.write_cbor(w)?;
        } else {
            let mut buf = [0xfb, 0, 0, 0, 0, 0, 0, 0, 0];
            BigEndian::write_f64(&mut buf[1..], *self);
            w.write_all(&buf)?;
        }
        Ok(())
    }
}

impl WriteCbor for [u8] {
    #[inline]
    fn write_cbor<W: Write>(&self, w: &mut W) -> CborResult<()> {
        write_u64(w, 2, self.len() as u64)?;
        w.write_all(self)?;
        Ok(())
    }
}

impl WriteCbor for str {
    #[inline]
    fn write_cbor<W: Write>(&self, w: &mut W) -> CborResult<()> {
        write_u64(w, 3, self.len() as u64)?;
        w.write_all(self.as_bytes())?;
        Ok(())
    }
}

impl WriteCbor for String {
    #[inline]
    fn write_cbor<W: Write>(&self, w: &mut W) -> CborResult<()> {
        self.as_str().write_cbor(w)
    }
}

impl WriteCbor for i128 {
    #[inline]
    fn write_cbor<W: Write>(&self, w: &mut W) -> CborResult<()> {
        if *self < 0 {
            if -(*self + 1) > u64::max_value() as i128 {
                return Err(CborError::NumberOutOfRange);
            }
            write_u64(w, 1, -(*self + 1) as u64)?;
        } else {
            if *self > u64::max_value() as i128 {
                return Err(CborError::NumberOutOfRange);
            }
            write_u64(w, 0, *self as u64)?;
        }
        Ok(())
    }
}

impl WriteCbor for Cid {
    #[inline]
    fn write_cbor<W: Write>(&self, w: &mut W) -> CborResult<()> {
        write_tag(w, 42)?;
        // insert zero byte per https://github.com/ipld/specs/blob/master/block-layer/codecs/dag-cbor.md#links
        let bytes = self.to_bytes();
        write_u64(w, 2, (bytes.len() + 1) as u64)?;
        w.write_all(&[0])?;
        w.write_all(&bytes)?;
        Ok(())
    }
}

impl<T: WriteCbor> WriteCbor for Option<T> {
    #[inline]
    fn write_cbor<W: Write>(&self, w: &mut W) -> CborResult<()> {
        if let Some(value) = self {
            value.write_cbor(w)?;
        } else {
            write_null(w)?;
        }
        Ok(())
    }
}

impl<T: WriteCbor> WriteCbor for Vec<T> {
    #[inline]
    fn write_cbor<W: Write>(&self, w: &mut W) -> CborResult<()> {
        write_u64(w, 4, self.len() as u64)?;
        for value in self {
            value.write_cbor(w)?;
        }
        Ok(())
    }
}

impl<T: WriteCbor + 'static> WriteCbor for BTreeMap<String, T> {
    #[inline]
    fn write_cbor<W: Write>(&self, w: &mut W) -> CborResult<()> {
        write_u64(w, 5, self.len() as u64)?;
        for (k, v) in self {
            k.write_cbor(w)?;
            v.write_cbor(w)?;
        }
        Ok(())
    }
}

impl WriteCbor for Ipld {
    fn write_cbor<W: Write>(&self, w: &mut W) -> CborResult<()> {
        match self {
            Ipld::Null => write_null(w),
            Ipld::Bool(b) => b.write_cbor(w),
            Ipld::Integer(i) => i.write_cbor(w),
            Ipld::Float(f) => f.write_cbor(w),
            Ipld::Bytes(b) => b.as_slice().write_cbor(w),
            Ipld::String(s) => s.as_str().write_cbor(w),
            Ipld::List(l) => l.write_cbor(w),
            Ipld::Map(m) => m.write_cbor(w),
            Ipld::Link(c) => c.write_cbor(w),
        }
    }
}

#[inline]
pub fn read_u8<R: Read>(r: &mut R) -> CborResult<u8> {
    let mut buf = [0; 1];
    r.read_exact(&mut buf)?;
    Ok(buf[0])
}

#[inline]
pub fn read_u16<R: Read>(r: &mut R) -> CborResult<u16> {
    let mut buf = [0; 2];
    r.read_exact(&mut buf)?;
    Ok(BigEndian::read_u16(&buf))
}

#[inline]
pub fn read_u32<R: Read>(r: &mut R) -> CborResult<u32> {
    let mut buf = [0; 4];
    r.read_exact(&mut buf)?;
    Ok(BigEndian::read_u32(&buf))
}

#[inline]
pub fn read_u64<R: Read>(r: &mut R) -> CborResult<u64> {
    let mut buf = [0; 8];
    r.read_exact(&mut buf)?;
    Ok(BigEndian::read_u64(&buf))
}

#[inline]
pub fn read_f32<R: Read>(r: &mut R) -> CborResult<f32> {
    let mut buf = [0; 4];
    r.read_exact(&mut buf)?;
    Ok(BigEndian::read_f32(&buf))
}

#[inline]
pub fn read_f64<R: Read>(r: &mut R) -> CborResult<f64> {
    let mut buf = [0; 8];
    r.read_exact(&mut buf)?;
    Ok(BigEndian::read_f64(&buf))
}

#[inline]
pub fn read_bytes<R: Read>(r: &mut R, len: usize) -> CborResult<Vec<u8>> {
    let mut buf = vec![0; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

#[inline]
pub fn read_str<R: Read>(r: &mut R, len: usize) -> CborResult<String> {
    let bytes = read_bytes(r, len)?;
    let string = std::str::from_utf8(&bytes)?;
    Ok(string.to_string())
}

#[inline]
pub fn read_list<R: Read, T: ReadCbor>(r: &mut R, len: usize) -> CborResult<Vec<T>> {
    let mut list: Vec<T> = Vec::with_capacity(len);
    for _ in 0..len {
        list.push(T::read_cbor(r)?);
    }
    Ok(list)
}

#[inline]
pub fn read_map<R: Read, T: ReadCbor>(r: &mut R, len: usize) -> CborResult<BTreeMap<String, T>> {
    let mut map: BTreeMap<String, T> = BTreeMap::new();
    for _ in 0..len {
        let key = String::read_cbor(r)?;
        let value = T::read_cbor(r)?;
        map.insert(key, value);
    }
    Ok(map)
}

#[inline]
pub fn read_link<R: Read>(r: &mut R) -> CborResult<Cid> {
    let tag = read_u8(r)?;
    if tag != 42 {
        return Err(CborError::UnknownTag);
    }
    let ty = read_u8(r)?;
    if ty != 0x58 {
        return Err(CborError::UnknownTag);
    }
    let len = read_u8(r)?;
    if len == 0 {
        return Err(CborError::LengthOutOfRange);
    }
    let bytes = read_bytes(r, len as usize)?;
    if bytes[0] != 0 {
        return Err(CborError::InvalidCidPrefix(bytes[0]));
    }

    // skip the first byte per
    // https://github.com/ipld/specs/blob/master/block-layer/codecs/dag-cbor.md#links
    Ok(Cid::try_from(&bytes[1..])?)
}

pub trait ReadCbor: Sized {
    fn try_read_cbor<R: Read>(r: &mut R, major: u8) -> CborResult<Option<Self>>;

    #[inline]
    fn read_cbor<R: Read>(r: &mut R) -> CborResult<Self> {
        let major = read_u8(r)?;
        if let Some(res) = Self::try_read_cbor(r, major)? {
            Ok(res)
        } else {
            Err(CborError::UnexpectedCode)
        }
    }
}

impl ReadCbor for bool {
    #[inline]
    fn try_read_cbor<R: Read>(_: &mut R, major: u8) -> CborResult<Option<Self>> {
        match major {
            0xf4 => Ok(Some(false)),
            0xf5 => Ok(Some(true)),
            _ => Ok(None),
        }
    }
}

impl ReadCbor for u8 {
    #[inline]
    fn try_read_cbor<R: Read>(r: &mut R, major: u8) -> CborResult<Option<Self>> {
        match major {
            0x00..=0x17 => Ok(Some(major)),
            0x18 => Ok(Some(read_u8(r)?)),
            _ => Ok(None),
        }
    }
}

impl ReadCbor for u16 {
    #[inline]
    fn try_read_cbor<R: Read>(r: &mut R, major: u8) -> CborResult<Option<Self>> {
        match major {
            0x00..=0x17 => Ok(Some(major as u16)),
            0x18 => Ok(Some(read_u8(r)? as u16)),
            0x19 => Ok(Some(read_u16(r)?)),
            _ => Ok(None),
        }
    }
}

impl ReadCbor for u32 {
    #[inline]
    fn try_read_cbor<R: Read>(r: &mut R, major: u8) -> CborResult<Option<Self>> {
        match major {
            0x00..=0x17 => Ok(Some(major as u32)),
            0x18 => Ok(Some(read_u8(r)? as u32)),
            0x19 => Ok(Some(read_u16(r)? as u32)),
            0x1a => Ok(Some(read_u32(r)?)),
            _ => Ok(None),
        }
    }
}

impl ReadCbor for u64 {
    #[inline]
    fn try_read_cbor<R: Read>(r: &mut R, major: u8) -> CborResult<Option<Self>> {
        match major {
            0x00..=0x17 => Ok(Some(major as u64)),
            0x18 => Ok(Some(read_u8(r)? as u64)),
            0x19 => Ok(Some(read_u16(r)? as u64)),
            0x1a => Ok(Some(read_u32(r)? as u64)),
            0x1b => Ok(Some(read_u64(r)?)),
            _ => Ok(None),
        }
    }
}

impl ReadCbor for i8 {
    #[inline]
    fn try_read_cbor<R: Read>(r: &mut R, major: u8) -> CborResult<Option<Self>> {
        match major {
            0x20..=0x37 => Ok(Some(-1 - (major - 0x20) as i8)),
            0x38 => Ok(Some(-1 - read_u8(r)? as i8)),
            _ => Ok(None),
        }
    }
}

impl ReadCbor for i16 {
    #[inline]
    fn try_read_cbor<R: Read>(r: &mut R, major: u8) -> CborResult<Option<Self>> {
        match major {
            0x20..=0x37 => Ok(Some(-1 - (major - 0x20) as i16)),
            0x38 => Ok(Some(-1 - read_u8(r)? as i16)),
            0x39 => Ok(Some(-1 - read_u16(r)? as i16)),
            _ => Ok(None),
        }
    }
}

impl ReadCbor for i32 {
    #[inline]
    fn try_read_cbor<R: Read>(r: &mut R, major: u8) -> CborResult<Option<Self>> {
        match major {
            0x20..=0x37 => Ok(Some(-1 - (major - 0x20) as i32)),
            0x38 => Ok(Some(-1 - read_u8(r)? as i32)),
            0x39 => Ok(Some(-1 - read_u16(r)? as i32)),
            0x3a => Ok(Some(-1 - read_u32(r)? as i32)),
            _ => Ok(None),
        }
    }
}

impl ReadCbor for i64 {
    #[inline]
    fn try_read_cbor<R: Read>(r: &mut R, major: u8) -> CborResult<Option<Self>> {
        match major {
            0x20..=0x37 => Ok(Some(-1 - (major - 0x20) as i64)),
            0x38 => Ok(Some(-1 - read_u8(r)? as i64)),
            0x39 => Ok(Some(-1 - read_u16(r)? as i64)),
            0x3a => Ok(Some(-1 - read_u32(r)? as i64)),
            0x3b => Ok(Some(-1 - read_u64(r)? as i64)),
            _ => Ok(None),
        }
    }
}

impl ReadCbor for f32 {
    #[inline]
    fn try_read_cbor<R: Read>(r: &mut R, major: u8) -> CborResult<Option<Self>> {
        match major {
            0xfa => Ok(Some(read_f32(r)?)),
            _ => Ok(None),
        }
    }
}

impl ReadCbor for f64 {
    #[inline]
    fn try_read_cbor<R: Read>(r: &mut R, major: u8) -> CborResult<Option<Self>> {
        match major {
            0xfa => Ok(Some(read_f32(r)? as f64)),
            0xfb => Ok(Some(read_f64(r)?)),
            _ => Ok(None),
        }
    }
}

impl ReadCbor for String {
    #[inline]
    fn try_read_cbor<R: Read>(r: &mut R, major: u8) -> CborResult<Option<Self>> {
        let len = match major {
            0x60..=0x77 => major as usize - 0x60,
            0x78 => read_u8(r)? as usize,
            0x79 => read_u16(r)? as usize,
            0x7a => read_u32(r)? as usize,
            0x7b => {
                let len = read_u64(r)?;
                if len > usize::max_value() as u64 {
                    return Err(CborError::LengthOutOfRange);
                }
                len as usize
            }
            _ => return Ok(None),
        };
        Ok(Some(read_str(r, len)?))
    }
}

impl ReadCbor for Cid {
    #[inline]
    fn try_read_cbor<R: Read>(r: &mut R, major: u8) -> CborResult<Option<Self>> {
        match major {
            0xd8 => Ok(Some(read_link(r)?)),
            _ => Ok(None),
        }
    }
}

impl ReadCbor for Box<[u8]> {
    #[inline]
    fn try_read_cbor<R: Read>(r: &mut R, major: u8) -> CborResult<Option<Self>> {
        let len = match major {
            0x40..=0x57 => major as usize - 0x40,
            0x58 => read_u8(r)? as usize,
            0x59 => read_u16(r)? as usize,
            0x5a => read_u32(r)? as usize,
            0x5b => {
                let len = read_u64(r)?;
                if len > usize::max_value() as u64 {
                    return Err(CborError::LengthOutOfRange);
                }
                len as usize
            }
            _ => return Ok(None),
        };
        Ok(Some(read_bytes(r, len)?.into_boxed_slice()))
    }
}

impl<T: ReadCbor> ReadCbor for Option<T> {
    #[inline]
    fn try_read_cbor<R: Read>(r: &mut R, major: u8) -> CborResult<Option<Self>> {
        match major {
            0xf6 => Ok(Some(None)),
            0xf7 => Ok(Some(None)),
            _ => {
                if let Some(res) = T::try_read_cbor(r, major)? {
                    Ok(Some(Some(res)))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

impl<T: ReadCbor> ReadCbor for Vec<T> {
    #[inline]
    fn try_read_cbor<R: Read>(r: &mut R, major: u8) -> CborResult<Option<Self>> {
        let len = match major {
            0x80..=0x97 => major as usize - 0x80,
            0x98 => read_u8(r)? as usize,
            0x99 => read_u16(r)? as usize,
            0x9a => read_u32(r)? as usize,
            0x9b => {
                let len = read_u64(r)?;
                if len > usize::max_value() as u64 {
                    return Err(CborError::LengthOutOfRange);
                }
                len as usize
            }
            _ => return Ok(None),
        };
        Ok(Some(read_list(r, len)?))
    }
}

impl<T: ReadCbor> ReadCbor for BTreeMap<String, T> {
    #[inline]
    fn try_read_cbor<R: Read>(r: &mut R, major: u8) -> CborResult<Option<Self>> {
        let len = match major {
            0xa0..=0xb7 => major as usize - 0xa0,
            0xb8 => read_u8(r)? as usize,
            0xb9 => read_u16(r)? as usize,
            0xba => read_u32(r)? as usize,
            0xbb => {
                let len = read_u64(r)?;
                if len > usize::max_value() as u64 {
                    return Err(CborError::LengthOutOfRange);
                }
                len as usize
            }
            _ => return Ok(None),
        };
        Ok(Some(read_map(r, len)?))
    }
}

impl ReadCbor for Ipld {
    #[inline]
    fn try_read_cbor<R: Read>(r: &mut R, major: u8) -> CborResult<Option<Self>> {
        let ipld = match major {
            // Major type 0: an unsigned integer
            0x00..=0x17 => Ipld::Integer(major as i128),
            0x18 => Ipld::Integer(read_u8(r)? as i128),
            0x19 => Ipld::Integer(read_u16(r)? as i128),
            0x1a => Ipld::Integer(read_u32(r)? as i128),
            0x1b => Ipld::Integer(read_u64(r)? as i128),

            // Major type 1: a negative integer
            0x20..=0x37 => Ipld::Integer(-1 - (major - 0x20) as i128),
            0x38 => Ipld::Integer(-1 - read_u8(r)? as i128),
            0x39 => Ipld::Integer(-1 - read_u16(r)? as i128),
            0x3a => Ipld::Integer(-1 - read_u32(r)? as i128),
            0x3b => Ipld::Integer(-1 - read_u64(r)? as i128),

            // Major type 2: a byte string
            0x40..=0x57 => {
                let len = major - 0x40;
                let bytes = read_bytes(r, len as usize)?;
                Ipld::Bytes(bytes)
            }
            0x58 => {
                let len = read_u8(r)?;
                let bytes = read_bytes(r, len as usize)?;
                Ipld::Bytes(bytes)
            }
            0x59 => {
                let len = read_u16(r)?;
                let bytes = read_bytes(r, len as usize)?;
                Ipld::Bytes(bytes)
            }
            0x5a => {
                let len = read_u32(r)?;
                let bytes = read_bytes(r, len as usize)?;
                Ipld::Bytes(bytes)
            }
            0x5b => {
                let len = read_u64(r)?;
                if len > usize::max_value() as u64 {
                    return Err(CborError::LengthOutOfRange);
                }
                let bytes = read_bytes(r, len as usize)?;
                Ipld::Bytes(bytes)
            }

            // Major type 3: a text string
            0x60..=0x77 => {
                let len = major - 0x60;
                let string = read_str(r, len as usize)?;
                Ipld::String(string)
            }
            0x78 => {
                let len = read_u8(r)?;
                let string = read_str(r, len as usize)?;
                Ipld::String(string)
            }
            0x79 => {
                let len = read_u16(r)?;
                let string = read_str(r, len as usize)?;
                Ipld::String(string)
            }
            0x7a => {
                let len = read_u32(r)?;
                let string = read_str(r, len as usize)?;
                Ipld::String(string)
            }
            0x7b => {
                let len = read_u64(r)?;
                if len > usize::max_value() as u64 {
                    return Err(CborError::LengthOutOfRange);
                }
                let string = read_str(r, len as usize)?;
                Ipld::String(string)
            }

            // Major type 4: an array of data items
            0x80..=0x97 => {
                let len = major - 0x80;
                let list = read_list(r, len as usize)?;
                Ipld::List(list)
            }
            0x98 => {
                let len = read_u8(r)?;
                let list = read_list(r, len as usize)?;
                Ipld::List(list)
            }
            0x99 => {
                let len = read_u16(r)?;
                let list = read_list(r, len as usize)?;
                Ipld::List(list)
            }
            0x9a => {
                let len = read_u32(r)?;
                let list = read_list(r, len as usize)?;
                Ipld::List(list)
            }
            0x9b => {
                let len = read_u64(r)?;
                if len > usize::max_value() as u64 {
                    return Err(CborError::LengthOutOfRange);
                }
                let list = read_list(r, len as usize)?;
                Ipld::List(list)
            }

            // Major type 5: a map of pairs of data items
            0xa0..=0xb7 => {
                let len = major - 0xa0;
                let map = read_map(r, len as usize)?;
                Ipld::Map(map)
            }
            0xb8 => {
                let len = read_u8(r)?;
                let map = read_map(r, len as usize)?;
                Ipld::Map(map)
            }
            0xb9 => {
                let len = read_u16(r)?;
                let map = read_map(r, len as usize)?;
                Ipld::Map(map)
            }
            0xba => {
                let len = read_u32(r)?;
                let map = read_map(r, len as usize)?;
                Ipld::Map(map)
            }
            0xbb => {
                let len = read_u64(r)?;
                if len > usize::max_value() as u64 {
                    return Err(CborError::LengthOutOfRange);
                }
                let map = read_map(r, len as usize)?;
                Ipld::Map(map)
            }

            // Major type 6: optional semantic tagging of other major types
            0xd8 => Ipld::Link(read_link(r)?),

            // Major type 7: floating-point numbers and other simple data types that need no content
            0xf4 => Ipld::Bool(false),
            0xf5 => Ipld::Bool(true),
            0xf6 => Ipld::Null,
            0xf7 => Ipld::Null,
            0xfa => Ipld::Float(read_f32(r)? as f64),
            0xfb => Ipld::Float(read_f64(r)?),
            _ => return Ok(None),
        };
        Ok(Some(ipld))
    }
}
