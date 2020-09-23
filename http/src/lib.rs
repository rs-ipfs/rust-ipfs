//! `ipfs-http` http API implementation.
//!
//! This crate is most useful as a binary used first and foremost for compatibility testing against
//! other ipfs implementations.

#[macro_use]
extern crate tracing;

pub mod v0;

pub mod config;
