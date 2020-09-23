//! Crate-wide errors.
//!
//! The error handling in `ipfs` is subject to change in the future.

/// Just re-export anyhow for now.
///
/// # Stability
///
/// Very likely to change in the future.
pub use anyhow::Error;

/// A try conversion failed.
///
/// # Stability
///
/// Very likely to change in the future.
pub struct TryError;
