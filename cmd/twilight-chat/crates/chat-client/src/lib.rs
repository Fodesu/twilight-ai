//! Headless chat transport and optional, explicitly owned Host bootstrap.
pub mod client;
pub mod config;
pub mod host;
mod sse;
mod transport;

pub use client::*;
