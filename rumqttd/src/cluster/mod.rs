//! Multi-node cluster support for rumqttd.
//!
//! Provides node discovery, health tracking, and inter-node communication
//! over TCP using a length-prefixed framing protocol. Each node maintains
//! outbound connections to all configured nodes and a listener for inbound
//! connections, exchanging periodic ping-pong messages to track health.
//!
//! # Modules
//!
//! - [`cluster`] — Core cluster state, health tracking, and networking loop.
//! - [`config`] — TOML-deserializable cluster configuration and resolved connection config.
//! - [`framing`] — Length-prefixed TCP frame encoding/decoding.
//! - [`outbound`] — [`OutboundNode`]: per-node outbound ping-pong loop with reconnection.
//! - [`outbound_result`] — Result and event enums for the outbound subsystem.
//! - [`replication`] — Ping and pong message formats for inter-node heartbeats.
//!
//! [`OutboundNode`]: outbound::OutboundNode

pub mod cluster;
pub mod config;
pub(crate) mod framing;
pub(crate) mod outbound;
pub(crate) mod outbound_result;
mod replication;
