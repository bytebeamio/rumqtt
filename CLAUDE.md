# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

TaleQTT is a fork of rumqtt - a high-performance MQTT broker and client library in Rust. Two main crates:
- **rumqttd**: Embeddable MQTT broker (supports v3.1.1 and v5.0)
- **rumqttc**: MQTT client library with async event loop

Current development (`feat/broker-cluster-setup`) adds cluster support for multi-node broker deployments.

## Build Commands

```bash
# Build all (workspace)
cargo build --release

# Build specific crate
cargo build --release -p rumqttd
cargo build --release -p rumqttc

# Run broker with config
cargo run --release --bin rumqttd -- -c rumqttd/rumqttd.toml -vvv
```

## Testing

```bash
# Test with all feature combinations (as CI does)
cargo hack --each-feature --optional-deps url test -p rumqttc -p rumqttd

# Test single crate
cargo test -p rumqttc
cargo test -p rumqttd

# Run specific test
cargo test -p rumqttc test_name -- --nocapture
```

## Linting

```bash
# Clippy with all feature combinations (CI requirement)
cargo hack clippy --verbose --each-feature --no-dev-deps --optional-deps url -p rumqttc -p rumqttd

# Doc check
cargo hack doc --verbose --no-deps --each-feature --no-dev-deps --optional-deps url -p rumqttc -p rumqttd
```

CI uses `RUSTFLAGS="-D warnings"` - all warnings are treated as errors.

## Architecture

### rumqttd Broker Architecture

Uses a **reactor pattern** with these core components:

- **Router** (`router/routing.rs`): Central event loop managing all data flow, message routing, and connection scheduling. Uses circular buffers (`ibufs`/`obufs`) for I/O and a ready queue for connection scheduling.

- **Links** (`link/`): Connection handlers for different purposes:
  - `remote.rs`: Client TCP/WebSocket connections
  - `bridge.rs`: Inter-broker connections
  - `console.rs`: HTTP API for metrics/management
  - `local.rs`: Programmatic publish/subscribe API

- **Segments** (`segments/`): Log-structured persistent message storage per topic

- **Connection** (`router/connection.rs`): Per-client state (QoS buffers, subscriptions, inflight tracking)

- **Cluster** (`cluster/`): New multi-node replication support (in progress)

### rumqttc Client Architecture

External event loop model - user controls the async runtime:
- `MqttOptions`: Configuration builder
- `AsyncClient`/`Client`: High-level API
- `EventLoop`: User-polled event loop handling network I/O and MQTT state

### Key Patterns

1. **Channel-based communication**: Uses flume channels; shared buffers for data, channels just notify
2. **No implicit threading**: Library provides async primitives, user controls spawning
3. **Connection state machine**: Pending → Busy (has pending data) → CaughtUp (all sent) → back to Busy on new data
4. **Backpressure handling**: Output buffer full → unschedule connection → reschedule on Ready event

## Configuration

Broker config: `rumqttd/rumqttd.toml` (TOML format)

Key sections: `[router]`, `[v4.*]`, `[v5.*]`, `[ws.*]`, `[console]`, `[prometheus]`, `[cluster]`

## Feature Flags

**rumqttd**: `use-rustls` (default), `use-native-tls`, `websocket` (default), `verify-client-cert`, `validate-tenant-prefix`, `allow-duplicate-clientid`

**rumqttc**: `use-rustls` (default), `use-native-tls`, `websocket`, `proxy`

## Commit Convention

Conventional commits: `<tag>(<component>): <title>`

Tags: `fix`, `feat`, `build`, `chore`, `ci`, `docs`, `style`, `refactor`, `perf`, `test`

Examples:
- `feat(router): Add dynamic log level updates`
- `fix(cluster): Handle node reconnection`
