# CLAUDE.md

This file provides guidance to Claude Code when working in this repository.

## Commands

```sh
cargo check --locked
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo test <name>
cargo llvm-cov --locked --lcov --output-path lcov.info
```

CI builds from the committed lockfile and requires 100% coverage of changed
lines on pull requests. Set `MQTT_INTEGRATION_URL` to test with a loopback
Mosquitto broker.

## Architecture

The library subscribes to MQTT topics, decodes CloudEvents, matches filters and
calls the application's actions in rule order.

- `lib.rs` defines the `Action` trait and exports the public API.
- `config.rs` defines the shared configuration and filter matching.
- `event.rs` defines the payload schema with the `sourcetype` extension.
- `bridge.rs` runs MQTT and exposes `handle_message` for broker-free testing.

Applications own their context lifecycle and action-specific validation. Actions
log and absorb expected failures. An action error stops the bridge.

## Releases

Commits follow Conventional Commits. Semantic-release runs on pushes to
`main`, updates the manifest and lockfile versions, and publishes a Git tag
and GitHub release.
