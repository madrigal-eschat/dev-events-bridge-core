# Dev Events Bridge Core

This Rust library subscribes to MQTT topics, decodes CloudEvents, and runs
matching rules in configuration order. Each app supplies its own actions and
initializes any shared state before calling `run`. The app also handles cleanup
after `run` returns.

## Install

Add the Git dependency to your app's `Cargo.toml`.

<!-- markdownlint-disable MD013 -->

```toml
dev-events-bridge-core = { git = "https://github.com/madrigal-eschat/dev-events-bridge-core", tag = "v0.1.0" }
```

<!-- markdownlint-enable MD013 -->

## Define an action

**Handle every expected or expectable error in `Action::apply`.** Log it with
`log::warn!` or `log::error!` and return `Ok(())`. Keep the method as close
to infallible as possible. Returning `Err` signals an unrecoverable condition
that terminates the entire bridge.

```rust
use dev_events_bridge_core::{Action, CloudEvent, CoreConfig};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct LogAction {
    message: String,
}

impl Action for LogAction {
    type Context = ();
    const DEFAULT_CLIENT_ID: &'static str = "event-logger";

    async fn apply(&self, _: &(), event: &CloudEvent) -> anyhow::Result<()> {
        log::info!("{} (event {})", self.message, event.id);
        Ok(())
    }
}

#[derive(Deserialize)]
struct AppConfig {
    #[serde(flatten)]
    core: CoreConfig<LogAction>,
    logging: LoggingConfig,
}

#[derive(Deserialize)]
struct LoggingConfig {
    level: String,
}
```

Deserialize your app's configuration with a format such as YAML. Validate the
shared configuration with `CoreConfig::validate` and check app settings and
actions in your app. Initialize your logger and shared state, then call
`run(&config.core, &ctx).await`. `run` returns successfully on Ctrl-C and
returns an error if MQTT or an action fails.

## Configure the bridge

```yaml
broker:
  host: localhost
  port: 1883
topics:
  - dev-events/#
rules:
  - filter:
      type: devevents.task.failed
      source: editor/*
      data:
        exit_code: 1
    message: Task failed
logging:
  level: info
```

The broker port defaults to 1883. Set `broker.client_id` to override the
action's default. If the broker requires credentials, set
`broker.auth.username` and `broker.auth.password`.

The filter accepts `type`, `source`, `sourcetype`, `subject`, and `data`.
Every specified field must match. The four string fields support `*` for any
sequence of characters, including across `/`. Each `data` key must have the
exact JSON value, including its type. An empty `filter: {}` matches every
event. Action fields appear alongside `filter` in the rule.

## Develop

```sh
cargo fmt --check
cargo check --locked
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo llvm-cov --locked --lcov --output-path lcov.info
```

To test with Mosquitto, run a local broker on a free loopback port and set
`MQTT_INTEGRATION_URL=tcp://127.0.0.1:<port>` when running the tests.
