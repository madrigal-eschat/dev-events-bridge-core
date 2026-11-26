//! Shared CloudEvent filtering and MQTT routing for dev-events bridges.

mod bridge;
mod config;
mod event;

pub use bridge::{handle_message, run};
pub use config::{AuthConfig, Broker, CoreConfig, Filter, Rule};
pub use event::CloudEvent;

/// The application-specific part of a routing rule.
pub trait Action: serde::de::DeserializeOwned + std::fmt::Debug {
    /// Shared state initialized by the application before running the bridge.
    type Context;

    /// MQTT client ID to use when the configuration omits one.
    const DEFAULT_CLIENT_ID: &'static str;

    /// Apply this rule to a matching event.
    ///
    /// **Handle every expected or expectable failure here.** Log it with
    /// `log::warn!` or `log::error!` and return `Ok(())`. Keep this method
    /// as close to infallible as possible. Returning `Err` signals an
    /// unrecoverable condition and terminates the entire bridge.
    #[allow(async_fn_in_trait)]
    async fn apply(&self, ctx: &Self::Context, event: &CloudEvent) -> anyhow::Result<()>;
}
