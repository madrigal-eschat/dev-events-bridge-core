use std::future::Future;

use anyhow::Result;
use rumqttc::{AsyncClient, Event as MqttEvent, MqttOptions, Packet, QoS, SubscribeFilter};

use crate::{Action, CloudEvent, CoreConfig, Rule};

/// Subscribe to configured topics and route events until Ctrl-C or a fatal error.
pub async fn run<A: Action>(config: &CoreConfig<A>, ctx: &A::Context) -> Result<()> {
    let broker = &config.broker;
    let client_id = broker.client_id.as_deref().unwrap_or(A::DEFAULT_CLIENT_ID);
    let mut options = MqttOptions::new(client_id, &broker.host, broker.port);
    if let Some(auth) = &broker.auth {
        options.set_credentials(&auth.username, &auth.password);
    }

    let (client, mut eventloop) = AsyncClient::new(options, 64);
    let mqtt_loop = async {
        if !config.topics.is_empty() {
            let subscriptions = config
                .topics
                .iter()
                .map(|topic| SubscribeFilter::new(topic.clone(), QoS::AtMostOnce));
            client.subscribe_many(subscriptions).await?;
        }
        loop {
            if let MqttEvent::Incoming(Packet::Publish(publish)) = eventloop.poll().await? {
                handle_message(&config.rules, ctx, &publish.topic, &publish.payload).await?;
            }
        }
    };

    tokio::select! {
        result = mqtt_loop => result,
        result = wait_for_shutdown(tokio::signal::ctrl_c()) => result,
    }
}

async fn wait_for_shutdown(signal: impl Future<Output = std::io::Result<()>>) -> Result<()> {
    signal.await?;
    log::info!("received ctrl-c, shutting down");
    Ok(())
}

/// Decode one MQTT message and apply matching rules in configuration order.
///
/// Malformed messages are logged and skipped. The first action error is returned.
pub async fn handle_message<A: Action>(
    rules: &[Rule<A>],
    ctx: &A::Context,
    topic: &str,
    payload: &[u8],
) -> Result<()> {
    let Ok(payload) = std::str::from_utf8(payload) else {
        log::warn!("non-UTF8 payload on {topic}");
        return Ok(());
    };
    let cloud_event: CloudEvent = match serde_json::from_str(payload) {
        Ok(event) => event,
        Err(error) => {
            log::warn!("invalid CloudEvent on {topic}: {error}");
            return Ok(());
        }
    };

    let mut matched_any = false;
    for (index, rule) in rules.iter().enumerate() {
        if !rule.filter.matches(&cloud_event) {
            continue;
        }
        matched_any = true;
        log::debug!(
            "rule[{index}] matched event {:?} on {topic}",
            cloud_event.type_
        );
        rule.action.apply(ctx, &cloud_event).await?;
    }

    if !matched_any {
        log::debug!("no rule matched event {:?} on {topic}", cloud_event.type_);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_returns_success() {
        wait_for_shutdown(std::future::ready(Ok(()))).await.unwrap();
    }
}
