use std::cell::RefCell;

use dev_events_bridge_core::{Action, CloudEvent, CoreConfig, handle_message, run};
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, timeout};

#[derive(Debug, Deserialize)]
struct Record {
    number: u16,
    #[serde(default)]
    fail: bool,
}

impl Action for Record {
    type Context = RefCell<Vec<(u16, String)>>;
    const DEFAULT_CLIENT_ID: &'static str = "test-bridge";

    async fn apply(&self, ctx: &Self::Context, event: &CloudEvent) -> anyhow::Result<()> {
        ctx.borrow_mut().push((self.number, event.id.clone()));
        anyhow::ensure!(!self.fail, "action failed");
        Ok(())
    }
}

#[derive(Deserialize)]
struct AppConfig {
    #[serde(flatten)]
    core: CoreConfig<Record>,
    homeassistant: AppSettings,
}

#[derive(Deserialize)]
struct AppSettings {
    timeout: u32,
}

const PAYLOAD: &[u8] = br#"{"id":"test","type":"task.failed","source":"editor/a/b","sourcetype":"editor","data":{"code":1}}"#;

#[test]
fn wrapper_config_preserves_types_and_defaults() {
    let app: AppConfig = serde_yaml::from_str(
        "broker: {host: localhost}\ntopics: ['events/#']\n\
         rules:\n  - filter: {type: 'task.*', data: {code: 1}}\n    number: 7\n\
         homeassistant: {timeout: 50}",
    )
    .unwrap();
    assert_eq!(app.homeassistant.timeout, 50);
    assert_eq!(app.core.broker.port, 1883);
    assert!(app.core.broker.auth.is_none());
    assert!(app.core.broker.client_id.is_none());
    assert_eq!(app.core.topics, ["events/#"]);
    assert_eq!(app.core.rules[0].action.number, 7);
    assert!(!app.core.rules[0].action.fail);
    assert!(
        app.core.rules[0]
            .filter
            .matches(&serde_json::from_slice(PAYLOAD).unwrap())
    );
    app.core.validate().unwrap();
}

#[test]
fn explicit_broker_options() {
    let config: CoreConfig<Record> = serde_yaml::from_str(
        "broker: {host: example, port: 1884, client_id: app, auth: {username: user, password: pass}}\n\
         topics: []\nrules: []",
    )
    .unwrap();
    assert_eq!(config.broker.host, "example");
    assert_eq!(config.broker.port, 1884);
    assert_eq!(config.broker.client_id.as_deref(), Some("app"));
    let auth = config.broker.auth.unwrap();
    assert_eq!(auth.username, "user");
    assert_eq!(auth.password, "pass");
}

#[tokio::test]
async fn skips_invalid_and_unmatched_messages() {
    let config: CoreConfig<Record> = serde_yaml::from_str(
        "broker: {host: localhost}\ntopics: []\nrules: [{filter: {type: other}, number: 1}]",
    )
    .unwrap();
    let ctx = RefCell::default();
    for payload in [&[0xff][..], b"{}", PAYLOAD] {
        handle_message(&config.rules, &ctx, "events/test", payload)
            .await
            .unwrap();
    }
    assert!(ctx.into_inner().is_empty());
}

#[tokio::test]
async fn dispatches_in_order_and_returns_first_error() {
    let mut config: CoreConfig<Record> = serde_yaml::from_str(
        "broker: {host: localhost}\ntopics: []\nrules:\n\
         - {filter: {source: 'editor/*'}, number: 1}\n\
         - {filter: {type: other}, number: 2}\n\
         - {filter: {}, number: 3}\n\
         - {filter: {}, number: 4}",
    )
    .unwrap();
    let ctx = RefCell::default();
    handle_message(&config.rules, &ctx, "events/test", PAYLOAD)
        .await
        .unwrap();
    assert_eq!(
        *ctx.borrow(),
        [(1, "test".into()), (3, "test".into()), (4, "test".into())]
    );
    ctx.borrow_mut().clear();
    config.rules[2].action.fail = true;
    let error = handle_message(&config.rules, &ctx, "events/test", PAYLOAD)
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "action failed");
    assert_eq!(ctx.into_inner(), [(1, "test".into()), (3, "test".into())]);
}

fn mqtt_config(port: u16) -> CoreConfig<Record> {
    serde_yaml::from_str(&format!(
        "broker: {{host: 127.0.0.1, port: {port}}}\n\
         topics: ['events/#']\nrules: [{{filter: {{}}, number: 7, fail: true}}]"
    ))
    .unwrap()
}

async fn read_packet(stream: &mut TcpStream, header: u8) -> Vec<u8> {
    assert_eq!(stream.read_u8().await.unwrap(), header);
    let len = stream.read_u8().await.unwrap();
    assert!(len < 128);
    let mut payload = vec![0; len.into()];
    stream.read_exact(&mut payload).await.unwrap();
    payload
}

#[tokio::test]
async fn mqtt_routes_publish_and_propagates_action_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = mqtt_config(listener.local_addr().unwrap().port());
    let ctx = RefCell::default();
    let broker = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        let connect = read_packet(&mut stream, 0x10).await;
        assert!(connect.ends_with(b"test-bridge"));
        stream.write_all(&[0x20, 0x02, 0x00, 0x00]).await.unwrap();
        let subscribe = read_packet(&mut stream, 0x82).await;
        assert_eq!(&subscribe[2..], b"\0\x08events/#\0");
        stream
            .write_all(&[0x90, 0x03, subscribe[0], subscribe[1], 0])
            .await
            .unwrap();
        let mut publish = vec![0x30, 3 + PAYLOAD.len() as u8, 0, 1, b'e'];
        publish.extend_from_slice(PAYLOAD);
        stream.write_all(&publish).await.unwrap();
        assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
    };
    let (result, ()) = timeout(Duration::from_secs(5), async {
        tokio::join!(run(&config, &ctx), broker)
    })
    .await
    .unwrap();
    assert_eq!(result.unwrap_err().to_string(), "action failed");
    assert_eq!(ctx.into_inner(), [(7, "test".into())]);
}

#[tokio::test]
async fn mqtt_returns_event_loop_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = mqtt_config(listener.local_addr().unwrap().port());
    config.topics.clear();
    config.broker.client_id = Some("explicit-id".into());
    config.broker.auth = Some(dev_events_bridge_core::AuthConfig {
        username: "mqtt-user".into(),
        password: "mqtt-password".into(),
    });
    let ctx = RefCell::default();
    let broker = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        let connect = read_packet(&mut stream, 0x10).await;
        assert!(connect.ends_with(b"\0\x0bexplicit-id\0\x09mqtt-user\0\x0dmqtt-password"));
    };
    let (result, ()) = timeout(Duration::from_secs(5), async {
        tokio::join!(run(&config, &ctx), broker)
    })
    .await
    .unwrap();
    assert!(result.unwrap_err().is::<rumqttc::ConnectionError>());
    assert!(ctx.into_inner().is_empty());
}

#[tokio::test]
async fn mosquitto_routes_retained_event() {
    let Ok(url) = std::env::var("MQTT_INTEGRATION_URL") else {
        log::debug!("skipping Mosquitto test without MQTT_INTEGRATION_URL");
        return;
    };
    let endpoint = url.strip_prefix("tcp://127.0.0.1:").unwrap();
    let port = endpoint.parse().unwrap();
    let config = mqtt_config(port);
    let ctx = RefCell::default();
    let options = rumqttc::MqttOptions::new("integration-publisher", "127.0.0.1", port);
    let (publisher, mut events) = rumqttc::AsyncClient::new(options, 1);
    timeout(Duration::from_secs(5), async {
        publisher
            .publish("events/test", rumqttc::QoS::AtLeastOnce, true, PAYLOAD)
            .await
            .unwrap();
        while !matches!(
            events.poll().await.unwrap(),
            rumqttc::Event::Incoming(rumqttc::Packet::PubAck(_))
        ) {}
        let error = run(&config, &ctx).await.unwrap_err();
        assert_eq!(error.to_string(), "action failed");
        publisher
            .publish("events/test", rumqttc::QoS::AtLeastOnce, true, [])
            .await
            .unwrap();
        while !matches!(
            events.poll().await.unwrap(),
            rumqttc::Event::Incoming(rumqttc::Packet::PubAck(_))
        ) {}
    })
    .await
    .unwrap();
    assert_eq!(ctx.into_inner(), [(7, "test".into())]);
}
