use std::time::Duration;

use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, ConnectionError, MqttOptions};
use rumqttc::NetworkOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::{task, time};

/// MQTT v5 CONNACK: session present = 0, reason = success, no properties.
const CONNACK: [u8; 5] = [0x20, 0x03, 0x00, 0x00, 0x00];

/// Accepts a single connection, answers CONNECT with a CONNACK and then never reads
/// from the socket again, so the client's TCP send buffer eventually fills up.
async fn start_stalled_broker() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    task::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = stream.read(&mut buf).await.unwrap();
        stream.write_all(&CONNACK).await.unwrap();
        // Hold the connection open without reading.
        time::sleep(Duration::from_secs(3600)).await;
        let _ = stream;
    });

    port
}

async fn assert_flush_timeout_when_broker_stops_reading(qos: QoS) {
    let port = start_stalled_broker().await;

    let mut options = MqttOptions::new("stalled", "127.0.0.1", port);
    options.set_keep_alive(Duration::from_secs(5));
    let mut network_options = NetworkOptions::new();
    network_options.set_tcp_send_buffer_size(1024);
    options.set_network_options(network_options);
    // v5 bounds network writes with `MqttOptions::connection_timeout`.
    options.set_connection_timeout(1);

    let (client, mut eventloop) = AsyncClient::new(options, 10);
    task::spawn(async move {
        let payload = vec![0u8; 16 * 1024];
        loop {
            if client
                .publish("hello/world", qos, false, payload.clone())
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let result = time::timeout(Duration::from_secs(30), async {
        loop {
            if let Err(e) = eventloop.poll().await {
                return e;
            }
        }
    })
    .await;

    match result {
        Ok(ConnectionError::FlushTimeout) => {}
        Ok(e) => panic!("expected FlushTimeout, got {e:?}"),
        Err(_) => panic!("eventloop.poll() hung while the broker was not reading"),
    }
}

#[tokio::test]
async fn poll_returns_flush_timeout_when_broker_stops_reading_qos0() {
    assert_flush_timeout_when_broker_stops_reading(QoS::AtMostOnce).await;
}

#[tokio::test]
async fn poll_returns_flush_timeout_when_broker_stops_reading_qos1() {
    assert_flush_timeout_when_broker_stops_reading(QoS::AtLeastOnce).await;
}
