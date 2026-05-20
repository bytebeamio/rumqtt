#[cfg(feature = "websocket")]
#[allow(dead_code)]
mod broker;

//
// Request modifier tests (websocket feature)
//

#[cfg(feature = "websocket")]
#[derive(Debug)]
struct RequestModifierTestError(&'static str);

#[cfg(feature = "websocket")]
impl std::fmt::Display for RequestModifierTestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(feature = "websocket")]
impl std::error::Error for RequestModifierTestError {}

/// Test that a fallible request modifier error propagates to ConnectionError
#[cfg(feature = "websocket")]
#[tokio::test]
async fn fallible_request_modifier_error_propagates() {
    use rumqttc::{ConnectionError, EventLoop, MqttOptions, Transport};
    use std::time::Duration;
    use tokio::{task, time};

    use broker::Broker;

    // Start a broker to accept TCP connections
    task::spawn(async move {
        let _broker = Broker::new(3010, 0, false).await;
        time::sleep(Duration::from_secs(5)).await;
    });

    time::sleep(Duration::from_millis(100)).await;

    let mut options = MqttOptions::new("test", "ws://127.0.0.1:3010/mqtt", 3010);
    options.set_transport(Transport::Ws);

    // Set a modifier that always fails
    options.set_request_modifier(|_req| async move {
        Err(RequestModifierTestError("modifier failed intentionally"))
    });

    let mut eventloop = EventLoop::new(options, 5);

    // The connection should fail with RequestModifier error
    let result = time::timeout(Duration::from_secs(5), eventloop.poll()).await;

    match result {
        Ok(Err(ConnectionError::RequestModifier(e))) => {
            assert!(
                e.to_string().contains("modifier failed intentionally"),
                "Error message should contain our message, got: {}",
                e
            );
        }
        Ok(Err(e)) => panic!("Expected RequestModifier error, got: {:?}", e),
        Ok(Ok(event)) => panic!("Expected error, got event: {:?}", event),
        Err(_) => panic!("Test timed out"),
    }
}

/// Test that an infallible request modifier doesn't cause RequestModifier error
#[cfg(feature = "websocket")]
#[tokio::test]
async fn infallible_request_modifier_no_modifier_error() {
    use rumqttc::{ConnectionError, EventLoop, MqttOptions, Transport};
    use std::time::Duration;
    use tokio::{task, time};

    use broker::Broker;

    // Start a broker to accept TCP connections
    task::spawn(async move {
        let _broker = Broker::new(3011, 0, false).await;
        time::sleep(Duration::from_secs(5)).await;
    });

    time::sleep(Duration::from_millis(100)).await;

    let mut options = MqttOptions::new("test", "ws://127.0.0.1:3011/mqtt", 3011);
    options.set_transport(Transport::Ws);

    // Set an infallible modifier (backwards compatible API)
    options.set_request_modifier(|req| async move { req });

    let mut eventloop = EventLoop::new(options, 5);

    // The connection will fail (broker doesn't speak WS), but NOT with RequestModifier error
    let result = time::timeout(Duration::from_secs(5), eventloop.poll()).await;

    match result {
        Ok(Err(ConnectionError::RequestModifier(_))) => {
            panic!("Infallible modifier should never produce RequestModifier error")
        }
        Ok(Err(_)) => {
            // Expected: connection fails for other reasons (no WS support)
        }
        Ok(Ok(_)) => {
            // Unexpected but acceptable if somehow connected
        }
        Err(_) => {
            // Timeout is acceptable
        }
    }
}
