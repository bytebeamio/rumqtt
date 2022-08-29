use matches::assert_matches;
use std::time::{Duration, Instant};
use tokio::{task, time};

use rumqttc::*;
use broker::Broker;

async fn run(eventloop: &mut EventLoop, reconnect: bool) -> Result<(), ConnectionError> {
    'reconnect: loop {
        loop {
            let o = eventloop.poll().await;
            println!("Polled = {:?}", o);
            match o {
                Ok(_) => continue,
                Err(_) if reconnect => continue 'reconnect,
                Err(e) => return Err(e),
            }
        }
    }
}

async fn _tick(
    eventloop: &mut EventLoop,
    reconnect: bool,
    count: usize,
) -> Result<(), ConnectionError> {
    'reconnect: loop {
        for i in 0..count {
            let o = eventloop.poll().await;
            println!("{}. Polled = {:?}", i, o);
            match o {
                Ok(_) => continue,
                Err(_) if reconnect => continue 'reconnect,
                Err(e) => return Err(e),
            }
        }

        break;
    }

    Ok(())
}

#[tokio::test]
async fn connection_should_timeout_on_time() {
    task::spawn(async move {
        let _broker = Broker::new(1880, 3).await;
        time::sleep(Duration::from_secs(10)).await;
    });

    time::sleep(Duration::from_secs(1)).await;
    let options = MqttOptions::new("dummy", "127.0.0.1", 1880);
    let mut eventloop = EventLoop::new(options, 5);

    let start = Instant::now();
    let o = eventloop.poll().await;
    let elapsed = start.elapsed();

    assert_matches!(o, Err(ConnectionError::Timeout(_)));
    assert_eq!(elapsed.as_secs(), 5);
}

//
// All keep alive tests here
//

#[tokio::test]
async fn idle_connection_triggers_pings_on_time() {
    let keep_alive = 5;

    let mut options = MqttOptions::new("dummy", "127.0.0.1", 1885);
    options.set_keep_alive(Duration::from_secs(keep_alive));

    // Create client eventloop and poll
    task::spawn(async move {
        let mut eventloop = EventLoop::new(options, 5);
        run(&mut eventloop, false).await.unwrap();
    });

    let mut broker = Broker::new(1885, 0).await;
    let mut count = 0;
    let mut start = Instant::now();

    for _ in 0..3 {
        let packet = broker.read_packet().await;
        match packet {
            Packet::PingReq => {
                count += 1;
                let elapsed = start.elapsed();
                assert_eq!(elapsed.as_secs(), keep_alive as u64);
                broker.pingresp().await;
                start = Instant::now();
            }
            _ => {
                panic!("Expecting ping, Received: {:?}", packet);
            }
        }
    }

    assert_eq!(count, 3);
}

#[tokio::test]
async fn some_incoming_and_no_outgoing_should_trigger_pings_on_time() {
    let keep_alive = 5;
    let mut options = MqttOptions::new("dummy", "127.0.0.1", 2000);

    options.set_keep_alive(Duration::from_secs(keep_alive));

    task::spawn(async move {
        let mut eventloop = EventLoop::new(options, 5);
        run(&mut eventloop, false).await.unwrap();
    });

    let mut broker = Broker::new(2000, 0).await;
    let mut count = 0;

    // Start sending qos 0 publishes to the client. This triggers
    // some incoming and no outgoing packets in the client
    broker.spawn_publishes(10, QoS::AtMostOnce, 1).await;

    let mut start = Instant::now();
    loop {
        let event = broker.tick().await;

        if event == Event::Incoming(Incoming::PingReq) {
            // wait for 3 pings
            count += 1;
            if count == 3 {
                break;
            }

            assert_eq!(start.elapsed().as_secs(), keep_alive as u64);
            broker.pingresp().await;
            start = Instant::now();
        }
    }

    assert_eq!(count, 3);
}

#[tokio::test]
async fn detects_halfopen_connections_in_the_second_ping_request() {
    let mut options = MqttOptions::new("dummy", "127.0.0.1", 2001);
    options.set_keep_alive(Duration::from_secs(5));

    // A broker which consumes packets but doesn't reply
    task::spawn(async move {
        let mut broker = Broker::new(2001, 0).await;
        broker.blackhole().await;
    });

    time::sleep(Duration::from_secs(1)).await;
    let start = Instant::now();
    let mut eventloop = EventLoop::new(options, 5);
    loop {
        if let Err(e) = eventloop.poll().await {
            match e {
                ConnectionError::MqttState(StateError::AwaitPingResp) => break,
                v => panic!("Expecting pingresp error. Found = {:?}", v),
            }
        }
    }

    assert_eq!(start.elapsed().as_secs(), 10);
}

//
// All flow control tests here
//

//
// All reconnection tests here
//
#[tokio::test]
async fn next_poll_after_connect_failure_reconnects() {
    let options = MqttOptions::new("dummy", "127.0.0.1", 3000);

    task::spawn(async move {
        let _broker = Broker::new(3000, 1).await;
        let _broker = Broker::new(3000, 0).await;
        time::sleep(Duration::from_secs(15)).await;
    });

    time::sleep(Duration::from_secs(1)).await;
    let mut eventloop = EventLoop::new(options, 5);

    match eventloop.poll().await {
        Err(ConnectionError::ConnectionRefused(ConnectReturnCode::BadUserNamePassword)) => (),
        v => panic!("Expected bad username password error. Found = {:?}", v),
    }

    match eventloop.poll().await {
        Ok(Event::Incoming(Packet::ConnAck(ConnAck {
            code: ConnectReturnCode::Success,
            session_present: false,
        }))) => (),
        v => panic!("Expected ConnAck Success. Found = {:?}", v),
    }
}
