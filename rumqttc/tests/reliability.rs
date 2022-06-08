use matches::assert_matches;
use std::time::{Duration, Instant};
use tokio::{task, time};

mod broker;

use broker::*;
use rumqttc::*;

async fn start_requests(count: u8, qos: QoS, delay: u64, requests_tx: Sender<Request>) {
    for i in 1..=count {
        let topic = "hello/world".to_owned();
        let payload = vec![i, 1, 2, 3];

        let publish = Publish::new(topic, qos, payload);
        let request = Request::Publish(publish);
        drop(requests_tx.send_async(request).await);
        time::sleep(Duration::from_secs(delay)).await;
    }
}

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
    let options = MqttOptions::builder()
        .port(1880)
        .client_id("dummy".parse().unwrap())
        .build();
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
    let options = MqttOptions::builder()
        .port(1885)
        .client_id("dummy".parse().unwrap())
        .keep_alive(Duration::from_secs(keep_alive))
        .build();

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
async fn some_outgoing_and_no_incoming_should_trigger_pings_on_time() {
    let keep_alive = 5;
    let options = MqttOptions::builder()
        .port(1886)
        .client_id("dummy".parse().unwrap())
        .keep_alive(Duration::from_secs(keep_alive))
        .build();

    // start sending qos0 publishes. this makes sure that there is
    // outgoing activity but no incoming activity
    let mut eventloop = EventLoop::new(options, 5);
    let requests_tx = eventloop.handle();

    // Start sending publishes
    task::spawn(async move {
        start_requests(10, QoS::AtMostOnce, 1, requests_tx).await;
    });

    // start the eventloop
    task::spawn(async move {
        run(&mut eventloop, false).await.unwrap();
    });

    let mut broker = Broker::new(1886, 0).await;
    let mut count = 0;
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
async fn some_incoming_and_no_outgoing_should_trigger_pings_on_time() {
    let keep_alive = 5;
    let options = MqttOptions::builder()
        .port(2000)
        .client_id("dummy".parse().unwrap())
        .keep_alive(Duration::from_secs(keep_alive))
        .build();

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
    let options = MqttOptions::builder()
        .port(2001)
        .client_id("dummy".parse().unwrap())
        .keep_alive(Duration::from_secs(5))
        .build();

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

#[tokio::test]
async fn requests_are_blocked_after_max_inflight_queue_size() {
    let inflight = 5;
    let options = MqttOptions::builder()
        .port(1887)
        .client_id("dummy".parse().unwrap())
        .inflight(inflight)
        .build();

    // start sending qos0 publishes. this makes sure that there is
    // outgoing activity but no incoming activity
    let mut eventloop = EventLoop::new(options, 5);
    let requests_tx = eventloop.handle();
    task::spawn(async move {
        start_requests(10, QoS::AtLeastOnce, 1, requests_tx).await;
    });

    // start the eventloop
    task::spawn(async move {
        run(&mut eventloop, false).await.unwrap();
    });

    let mut broker = Broker::new(1887, 0).await;
    for i in 1..=10 {
        let packet = broker.read_publish().await;

        if i > inflight {
            assert!(packet.is_none());
        }
    }
}

#[tokio::test]
async fn requests_are_recovered_after_inflight_queue_size_falls_below_max() {
    let options = MqttOptions::builder()
        .port(1888)
        .client_id("dummy".parse().unwrap())
        .inflight(3)
        .build();

    let mut eventloop = EventLoop::new(options, 5);
    let requests_tx = eventloop.handle();

    task::spawn(async move {
        start_requests(5, QoS::AtLeastOnce, 1, requests_tx).await;
        time::sleep(Duration::from_secs(60)).await;
    });

    // start the eventloop
    task::spawn(async move {
        run(&mut eventloop, true).await.unwrap();
    });

    let mut broker = Broker::new(1888, 0).await;

    // packet 1, 2, and 3
    assert!(broker.read_publish().await.is_some());
    assert!(broker.read_publish().await.is_some());
    assert!(broker.read_publish().await.is_some());

    // no packet 4. client inflight full as there aren't acks yet
    assert!(broker.read_publish().await.is_none());

    // ack packet 1 and client would produce packet 4
    broker.ack(1).await;
    assert!(broker.read_publish().await.is_some());
    assert!(broker.read_publish().await.is_none());

    // ack packet 2 and client would produce packet 5
    broker.ack(2).await;
    assert!(broker.read_publish().await.is_some());
    assert!(broker.read_publish().await.is_none());
}

#[tokio::test]
async fn packet_id_collisions_are_detected_and_flow_control_is_applied() {
    let options = MqttOptions::builder()
        .port(1891)
        .client_id("dummy".parse().unwrap())
        .inflight(10)
        .build();

    let mut eventloop = EventLoop::new(options, 5);
    let requests_tx = eventloop.handle();

    task::spawn(async move {
        start_requests(15, QoS::AtLeastOnce, 0, requests_tx).await;
        time::sleep(Duration::from_secs(60)).await;
    });

    task::spawn(async move {
        let mut broker = Broker::new(1891, 0).await;

        // read all incoming packets first
        for i in 1..=4 {
            let packet = broker.read_publish().await;
            assert_eq!(packet.unwrap().payload[0], i);
        }

        // out of order ack
        broker.ack(3).await;
        broker.ack(4).await;
        time::sleep(Duration::from_secs(5)).await;
        broker.ack(1).await;
        broker.ack(2).await;

        // read and ack remaining packets in order
        for i in 5..=15 {
            let packet = broker.read_publish().await;
            let packet = packet.unwrap();
            assert_eq!(packet.payload[0], i);
            broker.ack(packet.pkid).await;
        }

        time::sleep(Duration::from_secs(10)).await;
    });

    time::sleep(Duration::from_secs(1)).await;

    // sends 4 requests. 5th request will trigger collision
    // Poll until there is collision.
    loop {
        match eventloop.poll().await.unwrap() {
            Event::Outgoing(Outgoing::AwaitAck(1)) => break,
            v => {
                println!("Poll = {:?}", v);
                continue;
            }
        }
    }

    loop {
        let start = Instant::now();
        let event = eventloop.poll().await.unwrap();
        println!("Poll = {:?}", event);

        match event {
            Event::Outgoing(Outgoing::Publish(ack)) => {
                if ack == 1 {
                    let elapsed = start.elapsed().as_millis() as i64;
                    let deviation_millis: i64 = (5000 - elapsed).abs();
                    assert!(deviation_millis < 100);
                    break;
                }
            }
            _ => continue,
        }
    }
}

// #[tokio::test]
// async fn packet_id_collisions_are_timedout_on_second_ping() {
//     let mut options = MqttOptions::new("dummy", "127.0.0.1", 1892);
//     options.set_inflight(4).set_keep_alive(5);
//
//     let mut eventloop = EventLoop::new(options, 5);
//     let requests_tx = eventloop.handle();
//
//     task::spawn(async move {
//         start_requests(10, QoS::AtLeastOnce, 0, requests_tx).await;
//         time::sleep(Duration::from_secs(60)).await;
//     });
//
//     task::spawn(async move {
//         let mut broker = Broker::new(1892, 0).await;
//         // read all incoming packets first
//         for i in 1..=4 {
//             let packet = broker.read_publish().await;
//             assert_eq!(packet.unwrap().payload[0], i);
//         }
//
//         // out of order ack
//         broker.ack(3).await;
//         broker.ack(4).await;
//         time::sleep(Duration::from_secs(15)).await;
//     });
//
//     time::sleep(Duration::from_secs(1)).await;
//
//     // Collision error but no network disconneciton
//     match run(&mut eventloop, false).await.unwrap() {
//         Event::Outgoing(Outgoing::AwaitAck(1)) => (),
//         o => panic!("Expecting collision error. Found = {:?}", o),
//     }
//
//     match run(&mut eventloop, false).await {
//         Err(ConnectionError::MqttState(StateError::CollisionTimeout)) => (),
//         o => panic!("Expecting collision error. Found = {:?}", o),
//     }
// }

//
// All reconnection tests here
//
#[tokio::test]
async fn next_poll_after_connect_failure_reconnects() {
    let options = MqttOptions::builder()
        .port(3000)
        .client_id("dummy".parse().unwrap())
        .build();

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

#[tokio::test]
async fn reconnection_resumes_from_the_previous_state() {
    let options = MqttOptions::builder()
        .port(3001)
        .client_id("dummy".parse().unwrap())
        .keep_alive(Duration::from_secs(5))
        .build();

    // start sending qos0 publishes. Makes sure that there is out activity but no in activity
    let mut eventloop = EventLoop::new(options, 5);
    let requests_tx = eventloop.handle();
    task::spawn(async move {
        start_requests(10, QoS::AtLeastOnce, 1, requests_tx).await;
        time::sleep(Duration::from_secs(10)).await;
    });

    // start the eventloop
    task::spawn(async move {
        run(&mut eventloop, true).await.unwrap();
    });

    // broker connection 1
    let mut broker = Broker::new(3001, 0).await;
    for i in 1..=2 {
        let packet = broker.read_publish().await.unwrap();
        assert_eq!(i, packet.payload[0]);
        broker.ack(packet.pkid).await;
    }

    // NOTE: An interesting thing to notice here is that reassigning a new broker
    // is behaving like a half-open connection instead of cleanly closing the socket
    // and returning error immediately
    // Manually dropping (`drop(broker.framed)`) the connection or adding
    // a block around broker with {} is closing the connection as expected

    // broker connection 2
    let mut broker = Broker::new(3001, 0).await;
    for i in 3..=4 {
        let packet = broker.read_publish().await.unwrap();
        assert_eq!(i, packet.payload[0]);
        broker.ack(packet.pkid).await;
    }
}

#[tokio::test]
async fn reconnection_resends_unacked_packets_from_the_previous_connection_first() {
    let options = MqttOptions::builder()
        .port(3002)
        .client_id("dummy".parse().unwrap())
        .keep_alive(Duration::from_secs(5))
        .build();

    // start sending qos0 publishes. this makes sure that there is
    // outgoing activity but no incoming activity
    let mut eventloop = EventLoop::new(options, 5);
    let requests_tx = eventloop.handle();
    task::spawn(async move {
        start_requests(10, QoS::AtLeastOnce, 1, requests_tx).await;
        time::sleep(Duration::from_secs(10)).await;
    });

    // start the client eventloop
    task::spawn(async move {
        run(&mut eventloop, true).await.unwrap();
    });

    // broker connection 1. receive but don't ack
    let mut broker = Broker::new(3002, 0).await;
    for i in 1..=2 {
        let packet = broker.read_publish().await.unwrap();
        assert_eq!(i, packet.payload[0]);
    }

    // broker connection 2 receives from scratch
    let mut broker = Broker::new(3002, 0).await;
    for i in 1..=6 {
        let packet = broker.read_publish().await.unwrap();
        assert_eq!(i, packet.payload[0]);
    }
}
