use matches::assert_matches;
use std::time::{Duration, Instant};
use tokio::{
    task,
    time::{self, timeout},
};

mod broker;

use broker::*;
use rumqttc::*;

async fn start_requests(count: u8, qos: QoS, delay: u64, client: AsyncClient) {
    for i in 1..=count {
        let topic = "hello/world".to_owned();
        let payload = vec![i, 1, 2, 3];

        let _ = client.publish(topic, qos, false, payload).await;
        time::sleep(Duration::from_secs(delay)).await;
    }
}

async fn start_requests_with_payload(
    count: u8,
    qos: QoS,
    delay: u64,
    client: AsyncClient,
    payload: usize,
) {
    for i in 1..=count {
        let topic = "hello/world".to_owned();
        let payload = vec![i; payload];

        let _ = client.publish(topic, qos, false, payload).await;
        time::sleep(Duration::from_secs(delay)).await;
    }
}

async fn run(eventloop: &mut EventLoop, reconnect: bool) -> Result<(), ConnectionError> {
    'reconnect: loop {
        loop {
            let o = eventloop.poll().await;
            println!("Polled = {o:?}");
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
            println!("{i}. Polled = {o:?}");
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
        let _broker = Broker::new(1880, 3, false).await;
        time::sleep(Duration::from_secs(10)).await;
    });

    time::sleep(Duration::from_secs(1)).await;
    let options = MqttOptions::new("dummy", "127.0.0.1", 1880);
    let mut eventloop = EventLoop::new(options, 5);

    let start = Instant::now();
    let o = eventloop.poll().await;
    let elapsed = start.elapsed();

    dbg!(&o);
    assert_matches!(o, Err(ConnectionError::NetworkTimeout));
    assert_eq!(elapsed.as_secs(), 5);
}

//
// All keep alive tests here
//

#[test]
#[should_panic]
fn test_invalid_keep_alive_value() {
    let mut options = MqttOptions::new("dummy", "127.0.0.1", 1885);
    options.set_keep_alive(Duration::from_millis(10));
}

#[test]
fn test_zero_keep_alive_values() {
    let mut options = MqttOptions::new("dummy", "127.0.0.1", 1885);
    options.set_keep_alive(Duration::ZERO);
}

#[test]
fn test_valid_keep_alive_values() {
    let mut options = MqttOptions::new("dummy", "127.0.0.1", 1885);
    options.set_keep_alive(Duration::from_secs(1));
}

#[tokio::test]
async fn idle_connection_triggers_pings_on_time() {
    let keep_alive = 1;

    let mut options = MqttOptions::new("dummy", "127.0.0.1", 1885);
    options.set_keep_alive(Duration::from_secs(keep_alive));

    // Create client eventloop and poll
    task::spawn(async move {
        let mut eventloop = EventLoop::new(options, 5);
        run(&mut eventloop, false).await.unwrap();
    });

    let mut broker = Broker::new(1885, 0, false).await;
    let mut count = 0;
    let mut start = Instant::now();

    for _ in 0..3 {
        let packet = broker.read_packet().await.unwrap();
        match packet {
            Packet::PingReq => {
                count += 1;
                let elapsed = start.elapsed();
                assert_eq!(elapsed.as_secs(), { keep_alive });
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
    let mut options = MqttOptions::new("dummy", "127.0.0.1", 1886);

    options.set_keep_alive(Duration::from_secs(keep_alive));

    // start sending qos0 publishes. this makes sure that there is
    // outgoing activity but no incoming activity
    let (client, mut eventloop) = AsyncClient::new(options, 5);

    // Start sending publishes
    task::spawn(async move {
        start_requests(10, QoS::AtMostOnce, 1, client).await;
    });

    // start the eventloop
    task::spawn(async move {
        run(&mut eventloop, false).await.unwrap();
    });

    let mut broker = Broker::new(1886, 0, false).await;
    let mut count = 0;
    let mut start = Instant::now();

    loop {
        let event = broker.tick().await;

        if event == broker::Event::Incoming(Incoming::PingReq) {
            // wait for 3 pings
            count += 1;
            if count == 3 {
                break;
            }

            assert_eq!(start.elapsed().as_secs(), { keep_alive });
            broker.pingresp().await;
            start = Instant::now();
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

    let mut broker = Broker::new(2000, 0, false).await;
    let mut count = 0;

    // Start sending qos 0 publishes to the client. This triggers
    // some incoming and no outgoing packets in the client
    broker.spawn_publishes(10, QoS::AtMostOnce, 1).await;

    let mut start = Instant::now();
    loop {
        let event = broker.tick().await;

        if event == broker::Event::Incoming(Incoming::PingReq) {
            // wait for 3 pings
            count += 1;
            if count == 3 {
                break;
            }

            assert_eq!(start.elapsed().as_secs(), { keep_alive });
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
        let mut broker = Broker::new(2001, 0, false).await;
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
    let mut options = MqttOptions::new("dummy", "127.0.0.1", 1887);
    options.set_inflight(5);
    let inflight = options.inflight();

    // start sending qos0 publishes. this makes sure that there is
    // outgoing activity but no incoming activity
    let (client, mut eventloop) = AsyncClient::new(options, 5);
    task::spawn(async move {
        start_requests(10, QoS::AtLeastOnce, 1, client).await;
    });

    // start the eventloop
    task::spawn(async move {
        run(&mut eventloop, false).await.unwrap();
    });

    let mut broker = Broker::new(1887, 0, false).await;
    for i in 1..=10 {
        let packet = broker.read_publish().await;

        if i > inflight {
            assert!(packet.is_none());
        }
    }
}

#[tokio::test]
async fn requests_are_recovered_after_inflight_queue_size_falls_below_max() {
    let mut options = MqttOptions::new("dummy", "127.0.0.1", 1888);
    options.set_inflight(3);

    let (client, mut eventloop) = AsyncClient::new(options, 5);

    task::spawn(async move {
        start_requests(5, QoS::AtLeastOnce, 1, client).await;
        time::sleep(Duration::from_secs(60)).await;
    });

    // start the eventloop
    task::spawn(async move {
        run(&mut eventloop, true).await.unwrap();
    });

    let mut broker = Broker::new(1888, 0, false).await;

    // packet 1, 2, and 3
    assert!(broker.read_publish().await.is_some());
    assert!(broker.read_publish().await.is_some());
    assert!(broker.read_publish().await.is_some());

    // no packet 4. client inflight full as there aren't acks yet
    assert!(broker.read_publish().await.is_none());

    // ack packet 1 and client would produce packet 4
    broker.puback(1).await;
    assert!(broker.read_publish().await.is_some());
    assert!(broker.read_publish().await.is_none());

    // ack packet 2 and client would produce packet 5
    broker.puback(2).await;
    assert!(broker.read_publish().await.is_some());
    assert!(broker.read_publish().await.is_none());
}

#[ignore]
#[tokio::test]
async fn packet_id_collisions_are_detected_and_flow_control_is_applied() {
    let mut options = MqttOptions::new("dummy", "127.0.0.1", 1891);
    options.set_inflight(10);

    let (client, mut eventloop) = AsyncClient::new(options, 5);

    task::spawn(async move {
        start_requests(15, QoS::AtLeastOnce, 0, client).await;
        time::sleep(Duration::from_secs(60)).await;
    });

    task::spawn(async move {
        let mut broker = Broker::new(1891, 0, false).await;

        // read all incoming packets first
        for i in 1..=4 {
            let packet = broker.read_publish().await;
            assert_eq!(packet.unwrap().payload[0], i);
        }

        // out of order ack
        broker.puback(3).await;
        broker.puback(4).await;
        time::sleep(Duration::from_secs(5)).await;
        broker.puback(1).await;
        broker.puback(2).await;

        // read and ack remaining packets in order
        for i in 5..=15 {
            let packet = broker.read_publish().await;
            let packet = packet.unwrap();
            assert_eq!(packet.payload[0], i);
            broker.puback(packet.pkid).await;
        }

        time::sleep(Duration::from_secs(10)).await;
    });

    time::sleep(Duration::from_secs(1)).await;

    // sends 4 requests. 5th request will trigger collision
    // Poll until there is collision.
    loop {
        match eventloop.poll().await.unwrap() {
            rumqttc::Event::Outgoing(rumqttc::Outgoing::AwaitAck(1)) => break,
            v => {
                println!("Poll = {v:?}");
                continue;
            }
        }
    }

    loop {
        let start = Instant::now();
        let event = eventloop.poll().await.unwrap();
        println!("Poll = {event:?}");

        match event {
            rumqttc::Event::Outgoing(rumqttc::Outgoing::Publish(ack)) => {
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
    let options = MqttOptions::new("dummy", "127.0.0.1", 3000);

    task::spawn(async move {
        let _broker = Broker::new(3000, 1, false).await;
        let _broker = Broker::new(3000, 0, false).await;
        time::sleep(Duration::from_secs(15)).await;
    });

    time::sleep(Duration::from_secs(1)).await;
    let mut eventloop = EventLoop::new(options, 5);

    match eventloop.poll().await {
        Err(ConnectionError::ConnectionRefused(ConnectReturnCode::BadUserNamePassword)) => (),
        v => panic!("Expected bad username password error. Found = {:?}", v),
    }

    match eventloop.poll().await {
        Ok(rumqttc::Event::Incoming(Packet::ConnAck(ConnAck {
            code: ConnectReturnCode::Success,
            session_present: false,
        }))) => (),
        v => panic!("Expected ConnAck Success. Found = {:?}", v),
    }
}

#[tokio::test]
async fn reconnection_resumes_from_the_previous_state() {
    let mut options = MqttOptions::new("dummy", "127.0.0.1", 3001);
    options
        .set_keep_alive(Duration::from_secs(5))
        .set_clean_session(false);

    // start sending qos0 publishes. Makes sure that there is out activity but no in activity
    let (client, mut eventloop) = AsyncClient::new(options, 5);
    task::spawn(async move {
        start_requests(10, QoS::AtLeastOnce, 1, client).await;
        time::sleep(Duration::from_secs(10)).await;
    });

    // start the eventloop
    task::spawn(async move {
        run(&mut eventloop, true).await.unwrap();
    });

    // broker connection 1
    let mut broker = Broker::new(3001, 0, false).await;
    for i in 1..=2 {
        let packet = broker.read_publish().await.unwrap();
        assert_eq!(i, packet.payload[0]);
        broker.puback(packet.pkid).await;
    }

    // NOTE: An interesting thing to notice here is that reassigning a new broker
    // is behaving like a half-open connection instead of cleanly closing the socket
    // and returning error immediately
    // Manually dropping (`drop(broker.framed)`) the connection or adding
    // a block around broker with {} is closing the connection as expected

    // broker connection 2
    let mut broker = Broker::new(3001, 0, true).await;
    for i in 3..=4 {
        let packet = broker.read_publish().await.unwrap();
        assert_eq!(i, packet.payload[0]);
        broker.puback(packet.pkid).await;
    }
}

#[tokio::test]
async fn reconnection_resends_unacked_packets_from_the_previous_connection_first() {
    let mut options = MqttOptions::new("dummy", "127.0.0.1", 3002);
    options
        .set_keep_alive(Duration::from_secs(5))
        .set_clean_session(false);

    // start sending qos0 publishes. this makes sure that there is
    // outgoing activity but no incoming activity
    let (client, mut eventloop) = AsyncClient::new(options, 5);
    task::spawn(async move {
        start_requests(10, QoS::AtLeastOnce, 1, client).await;
        time::sleep(Duration::from_secs(10)).await;
    });

    // start the client eventloop
    task::spawn(async move {
        run(&mut eventloop, true).await.unwrap();
    });

    // broker connection 1. receive but don't ack
    let mut broker = Broker::new(3002, 0, false).await;
    for i in 1..=2 {
        let packet = broker.read_publish().await.unwrap();
        assert_eq!(i, packet.payload[0]);
    }

    // broker connection 2 receives from scratch
    let mut broker = Broker::new(3002, 0, true).await;
    for i in 1..=6 {
        let packet = broker.read_publish().await.unwrap();
        assert_eq!(i, packet.payload[0]);
    }
}

#[tokio::test]
async fn state_is_being_cleaned_properly_and_pending_request_calculated_properly() {
    let mut options = MqttOptions::new("dummy", "127.0.0.1", 3004);
    options.set_keep_alive(Duration::from_secs(5));
    let mut network_options = NetworkOptions::new();
    network_options.set_tcp_send_buffer_size(1024);

    let (client, mut eventloop) = AsyncClient::new(options, 5);
    eventloop.set_network_options(network_options);
    task::spawn(async move {
        start_requests_with_payload(100, QoS::AtLeastOnce, 0, client, 5000).await;
        time::sleep(Duration::from_secs(10)).await;
    });

    task::spawn(async move {
        let mut broker = Broker::new(3004, 0, false).await;
        while (broker.read_packet().await).is_some() {
            time::sleep(Duration::from_secs_f64(0.5)).await;
        }
    });

    let handle = task::spawn(async move {
        let res = run(&mut eventloop, false).await;
        if let Err(e) = res {
            match e {
                ConnectionError::FlushTimeout => {
                    assert!(eventloop.network.is_none());
                    println!("State is being clean properly");
                }
                _ => {
                    println!("Couldn't fill the TCP send buffer to run this test properly. Try reducing the size of buffer.");
                }
            }
        }
    });
    handle.await.unwrap();
}

#[tokio::test]
async fn resolve_on_qos0_before_write_to_tcp_buffer() {
    let options = MqttOptions::new("dummy", "127.0.0.1", 3005);
    let (client, mut eventloop) = AsyncClient::new(options, 5);

    task::spawn(async move {
        let res = run(&mut eventloop, false).await;
        if let Err(e) = res {
            match e {
                ConnectionError::FlushTimeout => {
                    assert!(eventloop.network.is_none());
                    println!("State is being clean properly");
                }
                _ => {
                    println!("Couldn't fill the TCP send buffer to run this test properly. Try reducing the size of buffer.");
                }
            }
        }
    });

    let mut broker = Broker::new(3005, 0, false).await;

    let token = client
        .publish("hello/world", QoS::AtMostOnce, false, [1; 1])
        .await
        .unwrap();

    // Token can resolve as soon as it was processed by eventloop
    assert_eq!(
        timeout(Duration::from_secs(1), token)
            .await
            .unwrap()
            .unwrap(),
        AckOfPub::None
    );

    // Verify the packet still reached broker
    // NOTE: this can't always be guaranteed
    let Packet::Publish(Publish {
        qos,
        topic,
        pkid,
        payload,
        ..
    }) = broker.read_packet().await.unwrap()
    else {
        unreachable!()
    };
    assert_eq!(topic, "hello/world");
    assert_eq!(qos, QoS::AtMostOnce);
    assert_eq!(payload.to_vec(), [1; 1]);
    assert_eq!(pkid, 0);
}

#[tokio::test]
async fn resolve_on_qos1_ack_from_broker() {
    let options = MqttOptions::new("dummy", "127.0.0.1", 3006);
    let (client, mut eventloop) = AsyncClient::new(options, 5);

    task::spawn(async move {
        let res = run(&mut eventloop, false).await;
        if let Err(e) = res {
            match e {
                ConnectionError::FlushTimeout => {
                    assert!(eventloop.network.is_none());
                    println!("State is being clean properly");
                }
                _ => {
                    println!("Couldn't fill the TCP send buffer to run this test properly. Try reducing the size of buffer.");
                }
            }
        }
    });

    let mut broker = Broker::new(3006, 0, false).await;

    let mut token = client
        .publish("hello/world", QoS::AtLeastOnce, false, [1; 1])
        .await
        .unwrap();

    // Token shouldn't resolve before reaching broker
    timeout(Duration::from_secs(1), &mut token)
        .await
        .unwrap_err();

    let Packet::Publish(Publish {
        qos,
        topic,
        pkid,
        payload,
        ..
    }) = broker.read_packet().await.unwrap()
    else {
        unreachable!()
    };
    assert_eq!(topic, "hello/world");
    assert_eq!(qos, QoS::AtLeastOnce);
    assert_eq!(payload.to_vec(), [1; 1]);
    assert_eq!(pkid, 1);

    // Token shouldn't resolve until packet is acked
    timeout(Duration::from_secs(1), &mut token)
        .await
        .unwrap_err();

    // Finally ack the packet
    broker.puback(1).await;

    // Token shouldn't resolve until packet is acked
    assert_eq!(
        timeout(Duration::from_secs(1), &mut token)
            .await
            .unwrap()
            .unwrap(),
        AckOfPub::PubAck(PubAck { pkid: 1 })
    );
}

#[tokio::test]
async fn resolve_on_qos2_ack_from_broker() {
    let options = MqttOptions::new("dummy", "127.0.0.1", 3007);
    let (client, mut eventloop) = AsyncClient::new(options, 5);

    task::spawn(async move {
        let res = run(&mut eventloop, false).await;
        if let Err(e) = res {
            match e {
                ConnectionError::FlushTimeout => {
                    assert!(eventloop.network.is_none());
                    println!("State is being clean properly");
                }
                _ => {
                    println!("Couldn't fill the TCP send buffer to run this test properly. Try reducing the size of buffer.");
                }
            }
        }
    });

    let mut broker = Broker::new(3007, 0, false).await;

    let mut token = client
        .publish("hello/world", QoS::ExactlyOnce, false, [1; 1])
        .await
        .unwrap();

    // Token shouldn't resolve before reaching broker
    timeout(Duration::from_secs(1), &mut token)
        .await
        .unwrap_err();

    let Packet::Publish(Publish {
        qos,
        topic,
        pkid,
        payload,
        ..
    }) = broker.read_packet().await.unwrap()
    else {
        unreachable!()
    };
    assert_eq!(topic, "hello/world");
    assert_eq!(qos, QoS::ExactlyOnce);
    assert_eq!(payload.to_vec(), [1; 1]);
    assert_eq!(pkid, 1);

    // Token shouldn't resolve till publish recorded
    timeout(Duration::from_secs(1), &mut token)
        .await
        .unwrap_err();

    // Record the publish message
    broker.pubrec(1).await;

    // Token shouldn't resolve till publish complete
    timeout(Duration::from_secs(1), &mut token)
        .await
        .unwrap_err();

    // Complete the publish message ack
    broker.pubcomp(1).await;

    // Finally the publish is QoS2 acked
    assert_eq!(
        timeout(Duration::from_secs(1), &mut token)
            .await
            .unwrap()
            .unwrap(),
        AckOfPub::PubComp(PubComp { pkid: 1 })
    );
}

#[tokio::test]
async fn resolve_on_sub_ack_from_broker() {
    let options = MqttOptions::new("dummy", "127.0.0.1", 3006);
    let (client, mut eventloop) = AsyncClient::new(options, 5);

    task::spawn(async move {
        let res = run(&mut eventloop, false).await;
        if let Err(e) = res {
            match e {
                ConnectionError::FlushTimeout => {
                    assert!(eventloop.network.is_none());
                    println!("State is being clean properly");
                }
                _ => {
                    println!("Couldn't fill the TCP send buffer to run this test properly. Try reducing the size of buffer.");
                }
            }
        }
    });

    let mut broker = Broker::new(3006, 0, false).await;

    let mut token = client
        .subscribe("hello/world", QoS::AtLeastOnce)
        .await
        .unwrap();

    // Token shouldn't resolve before reaching broker
    timeout(Duration::from_secs(1), &mut token)
        .await
        .unwrap_err();

    let Packet::Subscribe(Subscribe { pkid, filters, .. }) = broker.read_packet().await.unwrap()
    else {
        unreachable!()
    };
    assert_eq!(
        filters,
        [SubscribeFilter {
            path: "hello/world".to_owned(),
            qos: QoS::AtLeastOnce
        }]
    );
    assert_eq!(pkid, 1);

    // Token shouldn't resolve until packet is acked
    timeout(Duration::from_secs(1), &mut token)
        .await
        .unwrap_err();

    // Finally ack the packet
    broker.suback(1, QoS::AtLeastOnce).await;

    // Token shouldn't resolve until packet is acked
    assert_eq!(
        timeout(Duration::from_secs(1), &mut token)
            .await
            .unwrap()
            .unwrap()
            .pkid,
        1
    );
}

#[tokio::test]
async fn resolve_on_unsub_ack_from_broker() {
    let options = MqttOptions::new("dummy", "127.0.0.1", 3006);
    let (client, mut eventloop) = AsyncClient::new(options, 5);

    task::spawn(async move {
        let res = run(&mut eventloop, false).await;
        if let Err(e) = res {
            match e {
                ConnectionError::FlushTimeout => {
                    assert!(eventloop.network.is_none());
                    println!("State is being clean properly");
                }
                _ => {
                    println!("Couldn't fill the TCP send buffer to run this test properly. Try reducing the size of buffer.");
                }
            }
        }
    });

    let mut broker = Broker::new(3006, 0, false).await;

    let mut token = client.unsubscribe("hello/world").await.unwrap();

    // Token shouldn't resolve before reaching broker
    timeout(Duration::from_secs(1), &mut token)
        .await
        .unwrap_err();

    let Packet::Unsubscribe(Unsubscribe { topics, pkid, .. }) = broker.read_packet().await.unwrap()
    else {
        unreachable!()
    };
    assert_eq!(topics, vec!["hello/world"]);
    assert_eq!(pkid, 1);

    // Token shouldn't resolve until packet is acked
    timeout(Duration::from_secs(1), &mut token)
        .await
        .unwrap_err();

    // Finally ack the packet
    broker.unsuback(1).await;

    // Token shouldn't resolve until packet is acked
    assert_eq!(
        timeout(Duration::from_secs(1), &mut token)
            .await
            .unwrap()
            .unwrap(),
        UnsubAck { pkid: 1 }
    );
}
