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
        let _ = requests_tx.send(request).await;
        time::delay_for(Duration::from_secs(delay)).await;
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

async fn tick(
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
        let _broker = Broker::new(1880, false).await;
        time::delay_for(Duration::from_secs(10)).await;
    });

    time::delay_for(Duration::from_secs(1)).await;
    let options = MqttOptions::new("dummy", "127.0.0.1", 1880);
    let mut eventloop = EventLoop::new(options, 5);

    let start = Instant::now();
    let o = eventloop.poll().await;
    let elapsed = start.elapsed();

    assert_matches!(o, Err(ConnectionError::Timeout(_)));
    assert_eq!(elapsed.as_secs(), 5);
}

#[tokio::test]
async fn idle_connection_triggers_pings_on_time() {
    let mut options = MqttOptions::new("dummy", "127.0.0.1", 1885);
    options.set_keep_alive(5);
    let keep_alive = options.keep_alive();

    // start sending requests
    let mut eventloop = EventLoop::new(options, 5);
    // start the eventloop
    task::spawn(async move {
        run(&mut eventloop, false).await.unwrap();
    });

    let mut broker = Broker::new(1885, true).await;

    // check incoming rate at th broker
    let start = Instant::now();
    let mut ping_received = false;

    for _ in 0..10 {
        let packet = broker.read_packet().await;
        let elapsed = start.elapsed();
        if let Packet::PingReq = packet {
            ping_received = true;
            assert_eq!(elapsed.as_secs(), keep_alive.as_secs());
            break;
        }
    }

    assert!(ping_received);
}

#[tokio::test]
async fn some_outgoing_and_no_incoming_packets_should_trigger_pings_on_time() {
    let mut options = MqttOptions::new("dummy", "127.0.0.1", 1886);
    options.set_keep_alive(5);
    let keep_alive = options.keep_alive();

    // start sending qos0 publishes. this makes sure that there is
    // outgoing activity but no incomin activity
    let mut eventloop = EventLoop::new(options, 5);
    let requests_tx = eventloop.handle();
    task::spawn(async move {
        start_requests(10, QoS::AtMostOnce, 1, requests_tx).await;
    });

    // start the eventloop
    task::spawn(async move {
        run(&mut eventloop, false).await.unwrap();
    });

    let mut broker = Broker::new(1886, true).await;

    let start = Instant::now();
    let mut ping_received = false;

    for _ in 0..10 {
        let packet = broker.read_packet_and_respond().await;
        let elapsed = start.elapsed();
        if let Packet::PingReq = packet {
            ping_received = true;
            assert_eq!(elapsed.as_secs(), keep_alive.as_secs());
            break;
        }
    }

    assert!(ping_received);
}

#[tokio::test]
async fn some_incoming_and_no_outgoing_packets_should_trigger_pings_on_time() {
    let mut options = MqttOptions::new("dummy", "127.0.0.1", 2000);
    options.set_keep_alive(5);
    let keep_alive = options.keep_alive();

    let mut eventloop = EventLoop::new(options, 5);
    task::spawn(async move {
        run(&mut eventloop, false).await.unwrap();
    });

    let mut broker = Broker::new(2000, true).await;
    let start = Instant::now();
    broker
        .start_publishes(5, QoS::AtMostOnce, Duration::from_secs(1))
        .await;
    let packet = broker.read_packet().await;
    match packet {
        Packet::PingReq => (),
        packet => panic!("Expecting pingreq. Found = {:?}", packet),
    };
    assert_eq!(start.elapsed().as_secs(), keep_alive.as_secs());
}

#[tokio::test]
async fn detects_halfopen_connections_in_the_second_ping_request() {
    let mut options = MqttOptions::new("dummy", "127.0.0.1", 2001);
    options.set_keep_alive(5);

    // A broker which consumes packets but doesn't reply
    task::spawn(async move {
        let mut broker = Broker::new(2001, true).await;
        broker.blackhole().await;
    });

    time::delay_for(Duration::from_secs(1)).await;
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

#[tokio::test]
async fn requests_are_blocked_after_max_inflight_queue_size() {
    let mut options = MqttOptions::new("dummy", "127.0.0.1", 1887);
    options.set_inflight(5);
    let inflight = options.inflight();

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

    let mut broker = Broker::new(1887, true).await;
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

    let mut eventloop = EventLoop::new(options, 5);
    let requests_tx = eventloop.handle();

    task::spawn(async move {
        start_requests(5, QoS::AtLeastOnce, 1, requests_tx).await;
        time::delay_for(Duration::from_secs(60)).await;
    });

    // start the eventloop
    task::spawn(async move {
        run(&mut eventloop, true).await.unwrap();
    });

    let mut broker = Broker::new(1888, true).await;

    // packet 1
    let packet = broker.read_publish().await;
    assert!(packet.is_some());
    // packet 2
    let packet = broker.read_publish().await;
    assert!(packet.is_some());
    // packet 3
    let packet = broker.read_publish().await;
    assert!(packet.is_some());

    // no packet 4. client inflight full as there aren't acks yet
    let packet = broker.read_publish().await;
    assert!(packet.is_none());

    // ack packet 1 and client would produce packet 4
    broker.ack(1).await;
    let packet = broker.read_publish().await;
    assert!(packet.is_some());
    let packet = broker.read_publish().await;
    assert!(packet.is_none());

    // ack packet 2 and client would produce packet 5
    broker.ack(2).await;
    let packet = broker.read_publish().await;
    assert!(packet.is_some());
    let packet = broker.read_publish().await;
    assert!(packet.is_none());
}

#[tokio::test]
async fn packet_id_collisions_are_detected_and_flow_control_is_applied_correctly() {
    let mut options = MqttOptions::new("dummy", "127.0.0.1", 1891);
    options.set_inflight(4).set_collision_safety(true);

    let mut eventloop = EventLoop::new(options, 5);
    let requests_tx = eventloop.handle();

    task::spawn(async move {
        start_requests(10, QoS::AtLeastOnce, 0, requests_tx).await;
        time::delay_for(Duration::from_secs(60)).await;
    });

    task::spawn(async move {
        let mut broker = Broker::new(1891, true).await;
        // read all incoming packets first
        for i in 1..=4 {
            let packet = broker.read_publish().await;
            assert_eq!(packet.unwrap().payload[0], i);
        }

        // out of order ack
        broker.ack(3).await;
        broker.ack(4).await;
        time::delay_for(Duration::from_secs(5)).await;
        broker.ack(1).await;
        broker.ack(2).await;

        // read and ack remaining packets in order
        for i in 5..=10 {
            let packet = broker.read_publish().await;
            let packet = packet.unwrap();
            assert_eq!(packet.payload[0], i);
            broker.ack(packet.pkid).await;
        }

        time::delay_for(Duration::from_secs(5)).await;
    });

    time::delay_for(Duration::from_secs(1)).await;
    // sends 4 requests and receives ack 3, 4
    // 5th request will trigger collision
    match run(&mut eventloop, false).await {
        Err(ConnectionError::MqttState(StateError::Collision(1))) => (),
        o => panic!("Expecting collision error. Found = {:?}", o),
    }

    // Next poll will receive ack = 1 in 5 seconds and fixes collision
    let start = Instant::now();
    assert_eq!(
        eventloop.poll().await.unwrap(),
        Event::Incoming(Packet::PubAck(PubAck::new(1)))
    );
    assert_eq!(start.elapsed().as_secs(), 5);

    // Next poll unblocks failed publish due to collision
    assert_eq!(
        eventloop.poll().await.unwrap(),
        Event::Outgoing(Outgoing::Publish(1))
    );

    // handle remaining outgoing and incoming packets
    tick(&mut eventloop, false, 10).await.unwrap();
}

#[tokio::test]
async fn packet_id_collisions_are_timedout_on_second_ping() {
    let mut options = MqttOptions::new("dummy", "127.0.0.1", 1892);
    options
        .set_inflight(4)
        .set_collision_safety(true)
        .set_keep_alive(5);

    let mut eventloop = EventLoop::new(options, 5);
    let requests_tx = eventloop.handle();

    task::spawn(async move {
        start_requests(10, QoS::AtLeastOnce, 0, requests_tx).await;
        time::delay_for(Duration::from_secs(60)).await;
    });

    task::spawn(async move {
        let mut broker = Broker::new(1892, true).await;
        // read all incoming packets first
        for i in 1..=4 {
            let packet = broker.read_publish().await;
            assert_eq!(packet.unwrap().payload[0], i);
        }

        // out of order ack
        broker.ack(3).await;
        broker.ack(4).await;
        time::delay_for(Duration::from_secs(15)).await;
    });

    time::delay_for(Duration::from_secs(1)).await;

    // Collision error but no network disconneciton
    match run(&mut eventloop, false).await {
        Err(ConnectionError::MqttState(StateError::Collision(1))) => (),
        o => panic!("Expecting collision error. Found = {:?}", o),
    }

    match run(&mut eventloop, false).await {
        Err(ConnectionError::MqttState(StateError::CollisionTimeout)) => (),
        o => panic!("Expecting collision error. Found = {:?}", o),
    }
}

#[tokio::test]
async fn reconnection_resumes_from_the_previous_state() {
    let mut options = MqttOptions::new("dummy", "127.0.0.1", 1889);
    options.set_keep_alive(5);

    // start sending qos0 publishes. Makes sure that there is out activity but no in activity
    let mut eventloop = EventLoop::new(options, 5);
    let requests_tx = eventloop.handle();
    task::spawn(async move {
        start_requests(10, QoS::AtLeastOnce, 1, requests_tx).await;
        time::delay_for(Duration::from_secs(10)).await;
    });

    // start the eventloop
    task::spawn(async move {
        run(&mut eventloop, true).await.unwrap();
    });

    // broker connection 1
    let mut broker = Broker::new(1889, true).await;
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
    let mut broker = Broker::new(1889, true).await;
    for i in 3..=4 {
        let packet = broker.read_publish().await.unwrap();
        assert_eq!(i, packet.payload[0]);
        broker.ack(packet.pkid).await;
    }
}

#[tokio::test]
async fn reconnection_resends_unacked_packets_from_the_previous_connection_first() {
    let mut options = MqttOptions::new("dummy", "127.0.0.1", 1890);
    options.set_keep_alive(5);

    // start sending qos0 publishes. this makes sure that there is
    // outgoing activity but no incoming activity
    let mut eventloop = EventLoop::new(options, 5);
    let requests_tx = eventloop.handle();
    task::spawn(async move {
        start_requests(10, QoS::AtLeastOnce, 1, requests_tx).await;
        time::delay_for(Duration::from_secs(10)).await;
    });

    // start the client eventloop
    task::spawn(async move {
        run(&mut eventloop, true).await.unwrap();
    });

    // broker connection 1. receive but don't ack
    let mut broker = Broker::new(1890, true).await;
    for i in 1..=2 {
        let packet = broker.read_publish().await.unwrap();
        assert_eq!(i, packet.payload[0]);
    }

    // broker connection 2 receives from scratch
    let mut broker = Broker::new(1890, true).await;
    for i in 1..=6 {
        let packet = broker.read_publish().await.unwrap();
        assert_eq!(i, packet.payload[0]);
    }
}
