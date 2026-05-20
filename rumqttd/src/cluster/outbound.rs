//! Outbound cluster connection management.
//!
//! [`OutboundNode`] owns all mutable state for a single outbound peer
//! connection: the TCP stream, the reader-event channel, and the reconnect
//! counter. Its [`OutboundNode::run`] method is the main ping-pong loop that
//! previously lived inside the free function `run_outbound`.

use crate::cluster::cluster::{Cluster, NodeStatus};
use crate::cluster::framing::{read_frame, write_frame};
use crate::cluster::outbound_result::{ConnectionResult, DrainResult, ReaderEvent};
use crate::cluster::replication::ping::PingReplicationMessage;
use crate::cluster::replication::pong::PongReplicationMessage;
use std::io;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::thread::{self, sleep};
use std::time::Duration;

const PING_INTERVAL_SECS: u64 = 15;
const CONNECT_RETRY_WAIT_SECS: u64 = 5;

/// Manages the outbound connection to a single remote cluster node.
///
/// Owns the TCP stream, the reader-event channel receiver, and the reconnect
/// counter so the main loop in [`OutboundNode::run`] stays thin.
pub(crate) struct OutboundNode {
    /// Remote address in `host:port` form (peer node's MQTT port, resolved
    /// to the cluster port by the caller before construction).
    pub addr: String,
    node_port: u16,
    max_missed_pings: u32,
    max_reconnect_attempts: u32,
    // ── mutable connection state ─────────────────────────────────────
    stream: Option<TcpStream>,
    reconnect_attempts: u32,
    rx: Option<flume::Receiver<ReaderEvent>>,
}

impl OutboundNode {
    pub fn new(
        addr: String,
        node_port: u16,
        max_missed_pings: u32,
        max_reconnect_attempts: u32,
    ) -> Self {
        OutboundNode {
            addr,
            node_port,
            max_missed_pings,
            max_reconnect_attempts,
            stream: None,
            reconnect_attempts: 0,
            rx: None,
        }
    }

    /// Attempts a single TCP connect to `self.addr`.
    ///
    /// On success: stores the stream, spawns a reader thread, resets the
    /// reconnect counter, and returns [`ConnectionResult::Connected`].
    ///
    /// On failure: increments the counter and returns [`ConnectionResult::RetryLater`]
    /// or [`ConnectionResult::GaveUp`] depending on whether the limit is reached.
    fn try_connect(&mut self) -> ConnectionResult {
        match TcpStream::connect(&self.addr) {
            Ok(s) => {
                println!("Connected to {}!", self.addr);
                self.reconnect_attempts = 0;

                let read_stream = s.try_clone().expect("Failed to clone TcpStream");
                self.stream = Some(s);

                let (tx, new_rx) = flume::unbounded();
                self.rx = Some(new_rx);

                thread::spawn(move || Self::run_reader(read_stream, tx));

                ConnectionResult::Connected
            }
            Err(_) => {
                self.reconnect_attempts += 1;
                println!(
                    "Failed to connect to {}, attempt {}/{}",
                    self.addr, self.reconnect_attempts, self.max_reconnect_attempts
                );
                if self.reconnect_attempts >= self.max_reconnect_attempts {
                    eprintln!(
                        "Giving up on {} after {} reconnect attempts",
                        self.addr, self.max_reconnect_attempts
                    );
                    ConnectionResult::GaveUp
                } else {
                    ConnectionResult::RetryLater
                }
            }
        }
    }

    /// Writes a PING frame to the live stream.
    ///
    /// # Errors
    ///
    /// Returns an [`io::Error`] if the write fails (e.g. broken pipe).
    fn send_ping(&mut self) -> Result<(), io::Error> {
        let ping = PingReplicationMessage::new(self.node_port);
        write_frame(self.stream.as_mut().unwrap(), &ping)?;
        println!("Sent PING to {}", self.addr);
        Ok(())
    }

    /// Drops the stream and channel receiver so the next loop iteration
    /// triggers a fresh `try_connect`.
    fn reset_connection(&mut self) {
        self.stream = None;
        self.rx = None;
    }

    /// Non-blocking drain of all pending reader events.
    ///
    /// * [`ReaderEvent::Pong`] → calls `cluster.record_success` and notes a
    ///   pong was received.
    /// * [`ReaderEvent::InvalidFrame`] → calls `cluster.record_failure` and
    ///   continues draining.
    /// * [`ReaderEvent::ConnectionLost`] → returns [`DrainResult::ConnectionLost`]
    ///   immediately (caller is responsible for resetting the connection).
    ///
    /// Returns [`DrainResult::PongReceived`] if at least one pong arrived,
    /// [`DrainResult::NoPong`] otherwise.
    fn drain_events(&mut self, cluster: &Arc<Mutex<Cluster>>) -> DrainResult {
        let mut pong_received = false;

        if let Some(ref event_rx) = self.rx {
            while let Ok(event) = event_rx.try_recv() {
                match event {
                    ReaderEvent::Pong {
                        replica_id,
                        node_mode,
                    } => {
                        println!(
                            "Received PONG from {} (replica_id={}, mode={:?})",
                            self.addr, replica_id, node_mode
                        );
                        cluster.lock().unwrap().record_success(&self.addr, node_mode);
                        pong_received = true;
                    }
                    ReaderEvent::InvalidFrame(frame) => {
                        eprintln!("Invalid frame from {}: {}", self.addr, frame);
                        cluster
                            .lock()
                            .unwrap()
                            .record_failure(&self.addr, self.max_missed_pings);
                    }
                    ReaderEvent::ConnectionLost => {
                        eprintln!("Connection lost to {}", self.addr);
                        return DrainResult::ConnectionLost;
                    }
                }
            }
        }

        if pong_received {
            DrainResult::PongReceived
        } else {
            DrainResult::NoPong
        }
    }

    /// Main outbound loop.
    ///
    /// Delegates each distinct concern to a helper method:
    /// 1. [`try_connect`] — ensure a live stream exists.
    /// 2. [`send_ping`] — write a PING frame.
    /// 3. Sleep for [`PING_INTERVAL_SECS`].
    /// 4. [`drain_events`] — classify what arrived during the sleep.
    ///
    /// Exits (returns) only when `try_connect` gives up, marking the node
    /// [`NodeStatus::Disconnected`] before returning.
    ///
    /// [`try_connect`]: OutboundNode::try_connect
    /// [`send_ping`]: OutboundNode::send_ping
    /// [`drain_events`]: OutboundNode::drain_events
    pub fn run(mut self, cluster: Arc<Mutex<Cluster>>) {
        loop {
            // ── Ensure we have a live connection ────────────────────
            if self.stream.is_none() {
                match self.try_connect() {
                    ConnectionResult::Connected => {}
                    ConnectionResult::RetryLater => {
                        sleep(Duration::from_secs(CONNECT_RETRY_WAIT_SECS));
                        continue;
                    }
                    ConnectionResult::GaveUp => {
                        cluster
                            .lock()
                            .unwrap()
                            .set_node_status(&self.addr, NodeStatus::Disconnected);
                        return;
                    }
                }
            }

            // ── Send PING ──────────────────────────────────────────
            if let Err(e) = self.send_ping() {
                eprintln!("Failed to send PING to {}: {:?}", self.addr, e);
                self.reset_connection();
                cluster
                    .lock()
                    .unwrap()
                    .record_failure(&self.addr, self.max_missed_pings);
                sleep(Duration::from_secs(CONNECT_RETRY_WAIT_SECS));
                continue;
            }

            // ── Wait for the ping interval ─────────────────────────
            sleep(Duration::from_secs(PING_INTERVAL_SECS));

            // ── Drain reader events ────────────────────────────────
            match self.drain_events(&cluster) {
                DrainResult::ConnectionLost => {
                    self.reset_connection();
                    cluster
                        .lock()
                        .unwrap()
                        .record_failure(&self.addr, self.max_missed_pings);
                    sleep(Duration::from_secs(CONNECT_RETRY_WAIT_SECS));
                    continue;
                }
                DrainResult::NoPong => {
                    cluster
                        .lock()
                        .unwrap()
                        .record_failure(&self.addr, self.max_missed_pings);
                }
                DrainResult::PongReceived => {}
            }

            println!("Cluster Info: {:?}", cluster.lock().unwrap().info());
        }
    }

    /// Reader thread: blocks on [`read_frame`] and forwards events to the
    /// writer thread over a flume channel. Exits when the connection drops.
    fn run_reader(mut stream: TcpStream, tx: flume::Sender<ReaderEvent>) {
        loop {
            match read_frame(&mut stream) {
                Ok(frame) => match PongReplicationMessage::parse(&frame) {
                    Ok((replica_id, node_mode)) => tx
                        .send(ReaderEvent::Pong {
                            replica_id,
                            node_mode,
                        })
                        .unwrap(),
                    Err(_) => tx.send(ReaderEvent::InvalidFrame(frame)).unwrap(),
                },
                Err(_) => tx.send(ReaderEvent::ConnectionLost).unwrap(),
            }
        }
    }
}
