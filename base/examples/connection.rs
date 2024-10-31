use std::{io, sync::Arc, thread, time::Duration};

use humansize::{format_size, BINARY};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    select,
    task::JoinSet,
    time::{self, MissedTickBehavior},
};

use base::*;
use tracing::trace;

use tracing::*;

#[derive(clap::Parser)]
#[command(version)]
#[command(about = "")]
#[command(author = "tekjar <raviteja@bytebeam.io>")]
struct CommandLine {
    /// log level (v: info, vv: debug, vvv: trace)
    #[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count)]
    verbose: u8,
}

fn init() -> CommandLine {
    let commandline: CommandLine = CommandLine::parse();
    use clap::Parser;
    let level = match commandline.verbose {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };

    // tracing syntax ->
    let builder = tracing_subscriber::fmt()
        .pretty()
        .with_line_number(false)
        .with_file(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_env_filter(level)
        .with_filter_reloading();

    // let reload_handle = builder.reload_handle();

    builder
        .try_init()
        .expect("initialized subscriber succesfully");

    commandline
}

fn main() {
    let _cli = init();

    error!("error..");
    info!("info..");
    debug!("debug..");

    let mut events = EventsRx::new(100);

    // remote clients which generate data
    let mut client_a_list = Vec::with_capacity(100);

    // local clients which receive data from remote clients
    let mut client_b_list = Vec::with_capacity(100);
    let mut connections = Vec::with_capacity(100);

    // xchgpipebs to receive data from several cnnections
    let mut xchgpipe_b_list = Vec::with_capacity(100);

    for id in 0..100 {
        let (a, b) = tokio::io::duplex(64);
        client_a_list.push(a);
        client_b_list.push(b);

        let (connection, rx) = Connection::new(
            id,
            Arc::new(ConnectionSettings::default()),
            events.producer(id),
        );
        connections.push(connection);
        xchgpipe_b_list.push(rx);
    }

    // Thread with spawns tasks to read data from n remote connections
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(async {
            let mut set = JoinSet::new();
            // Read data from network(sort of) and forward to the other end (xchgpipeb)
            for (mut client_b, mut conn) in client_b_list.into_iter().zip(connections.into_iter()) {
                set.spawn(async move {
                    conn.start(&mut client_b).await.unwrap();
                });
            }

            loop {
                let v = set.join_next().await.unwrap();
                println!("v = {:?}", v);
            }
        });
    });

    // Thread with spawns n tasks to simulate remote connections
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(async {
            let mut set = JoinSet::new();
            for mut client_a in client_a_list.into_iter() {
                set.spawn(async move {
                    loop {
                        client_a.write_all(b"ping").await.unwrap();
                        time::sleep(Duration::from_secs(1)).await;
                    }
                });
            }

            loop {
                let v = set.join_next().await.unwrap();
                println!("v = {:?}", v);
            }
        });
    });

    loop {
        let (id, event) = events.poll().unwrap();
        info!("id = {id}, event = {event:?}");

        match event {
            Event::NewConnection => {}
            Event::NewConnectionData => {
                let rx = &mut xchgpipe_b_list[id];
                let mut data = rx.recv().unwrap();

                info!("data = {:?}", data);

                data.clear();
                rx.ack(data);
            }
            Event::ConnectionClosed => {}
        }
    }
}

#[derive(Debug)]
pub struct Connection {
    settings: Arc<ConnectionSettings>,
    incoming_tx: XchgPipeA<u8>,
    events_tx: EventsTx<Event>,
    total_recv_size: usize,
    total_sent_size: usize,
    total_recv_count: usize,
    total_sent_count: usize,
}

#[derive(Debug)]
pub enum Event {
    NewConnection,
    NewConnectionData,
    ConnectionClosed,
}

impl Connection {
    pub fn new(
        id: usize,
        settings: Arc<ConnectionSettings>,
        events_tx: EventsTx<Event>,
    ) -> (Connection, XchgPipeB<u8>) {
        // These are the buffers that use the pipes to perform the handshake between itself and the
        // hubs, to ensure memory-boundedness

        let (incoming_tx, incoming_rx) =
            pipe(id, settings.max_read_buf_size, settings.max_write_buf_size);

        (
            Connection {
                settings,
                incoming_tx,
                events_tx,
                total_recv_size: 0,
                total_sent_size: 0,
                total_recv_count: 0,
                total_sent_count: 0,
            },
            incoming_rx,
        )
    }

    /// NOTE: Footgun. Don't use write_all_buf. It affects BytesMut capacity.
    pub async fn start<T>(&mut self, stream: &mut T) -> Result<bool, Error>
    where
        T: AsyncReadExt + AsyncWriteExt + Unpin,
    {
        let max_read_buf_size = self.settings.max_read_buf_size;
        let mut interval = tokio::time::interval(Duration::from_millis(10));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        self.events_tx.send_async(Event::NewConnection).await;

        loop {
            let readable = self.incoming_tx.readable() > 0;

            trace!(
                "
                readable = {readable},
                read buf: {}/{}/{},
                total recv size = {},
                total sent size = {},
                total recv count = {},
                total sent count = {}",
                format_size(self.incoming_tx.read.len(), BINARY),
                format_size(self.incoming_tx.read.capacity(), BINARY),
                format_size(max_read_buf_size, BINARY),
                format_size(self.total_recv_size, BINARY),
                format_size(self.total_sent_size, BINARY),
                self.total_recv_count,
                self.total_sent_count,
            );

            select! {
                v = pull(&mut self.incoming_tx.read, stream), if readable => {
                    // Read from network and fill read buffer
                    let n = v?;
                    self.total_recv_count += 1;
                    self.total_recv_size += n;
                }
                _ = self.incoming_tx.incoming_recycler.wait() => {
                    trace!("received back incoming_tx buffer");
                }
                _ = interval.tick() => {
                    let n = self.incoming_tx.read.len();
                    trace!("n/w -> read_buf: incoming = {}", format_size(n, BINARY));
                    if self.incoming_tx.try_forward() {
                        self.events_tx.send_async(Event::NewConnectionData).await;
                    }
                }
            }
        }
    }

    pub async fn flush(&mut self) {
        self.incoming_tx.flush().await;
        self.incoming_tx.shutdown().await;
    }

    pub async fn nack(&mut self, stream: &mut TcpStream) -> Result<(), Error> {
        let acks = self.incoming_tx.shutdown().await;
        if let Some(acks) = acks {
            stream.write_all(acks).await?;
            self.total_sent_size += acks.len();
        }

        Ok(())
    }
}

async fn pull<T>(read: &mut Vec<u8>, stream: &mut T) -> Result<usize, io::Error>
where
    T: AsyncReadExt + Unpin,
{
    use std::io::{Error, ErrorKind};

    match stream.read_buf(read).await {
        Ok(0) => Err(Error::new(
            ErrorKind::ConnectionAborted,
            "connection closed by peer",
        )),
        Ok(n) => Ok(n),
        // TODO: add comment here with more info on why this was added
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
            panic!("cannot reach");
            // Ok(0)
        }
        Err(e) => Err(e),
    }
}

// TODO see if we can use an async fn push() here to write things back to the stream

// async fn push(write: &mut Vec<u8>, stream: &mut TcpStream) -> Result<usize, io::Error> {
//     let v = stream.write_all_buf(write).await?;
//     Ok(write.len())
// }

#[derive(Debug, thiserror::Error)]
#[error("Acceptor error")]
pub enum Error {
    #[error("I/O {0}")]
    Io(#[from] io::Error),
    #[error("Timeout")]
    Timeout(#[from] time::error::Elapsed),
    #[error("Flume recv error")]
    FlumeRecv(#[from] flume::RecvError),
}

#[derive(Debug, Clone)]
pub struct ConnectionSettings {
    pub connection_timeout_ms: u16,
    pub max_read_buf_size: usize,
    pub max_write_buf_size: usize,
    pub max_packet_size: usize,
    pub throttle_delay_ms: u64,
}

impl Default for ConnectionSettings {
    fn default() -> Self {
        Self {
            connection_timeout_ms: 5000,
            max_read_buf_size: 2048,
            max_write_buf_size: 2048,
            max_packet_size: 1024,
            throttle_delay_ms: 0,
        }
    }
}
