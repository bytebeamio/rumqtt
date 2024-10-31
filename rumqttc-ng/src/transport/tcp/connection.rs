use base::{pipe, EventsTx, XchgPipeA, XchgPipeB};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    select,
    time::timeout,
};

use crate::transport::{ConnectionSettings, TransportEvent};

pub struct Connection {
    id: usize,
    pub settings: ConnectionSettings,
    connection_to_io: XchgPipeA<u8>,
    io_to_connection: XchgPipeB<u8>,
    events_tx: EventsTx<TransportEvent>,
}

impl Connection {
    pub fn new(
        id: usize,
        settings: ConnectionSettings,
        events_tx: EventsTx<TransportEvent>,
    ) -> (Self, XchgPipeA<u8>, XchgPipeB<u8>) {
        // initialize pipes for connection_to_io:
        //  keep the A in Self, return B
        let (conn_to_io_tx, conn_to_io_rx) =
            pipe(id, settings.max_read_buf_size, settings.max_write_buf_size);

        // initialize pipes for io_to_connection:
        //  keep the B in Self, return A
        let (io_to_conn_tx, io_to_conn_rx) =
            pipe(id, settings.max_read_buf_size, settings.max_write_buf_size);

        let connection = Connection {
            settings,
            connection_to_io: conn_to_io_tx,
            io_to_connection: io_to_conn_rx,
            events_tx,
            id,
        };

        (connection, io_to_conn_tx, conn_to_io_rx)
    }

    pub async fn start<T>(&mut self, stream: &mut T) -> Result<(), std::io::Error>
    where
        T: AsyncReadExt + AsyncWriteExt + Unpin,
    {
        // we should get some data from stream within timeout!
        let _read = timeout(
            Duration::from_secs(self.settings.timeout),
            read_from_stream(&mut self.connection_to_io.read, stream),
        )
        .await??;

        // this will start the timer for session
        self.events_tx.send_async(TransportEvent::Reconnection(0)).await;

        if self.connection_to_io.try_forward() {
            self.events_tx.send_async(TransportEvent::IncomingData).await;
        }

        loop {
            // TODO(swanx): we shall improve these names!!
            let readable = self.connection_to_io.readable() > 0;

            select! {
                v = read_from_stream(&mut self.connection_to_io.read, stream), if readable => {
                    // Read from network and fill read buffer
                    let _n = v?;
                    // dbg!(_n);
                    if self.connection_to_io.try_forward() {
                        self.events_tx.send_async(TransportEvent::IncomingData).await;
                    }
                }
                _ = self.connection_to_io.incoming_recycler.wait() => {
                    // dbg!("received back incoming_tx buffer");
                    stream.write_all(self.connection_to_io.incoming_recycler.next.as_ref().unwrap()).await?;
                    self.connection_to_io.incoming_recycler.clear();
                    // try to send to active buffer to other end
                    if self.connection_to_io.try_forward() {
                        self.events_tx.send_async(TransportEvent::IncomingData).await;
                    }
                }
                data = self.io_to_connection.incoming.recv_async() => {
                    let mut data = data.expect("use ? here");
                    stream.write_all(&data).await?;
                    data.clear();
                    self.io_to_connection.ack(data);
                    self.events_tx.send_async(TransportEvent::OutgoingAck).await;
                }
            }
        }
    }

    pub fn cleanup(&mut self) {
        self.events_tx.send(TransportEvent::ConnectionClosed);
        // self.control_rx.drain();
        // TODO: cleanup all the pipes
        self.connection_to_io.incoming_recycler.recv();
        self.connection_to_io.incoming_recycler.clear();
        self.connection_to_io.read.clear();
    }

    pub fn print_stats(&mut self) {
        println!(
            "id: {} => active buffer: {}, standby buf: {:?}, max: {}",
            self.id,
            self.connection_to_io.read.len(),
            self.connection_to_io
                .incoming_recycler
                .next_buf()
                .map(|b| b.len()),
            self.connection_to_io.max_read_buf_size
        )
    }
}

async fn read_from_stream<T>(read: &mut Vec<u8>, stream: &mut T) -> Result<usize, std::io::Error>
where
    T: AsyncReadExt + Unpin,
{
    match stream.read_buf(read).await {
        Ok(0) => Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted,
            "connection closed by peer",
        )),
        Ok(n) => Ok(n),
        Err(e) => Err(e),
    }
}
