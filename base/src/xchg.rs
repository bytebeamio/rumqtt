use flume::{Receiver, Sender};
use std::mem;

pub trait Size {
    fn size(&self) -> usize;
}

impl Size for u8 {
    fn size(&self) -> usize {
        1
    }
}

impl Size for () {
    fn size(&self) -> usize {
        0
    }
}

#[derive(Debug)]
pub struct XchgPipeA<T>
where
    T: Size,
{
    pub id: usize,
    pub max_read_buf_size: usize,
    pub read: Vec<T>,
    pub incoming: Sender<Vec<T>>,
    pub incoming_recycler: Recycler<T>,
}

impl<T> XchgPipeA<T>
where
    T: Size,
{
    /// Swaps ready buffer with recycled buffer and forwards it. Ready buffer
    /// is empty after this operation
    pub fn try_forward(&mut self) -> bool {
        // If current read buffer is empty, there is nothing to forward
        if self.read.is_empty() {
            return false;
        }

        if let Some(recycled) = self.incoming_recycler.next.take() {
            // debug_assert!(recycled.is_empty());
            let ready = mem::replace(&mut self.read, recycled);
            self.incoming.try_send(ready).unwrap();
            return true;
        }

        false
    }

    pub fn standby(&mut self) -> Option<&mut Vec<T>> {
        self.incoming_recycler.next_buf()
    }

    /// Forwards empty buffers to the other side.
    pub fn force_forward(&mut self) -> bool {
        if let Some(recycled) = self.incoming_recycler.next.take() {
            let ready = mem::replace(&mut self.read, recycled);
            self.incoming.try_send(ready).unwrap();
            return true;
        }

        false
    }

    pub fn size(&self) -> usize {
        self.read.iter().map(|item| item.size()).sum()
    }

    pub fn readable(&self) -> usize {
        let current_size = self.size();
        if self.max_read_buf_size < current_size {
            return 0;
        }

        self.max_read_buf_size - current_size
    }

    pub async fn flush(&mut self) -> bool {
        if self.incoming_recycler.next.is_none() {
            self.incoming_recycler.wait().await;
            self.incoming_recycler.clear();
        }

        self.try_forward()
    }

    pub async fn shutdown(&mut self) -> Option<&mut Vec<T>> {
        if self.incoming_recycler.next.is_none() {
            self.incoming_recycler.wait().await;
        }

        self.incoming_recycler.next_buf()
    }
}

pub struct XchgPipeB<T>
where
    T: Size,
{
    pub max_write_buf_size: usize,
    pub incoming: Receiver<Vec<T>>,
    pub incoming_recycle: Sender<Vec<T>>,
}

impl<T> XchgPipeB<T>
where
    T: Size,
{
    pub fn recv(&self) -> Result<Vec<T>, Error> {
        let v = self.incoming.recv()?;
        Ok(v)
    }

    pub fn try_recv(&self) -> Result<Vec<T>, Error> {
        let v = self.incoming.try_recv()?;
        Ok(v)
    }

    pub fn ack(&self, buf: Vec<T>) {
        self.incoming_recycle.try_send(buf).ok().unwrap();
    }
}

#[derive(Debug)]
pub struct Recycler<T> {
    pub next: Option<Vec<T>>,
    pub rx: Receiver<Vec<T>>,
}

impl<T> Recycler<T> {
    /// Wait for notification and buffer to be recycled
    pub async fn wait(&mut self) {
        let v = self.rx.recv_async().await.unwrap();
        self.next = Some(v);
    }

    /// Receive buffer. Notification would have been sent in global events queue
    pub fn recv(&mut self) {
        if self.next.is_none() {
            let v = self.rx.recv().unwrap();
            self.next = Some(v);
        }
    }

    /// Receive buffer. Notification would have been sent in global events queue
    pub fn try_recv(&mut self) {
        let Some(v) = self.rx.try_recv().ok() else {
            return;
        };
        self.next = Some(v);
    }

    pub fn next_buf(&mut self) -> Option<&mut Vec<T>> {
        self.next.as_mut()
    }

    pub fn clear(&mut self) {
        if let Some(ref mut buf) = self.next {
            buf.clear();
        }
    }
}

pub fn pipe<T: Size>(
    id: usize,
    read_buf_size: usize,
    write_buf_size: usize,
) -> (XchgPipeA<T>, XchgPipeB<T>) {
    let (incoming_tx, incoming_rx) = flume::bounded(1);
    let (incoming_recycle_tx, incoming_recycle_rx) = flume::bounded(1);

    // Internal tx is used to send data to remote while
    // rx is use to recv previous buffer from remote to
    // maintain memory boundedness with buffer recycling
    let a = XchgPipeA {
        id,
        max_read_buf_size: read_buf_size,
        read: Vec::with_capacity(read_buf_size),
        incoming: incoming_tx,
        incoming_recycler: Recycler {
            next: Some(Vec::with_capacity(read_buf_size)),
            rx: incoming_recycle_rx,
        },
    };

    // Internal rx is used in remote to receive data from remote
    // while internal tx is used to send acks and to recycle buffer
    let b = XchgPipeB {
        max_write_buf_size: write_buf_size,
        incoming: incoming_rx,
        incoming_recycle: incoming_recycle_tx,
    };

    (a, b)
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("TryRecv")]
    TryRecv(#[from] flume::TryRecvError),
    #[error("Recv")]
    Recv(#[from] flume::RecvError),
}
