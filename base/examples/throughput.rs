use std::{io::Read, thread};

use base::*;
use humansize::{format_size, BINARY};

fn main() {
    let total_size = 10 * 1024 * 1024 * 1024;
    let write_buffer_size = 10 * 1024 * 1024;
    let read_buffer_size = 10 * 1024 * 1024;
    let incoming_chunk_size = 1024 * 1024;

    let (mut pipe_tx, pipe_rx) = pipe(1, read_buffer_size, write_buffer_size);
    let start_time = std::time::Instant::now();

    thread::spawn(move || {
        send_data(&mut pipe_tx, total_size, incoming_chunk_size);
    });

    let mut current_size = 0;
    loop {
        let mut v = pipe_rx.recv().unwrap();
        current_size += v.len();
        v.clear();

        pipe_rx.ack(v);
        if current_size >= total_size {
            break;
        }
    }

    let elapsed = start_time.elapsed();
    let throughput = (total_size as f64 / elapsed.as_secs_f64()) as u64;

    println!(
        "Time: {:?}. Throughput: {} bytes/sec",
        elapsed,
        format_size(throughput, BINARY)
    );
}

#[tokio::main(flavor = "current_thread")]
async fn send_data(tx: &mut XchgPipeA<u8>, data_size: usize, chunk_size: usize) {
    let data = vec![0; data_size];
    let mut cursor = std::io::Cursor::new(&data);

    loop {
        if tx.readable() == 0 {
            tx.incoming_recycler.wait().await;
            tx.try_forward();
        }

        let readable = tx.readable();
        let readable = std::cmp::min(readable, chunk_size);
        let mut temp_buf = vec![0u8; readable];
        let n = cursor.read(&mut temp_buf).unwrap();

        tx.read.extend_from_slice(&temp_buf[..n]);
        tx.try_forward();

        tx.incoming_recycler.try_recv();
        tx.try_forward();

        if n == 0 {
            break;
        }
    }
}
