use std::{thread, time::Duration};

use base::EventsRx;
use clap::Parser;

#[derive(clap::Parser)]
#[command(about = "")]
#[command(author = "tekjar <raviteja@bytebeam.io>")]
struct CommandLine {
    /// log level (v: info, vv: debug, vvv: trace)
    #[arg(short = 'v', action = clap::ArgAction::Count)]
    core_logs: u8,
    /// log level (v: info, vv: debug, vvv: trace)
    #[arg(short = 'V', action = clap::ArgAction::Count)]
    full_logs: u8,
    /// number of clients to simulate
    #[arg(short = 'n', default_value = "100")]
    max_clients: usize,
}

fn init() -> usize {
    let commandline: CommandLine = CommandLine::parse();

    let mut level = match commandline.core_logs {
        0 => "info,libghz=warn,transport=warn,xchg=warn,io=info",
        1 => "info,libghz=info,transport=info,xchg=info,io=info",
        2 => "info,libghz=debug,transport=debug,xchg=debug,io=info",
        _ => "info,libghz=trace,transport=trace,xchg=trace,io=info",
    };

    if commandline.full_logs > 0 {
        level = match commandline.full_logs {
            1 => "info",
            2 => "debug",
            _ => "trace",
        };
    }

    // tracing syntax ->
    let builder = tracing_subscriber::fmt()
        .pretty()
        .without_time()
        .with_line_number(false)
        .with_file(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_env_filter(level)
        .with_filter_reloading();

    builder
        .try_init()
        .expect("initialized subscriber succesfully");

    // console_subscriber::init();

    commandline.max_clients
}

enum Event {
    ClientNewData,
    BrokerMetrics,
}

fn main() {
    let max_clients = init();

    let mut event_bus = EventsRx::new(1000);
    let mut event_txs = Vec::with_capacity(max_clients);
    for i in 0..max_clients {
        let tx = event_bus.producer(i);
        event_txs.push(tx);
    }

    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(5)
            .build()
            .unwrap();

        runtime.block_on(async move {
            let mut handles = Vec::with_capacity(max_clients);
            for tx in event_txs.into_iter() {
                let handle = tokio::spawn(async move {
                    loop {
                        let event = Event::ClientNewData;
                        tx.send_async(event).await;
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        // tokio::time::sleep(Duration::from_nanos(25)).await;
                    }
                });

                handles.push(handle);
            }
            for handle in handles {
                handle.await.unwrap();
            }
        });
    });

    event_bus.add_timer(1, Event::BrokerMetrics, Duration::from_secs(5));
    loop {
        let event = event_bus.poll().unwrap();
        // Simulate avg time it takes to process an event

        thread::sleep(Duration::from_secs_f64(0.0001));
        if let (id, Event::BrokerMetrics) = event {
            event_bus.add_timer(id, Event::BrokerMetrics, Duration::from_secs(5));
            println!("Timer event got triggered");
        };
    }
}
