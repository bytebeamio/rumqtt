use std::time::Duration;

use base::EventsRx;

#[derive(Debug)]
pub enum Event {
    NewConnection,
    ConnectionNewData,
    ConnectionTimeout,
}

fn main() {
    let mut event_bus = EventsRx::new(10);

    // Spawn a new connection thread and send events
    let tx = event_bus.producer(0);
    std::thread::spawn(move || {
        let event = Event::NewConnection;
        tx.send(event);

        for _ in 0..10 {
            let event = Event::ConnectionNewData;
            std::thread::sleep(Duration::from_secs(1));
            tx.send(event);
        }
    });

    // Spawn a new connection thread and send events
    let tx = event_bus.producer(1);
    std::thread::spawn(move || {
        let event = Event::NewConnection;
        tx.send(event);

        for _ in 0..10 {
            let event = Event::ConnectionNewData;
            std::thread::sleep(Duration::from_secs(1));
            tx.send(event);
        }
    });

    let mut keys = [Option::None; 2];

    let mut remove_timer = true;

    // Poll the event bus for an event
    for i in 0..100 {
        let event = event_bus.poll().unwrap();
        match event {
            (id, Event::NewConnection) => {
                println!("{i}. Event: ClientNewConnection({id})");

                // Timeout after 5 seconds
                let duration = Duration::from_secs(5);
                let key = event_bus.add_timer(id, Event::ConnectionTimeout, duration);
                keys[id] = Some(key);
            }
            (id, Event::ConnectionNewData) => {
                // Remove id 1's timer after 2nd iteration. You shouldn't see timeout print on connection 1
                if id == 1 && remove_timer && i >= 2 {
                    event_bus.remove_timer(keys[id].unwrap());
                    remove_timer = false;
                }

                println!("{i}. Event: ClientNewData({id})");
            }
            (id, Event::ConnectionTimeout) => {
                println!("{i}. Event: ConnectionTimeout({id})");
            }
        }
    }
}
