use rumqttc::*;
use std::error::Error;
use tokio::time;

fn main() -> Result<(), Box<dyn Error>> {
    pretty_env_logger::init();

    'outer: loop {
        let mut mqttoptions = MqttOptions::new("parking_sensor", "localhost", 1883);
        mqttoptions.set_keep_alive(std::time::Duration::from_secs(60));

        let (mut client, mut connection) = Client::new(mqttoptions, 10);
        client.subscribe("hello/+/world", QoS::AtLeastOnce)?;

        use rumqttc::Event::*;
        use rumqttc::Packet::*;
        let mut last_seen_data = time::Instant::now();

        loop {
            if last_seen_data.elapsed() > time::Duration::from_secs(15) {
                eprint!("Haven't seen new data for > 15seconds. Reconnecting.");
                continue 'outer;
            }

            let notification = match connection.recv_timeout(std::time::Duration::from_millis(50)) {
                Ok(v) => v,
                Err(RecvTimeoutError::Disconnected) => {
                    eprintln!("client disconnected. Reconnecting.");
                    continue 'outer;
                }
                Err(RecvTimeoutError::Timeout) => continue,
            };

            let n = match notification {
                Ok(n) => n,
                Err(e) => {
                    eprintln!("MQTT error. Will reconnect: {e}.");
                    break;
                }
            };

            match n {
                Incoming(Publish(data)) => {
                    last_seen_data = time::Instant::now();
                    println!("{:?}", data);
                }
                Incoming(_) | Outgoing(_) => continue,
            };
        }
    }
}
