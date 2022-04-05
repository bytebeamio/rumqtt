use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

#[tokio::main]
async fn main() {
    pretty_env_logger::init();
    simplerouter::run(simplerouter::Config {
        addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1883)),
    })
    .await
    .unwrap();
}
