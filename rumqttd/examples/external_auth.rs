use rumqttd::{Broker, Config};

fn main() {
    let builder = tracing_subscriber::fmt()
        .pretty()
        .with_line_number(false)
        .with_file(false)
        .with_thread_ids(false)
        .with_thread_names(false);

    builder
        .try_init()
        .expect("initialized subscriber succesfully");

    let config = config::Config::builder()
        .add_source(config::File::with_name("rumqttd.toml"))
        .build()
        .unwrap();
    let mut config: Config = config.try_deserialize().unwrap();

    // for e.g. if you want it for [v4.1] server, you can do something like
    let server = config.v4.as_mut().and_then(|v4| v4.get_mut("1")).unwrap();

    // external_auth function / closure signature must be:
    // async fn(ClientId, AuthUser, AuthPass) -> bool
    // type for ClientId, AuthUser and AuthPass is String
    server.set_auth_handler(auth);

    // or you can pass closure
    // server.set_auth_handler(|_client_id, _username, _password| async {
    //     // perform auth
    //     true
    // });

    let mut broker = Broker::new(config);

    broker.start().unwrap();
}

async fn auth(_client_id: String, _username: String, _password: String) -> bool {
    // users can fetch data from DB or tokens and use them!
    // do the verification and return true if verified, else false
    true
}
