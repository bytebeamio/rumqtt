use tokio::task;

use librumqd::accept_loop;

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    let fut = accept_loop("0.0.0.0:1883");
    let o = task::spawn(fut).await;

    println!("{:?}", o);
}
