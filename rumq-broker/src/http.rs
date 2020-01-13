use futures_util::future::join;
use futures_util::future;
use hyper::service::{make_service_fn, service_fn, Service};
use hyper::{StatusCode, Body, Client, Request, Response, Server};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use std::future::Future;
use std::pin::Pin;
use std::task::{Poll, Context};
 
use crate::router::RouterMessage;
pub struct Backend {
    router_rx: Receiver<RouterMessage>
}
pub struct MakeSvc;

impl<T> Service<T> for MakeSvc {
    type Response = ();
    type Error = std::io::Error;
    type Future = future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, _: T) -> Self::Future {
        future::ok(())
    }
}

impl Backend {
    pub fn start(router_tx: Sender<RouterMessage>) {
        let (this_tx, this_rx) = channel(100);
        let backend = Backend { router_rx: this_rx };
        let addr = ([127, 0, 0, 1], 8080).into();
        let server = Server::bind(&addr).serve(MakeSvc);
    }
}

impl Service<Request<Vec<u8>>> for Backend {
    type Response = Response<Vec<u8>>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Vec<u8>>) -> Self::Future {
        // create the body
        let body: Vec<u8> = "hello, world!\n".as_bytes().to_owned();
        // Create the HTTP response
        let resp = Response::builder().status(StatusCode::OK).body(body).expect("Unable to create `http::Response`");
         
        // create a response in a future.
        let fut = async {
            Ok(resp)
        };

        // Return the response as an immediate future
        Box::pin(fut)
    }
}
