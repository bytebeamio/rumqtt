
Broker constitutes of following components
----------------

* N connections. 1 per client 
* Router which holds all subscription information and handles to
  communicate with all the clients. This might not be super efficient as
  data travels from connection 1 -> router -> connection 2. But might
  not be significant if prevent clones (just send fat pointers) and batch
* Same router which reads device state from all the connections and actions
  from the backend
* Timestone which reads all the data from all the connections and writes
  to disk and tries to simulate kafka


Device id as part of topic in subscriptions and publishes
-----------------

* Maybe useful to directly send publishes to backend (where router picks up device id directly)
* Subscriptions only receive data directed to it
* Get in the way of wildcards inplace of device ids

TODO
---------------

* Use hash of connection id strings to numbers and validate perf 
* Make sure that whole router doesn't slowdown because of one slow connection
* Inflight message limits. This should be similar to client. Slow acks should 
  throttle down the connection eventloop channel receiver. This
  pushes the backpressure to the router. But this should not slow down 
  other connections. Essentially everything should be concurrent in a 
  select! in the router
* Add throughput metrics to every connection. Logging based
  instrumentation will be very handy here but we don't have a design on
  dashboards on top of log based metrics
* Production ready accept loop. https://github.com/async-rs/async-std/pull/666/files
* Better errors when tls connection happens on tcp port
```
Client side error:
Received = StreamEnd(Network(Io(Custom { kind: UnexpectedEof, error: "tls handshake eof" })))

Server side error:
ERROR librumqd::connection > Connect packet error = Timeout(Elapsed(()))
```

