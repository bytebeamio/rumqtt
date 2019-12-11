# rumq 
[![img](https://github.com/tekjar/rumq/workflows/CI/badge.svg)](https://github.com/tekjar/rumq/actions)
[![img](https://img.shields.io/discord/633193308033646605?style=flat-square)](https://discord.gg/mpkSqDg)

MQTT ecosystem in rust which strives to be simple, robust and performant

* Fully asynchronous. Built on top of tokio 0.2 and futures 0.3
* Mqtt eventloop is just a stream. Easily plug it into async ecosystem
* Takes a `Stream` for user requests. Solves both bounded and unbounded
  usecases
* Automatic reconnections
* Request throttling
* Inflight queue size based throttling
* Tls using RustTLS. Cross compilation and multi platform support is painless


#### REFERENCES
----------------
* http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
