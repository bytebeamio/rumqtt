
Measuring throughput with benchmarking tool
---------

* start the broker
```
RUST_LOG=rumq_broker=info cargo run --release
```

* run the tool with the configuration you like
```
./benchmark --help
./benchmark -c 10 -m 10000 (open 10 connection with 10000 publishes each)
```

* results look like this
```
tekjar@thinkpad:~/Workspace/rumq/utils/benchmark$ ./benchmark -c 10 -m 50000
  99% |||||||||||||||||||||||||||||||||||||||.  [29s:0s] 

Id = bench-0 Total Messages = 50000 Average throughput = 1.6353620750123208 MB/s
Id = bench-1 Total Messages = 50001 Average throughput = 1.6310719156488749 MB/s
...

 Total Messages =  500005 Average throughput =  16.03808488093616 MB/s
```

Findings
---------

* There doesn't seem to be any improvement wrt cpu and throughput using
  `BufStream`

```
./benchmark -c 10 -m 10000 (open 10 connection with 10000 publishes each)

```

* Using codec on top of `Read` has a huge perf benefit over async
  MqttRead/Write

There is 8X improvement in throughput

```rust
// https://docs.rs/tokio/0.2.6/src/tokio/io/util/buf_reader.rs.html#117
// TODO What happens when socket receives only 4K of data and there is no new data?
// TODO Also might have to periodically flush to send mqtt packets which are part of
// partially filled write bufers 
let mut stream = BufStream::new(&mut self.stream);
```

Moooree tools
----------

#####Flamegraph

```
RUSTFLAGS="-g -C force-frame-pointers=y" cargo flamegraph rumq-cli 
```

#####Perf

Build in release mode with debug symbols (-g) and frame pointers enabled

```
RUSTFLAGS="-g -C force-frame-pointers=y" cargo build --release
perf record -g <binary>
perf report -g graph,0.5,caller
```

References
-----------

* https://github.com/flamegraph-rs/flamegraph
* https://gendignoux.com/blog/2019/11/09/profiling-rust-docker-perf.html

