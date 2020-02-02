Analyzing with `perf`
-----------

Build in release mode with debug symbols (-g) and frame pointers enabled

```
RUSTFLAGS="-g -C force-frame-pointers=y" cargo build --release
perf record -g <binary>
perf report -g graph,0.5,caller
```

Flamegraph
----------

```
RUSTFLAGS="-g -C force-frame-pointers=y" cargo flamegraph rumq-cli 
```

References
-----------

* https://github.com/flamegraph-rs/flamegraph
* https://gendignoux.com/blog/2019/11/09/profiling-rust-docker-perf.html
