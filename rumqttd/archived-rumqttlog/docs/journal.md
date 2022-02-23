- Takes incoming data from several tcp sockets over channels
- 3 possible backings for these channels at the moment
    - Mqtt
    - Kafka
    - Grpc


02/April/2020
------------------

* Write only records in the segment for send file optimization in future **[design]**
* Using `BufReader` is not useful for commitlog in pubsub models as different connections might be reading at different positions
* To get better performance with the reader each consumer can have a buffer while reading to amortize systemcalls. Since subscriptions are fanout, this quickly blows up the memory usage 
* Another option is to have a special `BufReader` where a tail of (say) 100MB is in memory. Connections which are caught up won't do a system call **[experiment]**
* Removing `len` from segment improved throughput by 40 percent. probably due to decrease in system calls 

03/April/2020
-------------------

* Vectored reads can now read a bunch of packets into Vec<u8>
* Combined with zero copy semantics, we can either do a send file or write a Vec<u8> which contains multiple packets
* 1 system call to send a file and 2 system calls and 1 copy to user space to send a batch (1 disk read & 1 n/w write)

* Having a benchmark suite in a very early stage of the project help alot. Find the bug
    - vectored `segmentthroughput` is operating at 3500 MB/s
    - vectored `logthroughput` is operating at 800 MB/s

04/April/2020
-------------------

* Allocations are damn costly. 5 times less throughput in this case. Check the diff of this commit 9766a6b2c2fd9f34a6668b1dd30976c77d65d919
* We mmap the index but not the segment as the cost of page fault is even high
    - https://github.com/liftbridge-io/liftbridge/issues/178
    - mlock might help though https://www.reddit.com/r/rust/comments/ft98nz/tokio_reducing_tail_latencies_with_automatic/ 

05/April/2020
-------------------

* Send file optimization paper from netflix https://people.freebsd.org/~rrs/asiabsd_2015_tls.pdf
* https://jvns.ca/blog/2016/01/23/sendfile-a-new-to-me-system-call/ 
* Make use of `splice` to send data partially from file to socket **[experiment]**

09/April/2020
------------------

- Lazy segment creation eliminates empty new segments during read. Saves a system call while crossing boundaries
- We can stop relying on EOF to detect end of index by tracking index `size`. Saves more system call
- System calls are damn costly. Saving system calls is worth the effort

10/April/2020
------------------
- For index which isn't closed correctly, segment current & next offset construction is going to be wrong **[notes]**
- In the current design, segment file doesn't have any meta data to enable mqtt sendfile & splice optimizations
- Makes index reconstruction difficult. We might be able to reconstruct index using mqtt deserializer
- `FileExt` has os specific calls `read_exact_at` and `seek_read`. These will prevent seek system call **[experiment]**
- Instead of a `BufWriter`, we can maintain a Vec<Vec<u8>> in the segment and do a `write_vectored`? Prevents some allocations **[experiment]**

Issues before design change to fail initialization when unclosed index is encountered during reinitialization **[design]**
```$xslt
    - for chunks with index which isn't closed properly, next_offset will be wrong during reinit and hence append returns wrong offset
    - Index which isn't closed will reinit with wrong size and hence wrong count. This will lead to read crossing the actual index boundary and returning 0 offset and 0 len due to 0s in index
    - Reading to these 0 sized buffer will lead to `EOF` while doing `read_exact`
    - Unclosed index also means unclosed segment i.e not properly flushed. Index and segment are basically in an inconsistent state
    - Buffers allocated based on `len` returned by index read doesn't mean that related data exists in segment. This results in `EOF buffer not full` during `read_exact`
```
   
Considering unclosed index as corruption simplifies all the above code handling

11/April/2020
------------------

* Using `read_at` instead of seeking and reading resulted in a massive perf boost with single `read`s

```bash
Benchmarking read throughput/segments: Warming up for 3.0000 s
Warning: Unable to complete 10 samples in 5.0s. You may wish to increase target time to 39.4s.
read throughput/segments
                        time:   [710.58 ms 711.44 ms 712.80 ms]
                        thrpt:  [1.5064 Gelem/s 1.5093 Gelem/s 1.5111 Gelem/s]
                 change:
                        time:   [-23.875% -23.259% -22.793%] (p = 0.00 < 0.05)
                        thrpt:  [+29.521% +30.308% +31.363%]
                        Performance has improved.
```

- As expected this didn't translate to buffered `readv` as the decrease in number of system calls isn't significant.
- Adds weight to the design points above to take number of system calls seriously
            

12/April/2020
-------------------

- IoT devices are usually in the order of 1000s. We need to understand the overhead of managing 10K files
per server. We can combine multiple topics into a single file by partitioning a single file with base offset
boundaries by topic. But this is going to add the complexity of rebalancing when a device disconnects from
one broker and connects another **[research]**

- In kafka, clients are sophisticated. They hold the knowledge of every broker to send data to correct partition
leader. MQTT clients are simple. By making topic bind with device id, each device is essentially owned by a server.
Find if there is a way to intelligently load balance (what does load balancer do when a server goes down? it should
connect to correct new leader which got elected).

- Easy way above is to make the router have ability to act as a proxy. Directly making connection task a proxy is 
tricky
    - How to fan in n connections to 1 router connection? n connections on both the servers is redundancy

**[research]** the above sections more

13/April/2020
---------------------

- Every time the commitlog is read, we flush the writer to deal with inconsistency. Commitlogs can be read heavy and
this system call might affect the performance. Build a file abstraction which is backed by custom in memory buffer
just like `BufWriter`. The abstraction continues after EOF and we read from in memory buffer. This allows to not force
flush during read **[todo]**

18/April/2020
---------------------

- mqtt4bytes benchmarks shows that deserialization happens at a rate of 4GB/s (on thinkpad p1). A core should easily 
handle all the connections (along with context switch costs) **[experiment]**

- Since connections are stateless (hence easily distributable) and under utilized, move some procesing from router to
connections if possible. This takes some load of the stateful (currently single threaded) router

- Convert client/topic id strings to ids in the connection task. This prevents clones while connection & replicator are sending
requests to router using Sender<(ClientId, Request). This also reduces hashmap cost **[benchmark]** **[design]**

19/April/2020
---------------------
- Check if there is a to cache data between requests across connections. When all the connections are caught up
(which is very probable), different connections with same subscription can ask same data. Find a way to optimize this

- Current offsets of a subscription are help with in connection/replicator instead of router to keep the design inline
with our previous analysis to move computations away from router if possible

20/April/2020
---------------------

- Subscriptions are part of connection/replicator, including wild card subscriptions. When there is a publish on a new
topic, router sends the topic to all the the connections. Connections check if the topic is matching their subscrition list.
If there is a match, they start pulling data.

- Also limit number of topics broker allows per connection. Sending a new topic every message will fill up the memory and
 crash the broker. Also find other patterns like this **[security]**
 
21/April/2020
----------------------

- Counter point to enforcing client id in the publish topic: Having same topic increases utilization of a topic well and 
subscriber can pull in more data in a single system call.

- Counter point to moving some processing to connections: Sync code is going to have better cache efficiency than async code

- Supporting mulitplie QoS complicates implementation. We need to deduplicate wildcard and concrete topics with different
QoS. We'll start by just supporting QoS1.

- Multiple QoS also forces the data in commit log to be mutated during sends when publish QoS and subscribe QoS are
different. This makes commitlog bulk forwards very costly. We can probably make QoS subscriptions in memory? But what
happens to QoS 1 publishes and QoS 0 subscriptions?

22/April/2020
------------------------

- When ever there is a new publish, the topics iterator gets updated and starts again
from scratch. Too many new topics might affect the fairness of next_sweep

24/April/2020
------------------------

When fanning out data sometimes publish data like packet id needs to be added/modified. Is there a way to save packets
in chunks (Publish = Vec<Bytes>), modify necessary parts on demand and do a vectored write?

25/April/2020
------------------------

- Allocate to a single Vec<u8> and do 1 tcp write or use Vec<Vec<u8>> and do a vectored write
. What's a better abstraction? **[experiment]**

- Use string interning and integer keys for all the router hashmaps

26/April/2020
------------------------
There are multiple replication strategies
- Multiple messages in one buffer without wrapping in a new publish. Will lead to multiple acks for
the other router (which is inefficient). The router receiving individual acks doesn't have to do any
other handling
- Multiple message buffer wrapped in a publish. This will return one ack for all the packets. But the
payload has to be split into publishes for commitlog to create correct index. Is there a way to write 
a buffer of packets to commitlog with correct index?

We'll start with method 1 for ease of implementation

18/May/2020
-------------------------

- Multiple clients can publish data on same topic and router merges them. With replication in picture where
acks should only happen after acheiving a repication factor, router should have a way to tell connections
about acks. E.g Connection A sends 1, 2, 3 and Connection B sends 4, 5 on same topic. After replication, router
should should notify Connection A with 1, 2, 3 and Connection B with 4, 5.

- One way to achieve this is to separate topic by publishing client id as well. This adds more tracking in router and
replicator and gets in the way of topic & client separation 

- Option 2 is to have router maintain a global id for a given topic. This id is intrinsic to commitlog and appended a
a new record is added. Router replies connection with with id for every publish. Connection maintains a map of couter id 
and actual id. While pulling data, it knows the id till replication has happened and replys with actual acks

We'll go with opition 2

19/May/2020
-------------------------

- Replication and connection link are drastically different interms of functionality. For example,
replicator can write a batch of publishes as 1 message. Other replicator reading this a bunch of publishes 
is only going to unnecessarily flood the mesh with acks.

20/May/2020
-------------------------

- Write only publish payload to commitlog
- Full publish is written to commitlog with the hope that we can achieve zero copy transmission to subscribers.
But this is little complicated than expected. With zero copy, router don't have access to modify packet id per  
subscribing connection. If this is BytesMut, then copy advantage is gone
- Commitlog merges same topic publishes from different connections. Forwarding full payload without any modifications
is not feasible as these merged publisLatest YouTube posts
hes can have same packet ids

30/May/2020
--------------------------

- Does writing the packet with modified packet id help achieve zero copy distribution (assuming we only support 
  QoS 1 subscriptions)??
  
- If the broker only supports QoS 1, We can write the publish packet with modified packet id. If we can separate
 router and connection data packet ids, we can be sure that packet ids won't collide while publishing. Since connection's 
 next pull only happens while all the data is acked back, we don't have to worry about inflight collisions
 
- To support all the QoS with (almost) zero copy, We can probably exploit the idea of framing batched publishes while
  pulling data from commitlog. Commitlog while extracting data could return a Vec of Bytes(meta data header, pkid, qos 
  etc) and Bytes (payload). Meta data is assembled based on DataRequest type (QoS 0, 1, 2 and dup) [experiment]
  
- The most approachable 1st step is to probably achieve zero copy for QoS 1 and don't bother too much about optimizing 
  QoS 0 and 2 as they step into each other's legs. Also QoS 1 is the most used one
  
31/May/2020
---------------------------

- Link can request data for all the topics in 1 DataRequest. This might cause fairness issues for other connections 
  it's handle a lot of topics
  
 28/June/2020
 --------------------------
 
 - Sending a bulk (merged Bytes) for replication needs the receiver to convert to Vec<Bytes> because subscribers expect 
   individual MQTT packets, not merged ones. This would've been a good addition to MQTT protocol?
  
  
TODO
---------------------------

-[ ] Subscriber should pull again from correct offset after reconnecting to a different broker
-[X] When a connection/replicator requests data from invalid topic, router doesn't return anything. Links perform next poll
  only after receiving reply on previous request. Not replying anything might be a footgun
-[ ] Limit number of connections
-[ ] Limit connection rate
-[ ] Limit number of topics in connection and router
 

References
-----------------

- https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Replication
- https://jvns.ca/blog/2016/01/23/sendfile-a-new-to-me-system-call/ 
