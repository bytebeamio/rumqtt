My current design looks like this. Each tcp/tls connection is spawned and they send all the incoming messages to a router 
thread where state is maintained and replication happens. My question is regarding the ineffecieny in router sending the 
acks back to connection again over a channel (where connection writes it to network). 
Option 2 would be splitting tcp/tls and router owning tcp writers. But io::split on tls is locked and the behavior won't be parallel. 
Is there a better way to do this for better throughput and efficiency?


Matthias from tokio discord:

It's a lot more of a difficult problem than it initially seems. You are on the right track with keeping the main state on a central "router task". 
It scales not well. But these things are so stateful it is hard to avoid.

I would not do writing on that task. That would mean any slow reader will slow down everything. 
Instead you want to enqueue messages back to the connection. 
That queuing should have a limit, and you need to decide on what to do after the max amount of messages had been queued:

- Drop the client
- Let the router task block on enqueuing, and then also let other tasks block on submitting messages to the router

The latter might not be ideal, since it means any slow client can slow down everything
I guess I would likely still have a read and a write task per connection. 
For communication between the router and the writer a Channel works, but you can also build more efficient queues 
using synchronous Mutex and a Queue for Messages, and an async ManualResetEvent or similar for waking up the writer 
when there is something to do. That way the writer can dequeue multiple messages at once send send them via 
vectored IO


Thanks @Matthias247. I recognized the "one slow connection slowing down the whole router and hence all the connections" problem. 
The router decides on which connection to forward the received data to based on its state and sends it to the target connection 
over a channel (where network write happens).  I thought to use a select! but looks like it's not trivial/posible as the target 
connection/connections depends on the state. I'm going to rely on try_send to detect slow connections and handle it in the router or 
disconnect the client (so channel's buffer size is the limit you mentioned)

Coming to Mutex Queue and ManualResetEvent. Isn't sending a microbatch of events (for n/w write) over channel and performing 
vectored IO a better option than building this queue?
