We'll assume all the routers are in sync w.r.t subscription information

Case study

- 3 brokers .. B1, B2, B3
- 3 client .. C1, C2, C3
- 2 topics .. T1, T2
- Replication factor 1

C1 C2 publishses to topic T1

C3 subscribes to T1

C1, 2, 3 are connected to B1, 2, 3

Things to keep in mind

- Client disconnecting from a broker and connecting to a different broker
- Routers disconnecting from each other
- Choosing a leader for a topic wil lead to too crosstalk due to dynamicness of mqtt connections
- Ordering of a C3's subscription data when C1 reconnects to B2. 
- T1 commitlog data is now a combination of data from C1 & C2
- C3 subscription progress tracking when it reconnects to B1
- B1 should detect halfopen connection when C1 reconnects to B2

**Round 1**

Assume C1 published 10 messages on T1: `C1-T1-M1@B1` to `C1-T1-M10@B1`

Assume C2 published 5 messages on T1: `C2-T1-M1@B2` to `C2-T1-M5@B2`