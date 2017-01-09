# Index Batcher

A wrapper to automatically batch modifications

IndexBatcher aggregates modifications (Index(), Delete(), SetInternal(), DeleteInternal()) that occur within close proximity into a batch execution for increased throughput at the cost of an amortized period / 2 latency increase. It is a fairly transparent wrapper around the Index interface that is itself an Index. The period is adjustable. When experimenting with Cassandra as a backend, I ran into throughput bottlenecks as a result of the number of keys being inserted at the KVStore layer. Guiding individual insertions into batches via this layer nearly doubled throughput.

The IndexBatcher is a contribution from [Rob McColl](https://github.com/robmccoll).
