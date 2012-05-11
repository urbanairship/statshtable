## StatsHTable

### Basics
StatsHTable is a tool for measuring the performance of HBase clusters in real time. It works by wrapping your HTable object with a StatsHTable object which measures the latency of every call to the underlying HTable. 

Latency and throughput are reported for each regionserver, region, and operation type (put, get, etc.). The latency data are stored and processed using the Yammer metrics library. This information can help isolate performance problems.

### HBase version support
StatsHTable currently supports only HBase 0.90.x. Patches adding support for later HBase versions would be welcome.

### Examples

See the test cases in the source tree.

