## StatsHTable

![statshtable jconsole screencap](https://github.com/urbanairship/statshtable/raw/master/statshtable_screenshot_resized.png)

### Basics
StatsHTable is a tool for measuring the performance of HBase clusters in real time. It works by wrapping your HTable object with a StatsHTable object which measures the latency of every call to the underlying HTable.

The main idea is that you can easily see over JMX how your HBase cluster is performing with only minor changes to your app. The regionservers are not touched. 

Latency and throughput are reported for each regionserver, region, and operation type (put, get, etc.). The latency data are stored and processed using the Yammer metrics library. This information can help isolate performance problems.

### HBase version support
StatsHTable currently supports only HBase 0.90.x. Patches adding support for later HBase versions would be welcome.

### How to use it

Create a StatsHTablePool object and use it like you would use a normal HTablePool. Alternately, use a StatsHTable instead of an HTable.

See the test cases in the source tree for examples.

