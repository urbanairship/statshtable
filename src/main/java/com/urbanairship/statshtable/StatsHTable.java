/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.statshtable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.GaugeMetric;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.reporting.JmxReporter;
import org.apache.commons.collections.comparators.ReverseComparator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Wraps an HTable and exposes latency metrics by region/server/operation.
 * 
 * The idea is that HBase clients would use a StatsHTable/StatsHTablePool/StatsHTableFactory in any place where 
 * they would ordinarily use an HTable/HTablePool/HTableFactory. Then, behind the scenes, request latencies are 
 * measured and the stats exposed via JMX. The only change to the client is to use the StatsXXXX classes.
 * 
 * HTables are not thread-safe, so this class is also not thread-safe.
 */
public class StatsHTable implements HTableInterface {
    private static final Log log = LogFactory.getLog(StatsHTable.class);

    // If an iterator next() call takes less than this long, we'll assume the result was cached locally
    // and we'll ignore its latency.
    private static final long IGNORE_ITERATOR_THRESHOLD_NANOS = TimeUnit.MICROSECONDS.toNanos(10);
    public static final int NUM_SLOW_QUERIES = 50; 
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Object jmxSetupLock = new Object();
    private static boolean jmxSetupDone = false;
    
    // These contain TimerMetrics for individual regions and servers. These are global singletons.
    static StatsTimerRegistry regionTimers = RegionTimers.getInstance();
    static StatsTimerRegistry serverTimers = ServerTimers.getInstance();
    
    // Contains TimerMetrics by operation type (e.g. put, get)
    static StatsTimerRegistry opTypeTimers = new StatsTimerRegistry("_opTypes");
    
    private final HTable normalHTable;
    private final String metricsScope;
    private final SlowQueryGauge slowQueryGauge;

    static Comparator<Double> doubleComparator = new Comparator<Double> () {
        @Override
        public int compare(Double d1, Double d2) {
            return d1.compareTo(d2);
        }
    };
    
    static Comparator<String> stringComparator = new Comparator<String> () {
        @Override
        public int compare(String s1, String s2) {
            return s1.compareTo(s2);
        }
    };
    
    /**
     * @param metricsScope all StatsHTables with the same scope update the same underlying stats. The JMX
     * output is segregated by scope. When two different workloads are sharing a JVM, they should use different
     * scopes to avoid influencing each other's stats.
     * @param normalHTable the HTable to be wrapped by this StatsHTable
     */
    public StatsHTable(final String metricsScope, HTable normalHTable) {
        this.metricsScope = metricsScope;
        this.normalHTable = normalHTable;
        
        RemoveOldTimers.startIfNot();
        
        /**
         * For each scope, there are gauges that give servers & regions in descending order of mean latency,
         * and a slow query gauge that gives the slowest queries and their stack traces. This code creates those 
         * gauges if they don't already exist.
         */
        // Get or create these gauges. If we're the first StatsHTable for our metricsScope, will create and register.
        Metrics.newGauge(newMetricName(metricsScope, "serversByLatency90th"), new GaugeForScope(serverTimers));
        Metrics.newGauge(newMetricName(metricsScope, "regionsByLatency90th"), new GaugeForScope(regionTimers));
        slowQueryGauge = (SlowQueryGauge)Metrics.newGauge(newMetricName(metricsScope, "slowQueries"), 
                new SlowQueryGauge(NUM_SLOW_QUERIES));
        
        // The first StatsHTable will set up JMX reporting for region detail and server detail registries
        if(!jmxSetupDone) {
            synchronized (jmxSetupLock) {
                if(!jmxSetupDone) {
                    new JmxReporter(regionTimers).start();
                    new JmxReporter(serverTimers).start();
                    new JmxReporter(opTypeTimers).start();
                    jmxSetupDone = true;
                }
            }
        }
    }
    
    private <T> T timedExecute(OpType opType, byte[] key, Callable<T> callable) throws Exception {
        List<byte[]> keys;
        if(key == null) {
            keys = Collections.emptyList();
        } else {
            keys = ImmutableList.of(key);
        }
        return timedExecute(ImmutableList.of(opType), keys, callable);
    }

    /**
     * Runs the callable and records its latency.
     * 
     * The latency will be recorded for every OpType given under timerLabels,
     * which allows to identify slow operations like GET or PUT.
     * 
     * The latency will also be recorded for every region that one of the "keys"
     * belongs to, which lets us identify slow regions.
     * 
     * The latency will also be recorded for every server touched by an
     * operation, which lets us identify slow servers.
     * 
     * @param callable
     * @param keys
     * @return the return value of the callable
     * @throws Exception if and only if the callable throws, the exception will bubble up
     */
    private <T> T timedExecute(List<OpType> opTypes, List<byte[]> keys, Callable<T> callable) throws Exception {
        long beforeNanos = System.nanoTime();
        T result = callable.call();
        long durationNanos = System.nanoTime() - beforeNanos;
        updateStats(opTypes, keys, durationNanos);
        return result; 
    }

    /**
     * Like {@link #timedExecute(List, List, Callable)} except it's used by iterator wrappers. When we're using 
     * an iterator, we don't know which regions we touched until the iterator returns Results. Each result has
     * a key, from which we can figure out the region that it came from.
     */
    private Result timedExecuteIterator(List<OpType> opTypes, Callable<Result> callable) throws Exception {
        long beforeNanos = System.nanoTime();
        Result result = callable.call();
        long durationNanos = System.nanoTime() - beforeNanos;
        
        List<byte[]> keys = new ArrayList<byte[]>(1);
        // If the iterator returned very quickly, we assume it's reading from a local cache
        // and not doing an RPC. We don't update the stats in this case because they skew the results
        // and make actual HBase problems harder to find. This is a nasty hack that will often be wrong,
        // but hopefully it will at least point in the direction of a problem if one exists.
        if(durationNanos > IGNORE_ITERATOR_THRESHOLD_NANOS) {
            if(result != null) {
                byte[] row = result.getRow(); 
                if(row != null) {
                    keys.add(row);
                    updateStats(opTypes, keys, durationNanos);
                }
            }
        }
        return result;
    }
    
    /**
     * Like {@link #timedExecute(List, List, Callable)} except it's used by iterator wrappers. When we're using 
     * an iterator, we don't know which regions we touched until the iterator returns Results. Each result has
     * a key, from which we can figure out the region that it came from.
     */
    private Result[] timedExecuteArrayIterator(List<OpType> opTypes, Callable<Result[]> callable) 
            throws Exception {
        long beforeNanos = System.nanoTime();
        Result[] results = callable.call();
        long durationNanos = System.nanoTime() - beforeNanos;
        
        // If the iterator returned very quickly, we assume it's reading from a local cache
        // and not doing an RPC. We don't update the stats in this case because they skew the results
        // and make actual HBase problems harder to find.
        if(durationNanos > IGNORE_ITERATOR_THRESHOLD_NANOS) {
            List<byte[]> keys = new ArrayList<byte[]>(results.length);
            for(int i=0; i<results.length; i++) {
                if(results[i] != null && !results[i].isEmpty()) {
                    keys.add(results[i].getRow());
                }
            }
            updateStats(opTypes, keys, durationNanos);
        }
        return results;
    }

    /**
     * Like {@link #timedExecute(List, List, Callable)} except it's used by the coprocessor Exec wrapper. After
     * we have the collected Exec result set, we can figure out the region that each came from.
     */
    private <T> Map<byte[],T> timedExecuteMapResult(OpType opType, Callable<Map<byte[],T>> callable)
            throws Exception {
        long beforeNanos = System.nanoTime();
        Map<byte[],T> results = callable.call();
        long durationNanos = System.nanoTime() - beforeNanos;

        List<OpType> opTypes = new ArrayList<OpType>(1);
        opTypes.add(opType);
        List<byte[]> keys = new ArrayList<byte[]>(results.keySet().size());
        keys.addAll(results.keySet());
        updateStats(opTypes, keys, durationNanos);

        return results;
    }

    private void updateStats(List<OpType> opTypes, List<byte[]> keys, long durationNanos) {
        try {
            for (OpType opType : opTypes) {
                opTypeTimers.newSHTimerMetric(metricsScope, opType.toString()).update(durationNanos, TimeUnit.NANOSECONDS);
            }

            Set<String> regionNames = new HashSet<String>();
            Set<String> serverNames = new HashSet<String>();

            slowQueryGauge.maybeUpdate(durationNanos);
            
            // Make sets of regions and servers that we'll record the latency for
            if(keys != null) {
                for (byte[] key : keys) {
                    // 'False' as second parameter to getRegionLocation allows use of cached
                    // information when getting region locations
                    HRegionLocation hRegionLocation = normalHTable.getRegionLocation(key);
                    regionNames.add(hRegionLocation.getRegionInfo().getEncodedName());
                    serverNames.add(hRegionLocation.getServerAddress().getHostname());
                }
            }

            // Track latencies by region, there may be hot regions that are slow
            String tableName = new String(normalHTable.getTableName());
            for (String regionName : regionNames) {
                String metricName = tableName + "|" + regionName;
                regionTimers.newSHTimerMetric(metricsScope, metricName).update(durationNanos, TimeUnit.NANOSECONDS);
            }

            // Track latencies by region server, there may be slow servers
            for (String serverName : serverNames) {
                serverTimers.newSHTimerMetric(metricsScope, serverName).update(durationNanos, TimeUnit.NANOSECONDS);
            }

        } catch (Exception e) {
            log.error("Couldn't update latency stats", e);
        }
    }
    
    @Override
    public Result get(final Get get) throws IOException {
        try {
            return timedExecute(OpType.GET, get.getRow(), new Callable<Result>() {
                public Result call() throws IOException {
                    return normalHTable.get(get);
                }
            });
        } catch (IOException e) {
            throw (IOException) e;
        } catch (Exception e) {
            final String errMsg = "Unexpected exception in stats wrapper for get()";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public byte[] getTableName() {
        return normalHTable.getTableName();
    }

    @Override
    public Configuration getConfiguration() {
        return normalHTable.getConfiguration();
    }

    @Override
    public HTableDescriptor getTableDescriptor() throws IOException {
        return normalHTable.getTableDescriptor();
    }

    @Override
    public boolean exists(final Get get) throws IOException {
        try {
            return timedExecute(OpType.EXISTS, get.getRow(), new Callable<Boolean>() {
                public Boolean call() throws IOException {
                    return normalHTable.exists(get);
                }
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            final String errMsg = "Unexpected exception in stats wrapper for exists()";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public void batch(final List<Row> actions, final Object[] results)
            throws IOException, InterruptedException {
        try {
            List<byte[]> keys = new ArrayList<byte[]>(actions.size());
            for (Row action : actions) {
                keys.add(action.getRow());
            }
            timedExecute(ImmutableList.of(OpType.BATCH), keys, new Callable<Object>() {
                @Override
                public Object call() throws IOException, InterruptedException {
                    normalHTable.batch(actions, results);
                    return null; // We're forced by Callable to return
                                 // *something*
                }
            });
        } catch (IOException e) {
            throw e;
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            final String errMsg = "Unexpected exception in stats wrapper for batch()";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public Object[] batch(final List<Row> actions)
            throws IOException, InterruptedException {
        try {
            List<byte[]> keys = new ArrayList<byte[]>(actions.size());
            for (Row action : actions) {
                keys.add(action.getRow());
            }
            return timedExecute(ImmutableList.of(OpType.BATCH), keys, new Callable<Object[]>() {
                @Override
                public Object[] call() throws IOException, InterruptedException {
                    return normalHTable.batch(actions);
                }
            });
        } catch (IOException e) {
            throw e;
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            final String errMsg = "Unexpected exception in stats wrapper for batch()";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public Result[] get(final List<Get> gets) throws IOException {
        try {
            List<byte[]> keys = new ArrayList<byte[]>(gets.size());
            for (Get get : gets) {
                keys.add(get.getRow());
            }
            return timedExecute(ImmutableList.of(OpType.MULTIGET), keys, new Callable<Result[]>() {
                @Override
                public Result[] call() throws IOException, InterruptedException {
                    return normalHTable.get(gets);
                }
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            final String errMsg = "Unexpected exception in stats wrapper for multiget";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public Result getRowOrBefore(final byte[] row, final byte[] family) throws IOException {
        try {
            return timedExecute(OpType.GET_ROW_OR_BEFORE, row, new Callable<Result>() {
                @Override
                public Result call() throws IOException {
                    return normalHTable.getRowOrBefore(row, family);
                }
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            final String errMsg = "Unexpected exception in stats wrapper for getRowOrBefore()";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public ResultScanner getScanner(final Scan scan) throws IOException {
        try {
            byte[] startRow = scan.getStartRow();
            if (startRow == null) {
                startRow = new byte[] {};
            }
            return timedExecute(OpType.GET_SCANNER, startRow, new Callable<ResultScanner>() {
                @Override
                public ResultScanner call() throws IOException {
                    return new WrappedResultScanner(normalHTable.getScanner(scan));
                }
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            final String errMsg = "Unexpected exception in stats wrapper for getScanner()";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }
    
    private class WrappedResultScanner implements ResultScanner {
        private final ResultScanner normalResultScanner;
        
        public WrappedResultScanner(ResultScanner rs) {
            this.normalResultScanner = rs;
        }
        
        @Override
        public Iterator<Result> iterator() { 
            try {
                return timedExecute(OpType.RESULTSCANNER_ITERATOR, null, new Callable<Iterator<Result>>() {
                    @Override
                    public Iterator<Result> call() {
                        return new WrappedResultIterator(normalResultScanner.iterator());
                    }
                });
            } catch (Exception e) {
                final String errMsg = "Unexpected exception in stats wrapper for ResultScanner.iterator()";
                log.error(errMsg, e);
                throw new RuntimeException(errMsg, e);
            }
        }

        @Override
        public Result next() throws IOException {
            try {
                return timedExecuteIterator(ImmutableList.of(OpType.RESULTSCANNER_NEXT), new Callable<Result>() {
                   @Override
                   public Result call() throws IOException {
                       return normalResultScanner.next();
                   }
                });
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                final String errMsg = "Unexpected exception in stats wrapper for ResultScanner.next()";
                log.error(errMsg, e);
                throw new RuntimeException(errMsg, e);
            }
        }

        @Override
        public Result[] next(final int nbRows) throws IOException {
            try {
                return timedExecuteArrayIterator(ImmutableList.of(OpType.RESULTSCANNER_NEXTARRAY), 
                        new Callable<Result[]>() {
                    @Override
                    public Result[] call() throws IOException {
                        return normalResultScanner.next(nbRows);
                    }
                });
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                final String errMsg = "Unexpected exception in stats wrapper for ResultScanner.next(int)";
                log.error(errMsg, e);
                throw new RuntimeException(errMsg, e);
            }
        }

        @Override
        public void close() {
            try {
                timedExecute(OpType.RESULTSCANNER_CLOSE, null, new Callable<Object>() {
                    @Override
                    public Object call() {
                        normalResultScanner.close();
                        return null; // Callable requires that we return something
                    }
                });
            } catch (Exception e) {
                final String errMsg = "Unexpected exception in stats wrapper for ResultScanner.close()";
                log.error(errMsg, e);
                throw new RuntimeException(errMsg, e);
            }
        }
    }
    
    private class WrappedResultIterator implements Iterator<Result> {
        private final Iterator<Result> normalIterator;
        
        public WrappedResultIterator(Iterator<Result> normalIterator) {
            this.normalIterator = normalIterator;
        }

        @Override
        public boolean hasNext() {
            try {
                return timedExecute(OpType.RESULTITERATOR_HASNEXT, null, new Callable<Boolean>() {
                    @Override
                    public Boolean call() {
                        return normalIterator.hasNext();
                    }
                });
            } catch (RuntimeException e) {
                // Since the Iterator interface prevents checked exceptions from being thrown, HBase's
                // Htable.ClientScanner throws IOExceptions wrapped in RuntimeException. This is a "normal"
                // error in that it will happen if there are network errors or host failures, and does not
                // indicate a problem in StatsHTable.
                throw e;
            } catch (Exception e) {
                final String errMsg = "Unexpected exception in stats wrapper for ResultIterator.hasNext()";
                log.error(errMsg, e);
                throw new RuntimeException(errMsg, e);
            }
        }

        @Override
        public Result next() {
            try {
                return timedExecuteIterator(ImmutableList.of(OpType.RESULTITERATOR_NEXT), new Callable<Result>() {
                    @Override
                    public Result call() throws Exception {
                        return normalIterator.next();
                    }
                });
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                final String errMsg = "Unexpected exception in stats wrapper for ResultIterator.next()";
                log.error(errMsg, e);
                throw new RuntimeException(errMsg, e);
            }
        }

        @Override
        public void remove() {
            normalIterator.remove(); // I assume this just throws UnsupprtedOperationException
        }
    }

    @Override
    public ResultScanner getScanner(final byte[] family) throws IOException {
        try {
            return timedExecute(ImmutableList.of(OpType.GET_SCANNER), null, new Callable<ResultScanner>() {
                @Override
                public ResultScanner call() throws IOException {
                    return new WrappedResultScanner(normalHTable.getScanner(family));                    
                }
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            final String errMsg = "Unexpected exception in stats wrapper for getScanner(byte[])";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public ResultScanner getScanner(final byte[] family, final byte[] qualifier) throws IOException {
        try {
            return timedExecute(ImmutableList.of(OpType.GET_SCANNER), null, new Callable<ResultScanner>() {
                @Override
                public ResultScanner call() throws IOException {
                    return new WrappedResultScanner(normalHTable.getScanner(family, qualifier));                    
                }
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            final String errMsg = "Unexpected exception in stats wrapper for getScanner(byte[],byte[])";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public void put(final Put put) throws IOException {
        try {
            timedExecute(OpType.PUT, put.getRow(), new Callable<Object>() {
                @Override
                public Object call() throws IOException {
                    normalHTable.put(put);
                    return null; // We're required by Callable to return
                                 // something
                }
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            final String errMsg = "Unexpected exception in stats wrapper for put()";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public void put(final List<Put> puts) throws IOException {
        try {
            List<byte[]> keys = new ArrayList<byte[]>();
            for(Put put: puts) {
                keys.add(put.getRow());
            }
            timedExecute(ImmutableList.of(OpType.MULTIPUT), keys, new Callable<Object>() {
                @Override
                public Object call() throws IOException {
                    normalHTable.put(puts);
                    return null; // We're required by Callable to return something
                }
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            final String errMsg = "Unexpected exception in stats wrapper for multiput";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public boolean checkAndPut(final byte[] row, final byte[] family, final byte[] qualifier, final byte[] value,
            final Put put) throws IOException {
        try {
            return timedExecute(OpType.CHECK_AND_PUT, row, new Callable<Boolean>() {
                @Override
                public Boolean call() throws IOException {
                    return normalHTable.checkAndPut(row, family, qualifier, value, put);
                }
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            final String errMsg = "Unexpected exception in stats wrapper for checkAndPut()";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public void delete(final Delete delete) throws IOException {
        try {
            timedExecute(OpType.DELETE, delete.getRow(), new Callable<Object>() {
                @Override
                public Object call() throws IOException {
                    normalHTable.delete(delete);
                    return null; // We're required by Callable to return
                                 // something
                }
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            final String errMsg = "Unexpected exception in stats wrapper for delete()";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public void delete(final List<Delete> deletes) throws IOException {
        try {
            List<byte[]> keys = new ArrayList<byte[]>();
            for(Delete delete: deletes) {
                keys.add(delete.getRow());
            }
            timedExecute(ImmutableList.of(OpType.MULTIDELETE), keys, new Callable<Object>() {
                @Override
                public Object call() throws IOException {
                    normalHTable.delete(deletes);
                    return null; // We're required by Callable to return something
                }
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            final String errMsg = "Unexpected exception in stats wrapper for multidelete";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
        
    }

    @Override
    public boolean checkAndDelete(final byte[] row, final byte[] family, final byte[] qualifier, final byte[] value,
            final Delete delete) throws IOException {
        try {
            return timedExecute(OpType.CHECK_AND_DELETE, row, new Callable<Boolean>() {
                @Override
                public Boolean call() throws IOException {
                    return normalHTable.checkAndDelete(row, family, qualifier, value, delete);
                }
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            final String errMsg = "Unexpected exception in stats wrapper for checkAndDelete";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public Result increment(final Increment increment) throws IOException {
        try {
            return timedExecute(OpType.INCREMENT, increment.getRow(), new Callable<Result>() {
                @Override
                public Result call() throws IOException {
                    return normalHTable.increment(increment);
                }
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            final String errMsg = "Unexpected exception in stats wrapper for increment";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public long incrementColumnValue(final byte[] row, final byte[] family, final byte[] qualifier, final long amount) 
            throws IOException {
        try {
            return timedExecute(OpType.INCREMENT, row, new Callable<Long>() {
                @Override
                public Long call() throws IOException {
                    return normalHTable.incrementColumnValue(row, family, qualifier, amount);
                }
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            final String errMsg = "Unexpected exception in stats wrapper for incrementColumnValue";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public long incrementColumnValue(final byte[] row, final byte[] family, final byte[] qualifier, final long amount, final boolean writeToWAL)
            throws IOException {
        try {
            return timedExecute(OpType.INCREMENT, row, new Callable<Long>() {
                @Override
                public Long call() throws IOException {
                    return normalHTable.incrementColumnValue(row, family, qualifier, amount, writeToWAL);
                }
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            final String errMsg = "Unexpected exception in stats wrapper for incrementColumnValue";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public boolean isAutoFlush() {
        return normalHTable.isAutoFlush();
    }

    @Override
    public void flushCommits() throws IOException {
        try {
            // This is a weird case since we don't know the keys/region/servers that we're talking to.
            timedExecute(OpType.FLUSHCOMMITS, null, new Callable<Object>() {
                @Override
                public Object call() throws IOException {
                    normalHTable.flushCommits();
                    return null;
                    
                }
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            final String errMsg = "Unexpected exception in stats wrapper for flushCommits";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public void close() throws IOException {
        normalHTable.close();
    }

    @Override
    public RowLock lockRow(final byte[] row) throws IOException {
        try {
            return timedExecute(OpType.LOCKROW, row, new Callable<RowLock>() {
                @Override
                public RowLock call() throws IOException {
                    return normalHTable.lockRow(row);
                }
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            final String errMsg = "Unexpected exception in stats wrapper for lockRow";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public void unlockRow(final RowLock rl) throws IOException {
        try {
            timedExecute(OpType.UNLOCKROW, rl.getRow(), new Callable<Object>() {
                @Override
                public Object call() throws IOException {
                    normalHTable.unlockRow(rl);
                    return null;
                }
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            final String errMsg = "Unexpected exception in stats wrapper for unlockRow";
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
        
    }



    /**
     * Get the normal HTable instance that underlies this StatsHTable.
     */
    public HTableInterface unwrap() {
        return normalHTable;
    }

    /**
     * Return an ordered Map where the order of entries is determined by the natural ordering of the value type.
     */
    public static Map<String,Double> sortMapReversedByValue(Map<String,Double> in) {
        @SuppressWarnings("unchecked")
        SortedSetMultimap<Double,String> treeMap = TreeMultimap.create(new ReverseComparator(doubleComparator), 
                stringComparator); 
        for(Map.Entry<String,Double> e: in.entrySet()) {
            treeMap.put(e.getValue(), e.getKey());
        }
        
        Map<String,Double> sorted = new LinkedHashMap<String,Double>();
        for(Map.Entry<Double,String> e: treeMap.entries()) {
            sorted.put(e.getValue(), e.getKey());
        }
        return sorted;
    }
    
    /**
     * Shared code for the gauges that show regions/servers in descending order of latency.
     */
    private class GaugeForScope extends GaugeMetric<String> {
        private final StatsTimerRegistry registry;
        
        public GaugeForScope(StatsTimerRegistry registry) {
            this.registry = registry;
        }
        
        @Override
        public String value() {
            Map<String,Double> latencies = new HashMap<String,Double>();
            for(Map.Entry<MetricName,Metric> e: registry.allMetrics().entrySet()) {
                Metric metric = e.getValue();
                if(metric instanceof SHTimerMetric) {
                    SHTimerMetric timerMetric = (SHTimerMetric)metric;
                    latencies.put(e.getKey().getName(), timerMetric.getValue(0.9D));
                }
            }
            try {
                return objectMapper.writeValueAsString(sortMapReversedByValue(latencies));
            } catch (Exception e) {
                log.error("JSON encoding problem", e);
                return "exception, see logs";
            }
        }
    }
    
    public static final MetricName newMetricName(String scope, String name) {
        return new MetricName("com.urbanairship.statshtable", "StatsHTable", name, scope);
    }
}

