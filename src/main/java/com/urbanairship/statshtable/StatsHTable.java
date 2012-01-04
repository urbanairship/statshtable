package com.urbanairship.statshtable;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.ImmutableList;
import com.urbanairship.statshtable.StatsHTableFactory.OpType;
import com.yammer.metrics.core.GaugeMetric;
import com.yammer.metrics.reporting.JmxReporter;

/**
 * Wraps an HTable and exposes latency metrics by region/server/operation.
 */
public class StatsHTable implements HTableInterface {
    private static final Logger log = LogManager.getLogger(StatsHTable.class);

    private static final int TIMER_TICK_EXECUTOR_THREADS = 20;
    
    private final static ObjectMapper objectMapper = new ObjectMapper();
    
    static final String REGION_TIMER_PREFIX = "region_";
    static final String SERVER_TIMER_PREFIX = "server_";

    // Contain TimerMetrics for individual regions and servers. These are global singletons.
    private static AtomicRegistry<String,String,SHTimerMetric> regionTimers = RegionTimers.getInstance();
    private static AtomicRegistry<String,String,SHTimerMetric> serverTimers = ServerTimers.getInstance();
    
    // Contains TimerMetrics by operation type (e.g. put, get)
    private static AtomicRegistry<String,OpType,SHTimerMetric> opTypeTimers = new AtomicRegistry<String,OpType,SHTimerMetric>();
    
    // JMX gauges that give a list of the regions/servers in descending order of latency
    private static ConcurrentMap<String,GaugeMetric<String>> serverGauges = new ConcurrentHashMap<String,GaugeMetric<String>>(); 
    private static ConcurrentMap<String,GaugeMetric<String>> regionGauges = new ConcurrentHashMap<String,GaugeMetric<String>>(); 

    // JMX gauge that shows the slowest queries by latency, with stack trace
    private static ConcurrentMap<String,SlowQueryGauge> slowQueryGauges = new ConcurrentHashMap<String,SlowQueryGauge>(); 

    // Executor shared by TimerMetrics, we don't want them to each create their own thread pool 
    private static final ScheduledExecutorService tickExecutor = Executors.newScheduledThreadPool(TIMER_TICK_EXECUTOR_THREADS);
    
    private final HTable normalHTable;
    private final String metricsScope;
    private final SlowQueryGauge slowQueryGauge;
    
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
        try {
            if(!serverGauges.containsKey(metricsScope)) {
                GaugeMetric<String> serverGauge = new GaugeForScope(serverTimers);
                GaugeMetric<String> existingServerGauge = serverGauges.putIfAbsent(metricsScope, serverGauge);
                if(existingServerGauge == null) {
                    // We're the first to create the slow server gauge, it's our responsibility to register JMX
                    ObjectName serversObjName = new ObjectName("com.urbanairship.statshtable:type=" + metricsScope + 
                            ", name=" + "serversByLatency");
                    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                    JmxReporter.Gauge serversReporter = new JmxReporter.Gauge(serverGauge, serversObjName);
                    mbs.registerMBean(serversReporter, serversObjName);
                }
            }
            
            if(!regionGauges.containsKey(metricsScope)) {
                GaugeMetric<String> regionGauge = new GaugeForScope(regionTimers);
                
                GaugeMetric<String> existingRegionGauge = regionGauges.putIfAbsent(metricsScope, regionGauge);
                if(existingRegionGauge == null) {
                    // We're the first to create the slow regions gauge, it's our responsibility to register JMX
                    ObjectName regionsObjName = new ObjectName("com.urbanairship.statshtable:type=" + metricsScope + 
                            ", name=" + "regionsByLatency");
                    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                    JmxReporter.Gauge regionsReporter = new JmxReporter.Gauge(regionGauge, regionsObjName);
                    mbs.registerMBean(regionsReporter, regionsObjName);
                }
            }
            
            if(!slowQueryGauges.containsKey(metricsScope)) {
                SlowQueryGauge newSlowQueriesGauge = new SlowQueryGauge(50);
                SlowQueryGauge existingGauge = slowQueryGauges.putIfAbsent(metricsScope, newSlowQueriesGauge);
                if(existingGauge == null) {
                    // We're the first to create the slow query gauge, it's our responsibility to register JMX
                    ObjectName slowQueriesObjName = new ObjectName("com.urbanairship.statshtable:type=" + metricsScope + 
                            ", name=" + "slowQueries");
                    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                    JmxReporter.Gauge regionsReporter = new JmxReporter.Gauge(newSlowQueriesGauge, slowQueriesObjName);
                    mbs.registerMBean(regionsReporter, slowQueriesObjName);
                }
            }
            slowQueryGauge = slowQueryGauges.get(metricsScope);
        } catch (JMException e) {
            log.error("JMX error prevented StatsHTable instantiation", e);
            throw new RuntimeException(e);
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
     * @param timerLabels
     *            the latency of the callable will be used to update each of the
     *            timers
     * @param callable
     * @param keys
     * @return the return value of the callable
     * @throws Exception
     *             if and only if the callable throws, the exception will bubble
     *             up
     */
    private <T> T timedExecute(List<OpType> opTypes, List<byte[]> keys, Callable<T> callable) throws Exception {
        long beforeMs = System.currentTimeMillis();
        T result = callable.call();
        long durationMs = System.currentTimeMillis() - beforeMs;

        try {
            for (OpType opType : opTypes) {
                getOrRegisterOpTypeTimer(opType).update(durationMs, TimeUnit.MILLISECONDS);
            }

            Set<String> regionNames = new HashSet<String>();
            Set<String> serverNames = new HashSet<String>();

            slowQueryGauge.maybeUpdate(durationMs);
            
            // Make sets of regions and servers that we'll record the latency for
            if(keys != null) {
                for (byte[] key : keys) {
                    HRegionLocation hRegionLocation = normalHTable.getRegionLocation(key);
                    regionNames.add(hRegionLocation.getRegionInfo().getEncodedName());
                    serverNames.add(hRegionLocation.getServerAddress().getHostname());
                }
            }

            // Track latencies by region, there may be hot regions that are slow
            for (String regionName : regionNames) {
                getOrRegisterRegionTimer(regionName).update(durationMs, TimeUnit.MILLISECONDS);
            }

            // Track latencies by region server, there may be slow servers
            for (String serverName : serverNames) {
                getOrRegisterServerTimer(serverName).update(durationMs, TimeUnit.MILLISECONDS);
            }

        } catch (Exception e) {
            log.error("Couldn't update latency stats", e);
        }

        return result;
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
    public void batch(final List<Row> actions, final Object[] results) throws IOException, InterruptedException {
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
    public Object[] batch(final List<Row> actions) throws IOException, InterruptedException {
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
        // TODO: return a wrapped ResultScanner that does latency measurements
        try {
            byte[] startRow = scan.getStartRow();
            if (startRow == null) {
                startRow = new byte[] {};
            }
            return timedExecute(OpType.GET_SCANNER, startRow, new Callable<ResultScanner>() {
                @Override
                public ResultScanner call() throws IOException {
                    return normalHTable.getScanner(scan);
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

    @Override
    public ResultScanner getScanner(byte[] family) throws IOException {
        // This is not used in shennendoah, don't bother measuring
        return normalHTable.getScanner(family);
    }

    @Override
    public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
        // This is not used in shennendoah, don't bother measuring
        return normalHTable.getScanner(family, qualifier);
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
    public static <K,V> Map<K,V> sortMapByValue(Map<K,V> in) {
        NavigableMap<V,K> treeMap = new TreeMap<V,K>(); 
        for(Map.Entry<K,V> e: in.entrySet()) {
            treeMap.put(e.getValue(), e.getKey());
        }
        
        Map<K,V> sorted = new LinkedHashMap<K,V>();
        for(Map.Entry<V,K> e: treeMap.entrySet()) {
            sorted.put(e.getValue(), e.getKey());
        }
        return sorted;
    }
    
    /**
     * Shared code for the gauges that show regions/servers in descending order of latency.
     */
    private class GaugeForScope implements GaugeMetric<String> {
        private final AtomicRegistry<String,String,SHTimerMetric> registry;
        
        public GaugeForScope(AtomicRegistry<String,String,SHTimerMetric> registry) {
            this.registry = registry;
        }
        
        @Override
        public String value() {
            Map<String,Double> meanLatencies = new HashMap<String,Double>();
            for(Map.Entry<String,SHTimerMetric> e: registry.innerMap(metricsScope).entrySet()) {
                meanLatencies.put(e.getKey(), e.getValue().mean());
            }
            try {
                return objectMapper.writeValueAsString(sortMapByValue(meanLatencies));
            } catch (Exception e) {
                log.error("JSON encoding problem", e);
                return "exception, see logs";
            }
        }
    }
    
    /**
     * Gets the SHTimerMetric that measures latency for the given region name, or creates it if it doesn't
     * already exist. 
     */
    private SHTimerMetric getOrRegisterRegionTimer(final String regionName) throws Exception {
        final String metricName = new String(normalHTable.getTableName()) + "|" + regionName;
        Callable<SHTimerMetric> metricFactory = new SHTimerMetric.Factory(tickExecutor, metricsScope + "_regions", 
                metricName);
        return regionTimers.registerOrGet(metricsScope, metricName, metricFactory, jmxRegistrator); 
    }
    
    /**
     * Gets the SHTimerMetric that measures latency for the given server, or creates it if it doesn't
     * already exist. 
     */
    private SHTimerMetric getOrRegisterServerTimer(final String serverName) throws Exception {
        Callable<SHTimerMetric> metricFactory = new SHTimerMetric.Factory(tickExecutor, metricsScope + "_servers", 
                serverName);
        return serverTimers.registerOrGet(metricsScope, serverName, metricFactory, jmxRegistrator); 
    }
    
    /**
     * Gets the SHTimerMetric that measures latency for the operation type, or creates it if it doesn't
     * already exist. 
     */
    private SHTimerMetric getOrRegisterOpTypeTimer(final OpType opType) throws Exception {
        Callable<SHTimerMetric> metricFactory = new SHTimerMetric.Factory(tickExecutor, metricsScope, opType.toString());
        return opTypeTimers.registerOrGet(metricsScope, opType, metricFactory, jmxRegistrator);
    }
    
    /**
     * Given a SHTimerMetric, this object will register it in JMX. This is passed to metrics factories to
     * register new SHTimerMetrics.
     */
    private UnaryFunction<SHTimerMetric> jmxRegistrator = new UnaryFunction<SHTimerMetric>() {
        @Override
        public void apply(SHTimerMetric t) {
            try {
                MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                JmxReporter.Timer reporter = new JmxReporter.Timer(t, t.getJmxName());
                mbs.registerMBean(reporter, t.getJmxName());
            } 
            catch (JMException e) {
                log.error(e);
            }
        }
    };
}
