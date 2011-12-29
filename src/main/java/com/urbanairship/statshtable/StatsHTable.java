package com.urbanairship.statshtable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.output.ByteArrayOutputStream;
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
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.GaugeMetric;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.TimerMetric;

/**
 * Wraps an HTable and exposes latency metrics by region/server/operation.
 */
public class StatsHTable implements HTableInterface {
    private static final Logger log = LogManager.getLogger(StatsHTable.class);
    
    static final String REGION_TIMER_PREFIX = "region:";
    static final String SERVER_TIMER_PREFIX = "server:";
    
    private final HTable normalHTable;
    private final String metricsScope;

    private final ObjectMapper objectMapper = new ObjectMapper();
    
    public StatsHTable(String metricsScope, HTable normalHTable) {
        this.metricsScope = metricsScope;
        this.normalHTable = normalHTable;
        RemoveOldRegionTimers.startIfNot();
    }

    private <T> T timedExecute(OpType opType, byte[] key, Callable<T> callable) throws Exception {
        return timedExecute(ImmutableList.of(opType), ImmutableList.of(key), callable);
    }
    
    /**
     * Runs the callable and records its latency. 
     * 
     * The latency will be recorded for every OpType given under timerLabels, which allows to identify
     * slow operations like GET or PUT. 
     * 
     * The latency will also be recorded for every region that one of the "keys" belongs to, which lets
     * us identify slow regions.
     * 
     * The latency will also be recorded for every server touched by an operation, which lets us identify
     * slow servers.
     * 
     * @param timerLabels the latency of the callable will be used to update each of the timers
     * @param callable
     * @param keys 
     * @return the return value of the callable
     * @throws Exception if and only if the callable throws, the exception will bubble up
     */
    private <T> T timedExecute(List<OpType> opTypes, List<byte[]> keys, Callable<T> callable)  
            throws Exception {
        long beforeMs = System.currentTimeMillis();
        T result = callable.call();
        long durationMs = System.currentTimeMillis() - beforeMs;
        
        try {
            for(OpType opType: opTypes) {
                TimerMetric timer = Metrics.newTimer(StatsHTable.class, opType.toString(), 
                            metricsScope, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
                timer.update(durationMs, TimeUnit.MILLISECONDS);
            }
            
            Set<String> regionNames = new HashSet<String>();
            Set<String> serverNames = new HashSet<String>();
            
            // Make sets of regions and servers that we'll record the latency for
            for(byte[] key: keys) {
                HRegionLocation hRegionLocation = normalHTable.getRegionLocation(key);
                regionNames.add(hRegionLocation.getRegionInfo().getRegionNameAsString());
                serverNames.add(hRegionLocation.getServerAddress().getHostname());
            }

            // Track latencies by region, there may be hot regions that are slow
            for(String regionName: regionNames) {
                TimerMetric regionTimer = Metrics.newTimer(StatsHTableFactory.class, 
                        REGION_TIMER_PREFIX + regionName, metricsScope, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
                regionTimer.update(durationMs, TimeUnit.MILLISECONDS);
            }
            
            // Track latencies by region server, there may be slow servers
            for(String serverName: serverNames) {
                TimerMetric serverTimer = Metrics.newTimer(StatsHTableFactory.class, 
                        SERVER_TIMER_PREFIX + serverName, metricsScope, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
                serverTimer.update(durationMs, TimeUnit.MILLISECONDS);
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
            throw (IOException)e;
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
            for(Row action: actions) {
                keys.add(action.getRow());
            }
            timedExecute(ImmutableList.of(OpType.BATCH), keys, new Callable<Object>() {
                @Override
                public Object call() throws IOException, InterruptedException {
                    normalHTable.batch(actions, results);
                    return null;  // We're forced by Callable to return *something* 
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
            for(Row action: actions) {
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
            for(Get get: gets) {
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
            if(startRow == null) {
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
    public void put(final  Put put) throws IOException {
        try {
            timedExecute(OpType.PUT, put.getRow(), new Callable<Object>() {
                @Override
                public Object call() throws IOException {
                    normalHTable.put(put);
                    return null; // We're required by Callable to return something
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
    public void put(List<Put> puts) throws IOException {
        // This is not used in shennendoah, don't bother measuring
        normalHTable.put(puts);
    }

    @Override
    public boolean checkAndPut(final byte[] row, final byte[] family, final byte[] qualifier, final byte[] value, final Put put)
            throws IOException {
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
                    return null; // We're required by Callable to return something
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
    public void delete(List<Delete> deletes) throws IOException {
        // This is not used in shennendoah, don't bother measuring
        normalHTable.delete(deletes);
    }

    @Override
    public boolean checkAndDelete(final byte[] row, final byte[] family, final byte[] qualifier, final byte[] value, final Delete delete)
            throws IOException {
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
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
        // This is not used in shennendoah, don't bother measuring
        return normalHTable.incrementColumnValue(row, family, qualifier, amount);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL)
            throws IOException {
        // This is not used in shennendoah, don't bother measuring
        return normalHTable.incrementColumnValue(row, family, qualifier, amount, writeToWAL);
    }

    @Override
    public boolean isAutoFlush() {
        return normalHTable.isAutoFlush();
    }

    @Override
    public void flushCommits() throws IOException {
        // This is not used in shennendoah, don't bother measuring
        normalHTable.flushCommits();
    }

    @Override
    public void close() throws IOException {
        normalHTable.close();
    }

    @Override
    public RowLock lockRow(byte[] row) throws IOException {
        // This is not used in shennendoah, don't bother measuring
        return normalHTable.lockRow(row);
    }

    @Override
    public void unlockRow(RowLock rl) throws IOException {
        // This is not used in shennendoah, don't bother measuring
        normalHTable.unlockRow(rl);
    }
    
    public HTableInterface unwrap() {
        return normalHTable;
    }
    
    // TODO come up with a generic way to expose "the worst N somethings according to measurement Y"
    //      instead of the following atrocious copypasta.
    
    // A JMX gauge that gives a list of regions in descending order of mean latency
    @SuppressWarnings("unused")
    private final GaugeMetric<String> regionsByLatency = Metrics.newGauge(StatsHTable.class, "regionsByLatency",
            new GaugeMetric<String>() {
                @Override
                public String value() {
                    Map<MetricName,Metric> metricsMap = Metrics.defaultRegistry().allMetrics();
                    NavigableMap<Double,String> regionsByLatency = new TreeMap<Double,String>();
                    
                    for(Entry<MetricName,Metric> e: metricsMap.entrySet()) {
                        MetricName metricName = e.getKey();
                        Metric metric = e.getValue();
                        
                        if(metricName.getScope().equals(metricsScope) && 
                                metricName.getName().startsWith(REGION_TIMER_PREFIX) &&
                                metric instanceof TimerMetric) {
                            TimerMetric timerMetric = (TimerMetric)metric;
                            String regionName = metricName.getName().substring(REGION_TIMER_PREFIX.length());
                            regionsByLatency.put(timerMetric.mean(), regionName);
                        }
                    }

                    
                    Map<String,Double> jsonMap = new LinkedHashMap<String,Double>();
                    
                    for(Entry<Double,String> e: regionsByLatency.descendingMap().entrySet()) {
                       jsonMap.put(e.getValue(), e.getKey());
                    }
                    
                    ByteArrayOutputStream bos = new ByteArrayOutputStream(jsonMap.size() * 50 + 5);
                    try {
                        objectMapper.writeValue(bos, jsonMap);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return new String(bos.toByteArray());
                }
        });
    
    // A JMX gauge that gives a list of servers in descending order of mean latency
    @SuppressWarnings("unused")
    private final GaugeMetric<String> serversByLatency = Metrics.newGauge(StatsHTable.class, "serversByLatency",
        new GaugeMetric<String>() {
            @Override
            public String value() {
                Map<MetricName,Metric> metricsMap = Metrics.defaultRegistry().allMetrics();
                NavigableMap<Double,String> serversByLatency = new TreeMap<Double,String>();
                
                for(Entry<MetricName,Metric> e: metricsMap.entrySet()) {
                    MetricName metricName = e.getKey();
                    Metric metric = e.getValue();
                    
                    if(metricName.getScope().equals(metricsScope) && 
                            metricName.getName().startsWith(SERVER_TIMER_PREFIX) &&
                            metric instanceof TimerMetric) {
                        TimerMetric timerMetric = (TimerMetric)metric;
                        String regionName = metricName.getName().substring(SERVER_TIMER_PREFIX.length());
                        serversByLatency.put(timerMetric.mean(), regionName);
                    }
                }

                
                Map<String,Double> jsonMap = new LinkedHashMap<String,Double>();
                
                for(Entry<Double,String> e: serversByLatency.descendingMap().entrySet()) {
                   jsonMap.put(e.getValue(), e.getKey());
                }
                
                ByteArrayOutputStream bos = new ByteArrayOutputStream(jsonMap.size() * 50 + 5);
                try {
                    objectMapper.writeValue(bos, jsonMap);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return new String(bos.toByteArray());
            }
        });
}
