package integration;

import java.util.UUID;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.urbanairship.statshtable.StatsHTable;
import com.urbanairship.statshtable.StatsHTableFactory.OpType;
import com.urbanairship.statshtable.StatsHTablePool;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.GaugeMetric;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.TimerMetric;
public class StatsHTableTest {
    private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    
    @BeforeClass
    public static final void init() throws Exception {
        TEST_UTIL.startMiniCluster();
    }
    
    @AfterClass
    public static final void cleanup() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
    }
    
    @Test
    public void basicGetPutTest() throws Exception {
        final String scope = "stats_test";
        final byte[] table = "testtable".getBytes();
        final byte[] cf = "testcf".getBytes(); 
        
        TEST_UTIL.createTable(table, cf).close();
        
        HTablePool hTablePool = new StatsHTablePool(TEST_UTIL.getConfiguration(), true, scope);
        HTableInterface hTable = hTablePool.getTable(table);
        Assert.assertTrue(hTable instanceof StatsHTable);

        byte[] uuid = UUID.randomUUID().toString().getBytes();
        Put put = new Put(uuid);
        put.add(cf, uuid, uuid);
        hTable.put(put);
        Result result = hTable.get(new Get(uuid));

        // Make sure the Put made it into the DB and the Get got it back
        Assert.assertNotNull(result);
        Assert.assertArrayEquals(uuid, result.getColumnLatest(cf, uuid).getValue());

        MetricName getsMetricsName = new MetricName(StatsHTable.class, OpType.GET.toString(), scope);
        MetricName putsMetricsName = new MetricName(StatsHTable.class, OpType.PUT.toString(), scope);
        TimerMetric getsTimer = (TimerMetric) Metrics.defaultRegistry().allMetrics().get(getsMetricsName);
        TimerMetric putsTimer = (TimerMetric) Metrics.defaultRegistry().allMetrics().get(putsMetricsName);
        
        Assert.assertTrue(getsTimer.count() != 0L);
        Assert.assertTrue(putsTimer.count() != 0L);
        
        for(Metric metric: Metrics.defaultRegistry().allMetrics().values()) {
            if(metric instanceof GaugeMetric) {
                ((GaugeMetric)metric).value(); // make sure gauges don't throw NullPointerException like that one time
            }
        }
    }
}
