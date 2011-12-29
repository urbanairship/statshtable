package integration;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.urbanairship.shennendoah.hbase.Schema;
import com.urbanairship.statshtable.StatsHTable;
import com.urbanairship.statshtable.StatsHTableFactory.OpType;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.TimerMetric;

public class StatsHTableTest {

    @BeforeClass
    public static void skipUnlessIntegration() {
        IntegrationConfig.init();
        Assume.assumeTrue(IntegrationConfig.doInteg());
    }
    
    @Test
    public void basicGetPutTest() throws IOException {
        final String scope = "stats_test";
        HTablePool hTablePool = new StatsHTablePool(true, scope);
        HTableInterface hTable = hTablePool.getTable(Schema.TABLE_SCRATCH);
        Assert.assertTrue(hTable instanceof StatsHTable);

        byte[] uuid = UUID.randomUUID().toString().getBytes();
        Put put = new Put(uuid);
        put.add(Schema.CF_SCRATCH_ALL, uuid, uuid);
        hTable.put(put);
        Result result = hTable.get(new Get(uuid));

        // Make sure the Put made it into the DB and the Get got it back
        Assert.assertNotNull(result);
        Assert.assertArrayEquals(uuid, result.getColumnLatest(Schema.CF_SCRATCH_ALL, uuid).getValue());

        MetricName getsMetricsName = new MetricName(StatsHTable.class, OpType.GET.toString(), scope);
        MetricName putsMetricsName = new MetricName(StatsHTable.class, OpType.PUT.toString(), scope);
        TimerMetric getsTimer = (TimerMetric) Metrics.defaultRegistry().allMetrics().get(getsMetricsName);
        TimerMetric putsTimer = (TimerMetric) Metrics.defaultRegistry().allMetrics().get(putsMetricsName);
        
        Assert.assertTrue(getsTimer.count() != 0L);
        Assert.assertTrue(putsTimer.count() != 0L);
    }
}
