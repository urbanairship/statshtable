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
import com.urbanairship.statshtable.StatsHTablePool;

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
    }
}
