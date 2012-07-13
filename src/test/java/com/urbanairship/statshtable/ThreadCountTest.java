package com.urbanairship.statshtable;

import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * There was an issue in early versions of statshtable where an excessive number of threads was being
 * created. This test verifies that we're not creating an insane number of threads.
 */
public class ThreadCountTest extends NeedsEmbeddedCluster {
    private final static byte[] tableName = ThreadCountTest.class.getName().getBytes();
    private final static byte[] cf = "myCf".getBytes();
    
    @BeforeClass
    public static void createTable() throws Exception {
        TEST_UTIL.createTable(tableName, cf);
    }
    
    @Test
    public void threadCountTest() throws Exception {
        StatsHTablePool pool = new StatsHTablePool(TEST_UTIL.getConfiguration(), true, "myscope");
        final int numHTables = 1000;
        List<HTableInterface> hTables = Lists.newLinkedList();
        
        final byte[] qual = "abc".getBytes();
        final byte[] val = "def".getBytes();
        final byte[] rowKey = "ghi".getBytes();
        
        HTableInterface hTable = pool.getTable(tableName);
        Put put = new Put(rowKey);
        put.add(cf, qual, val);
        hTable.put(put);
        hTable.close();
        
        try {
            hTable = pool.getTable(tableName);
            for(int i=0; i<numHTables; i++) {
                hTable.get(new Get(rowKey));
                hTables.add(pool.getTable(tableName));
            }
            
            // By asserting that the number of JVM threads is less than the number of HTables created,
            // we make sure that we're not creating a thread per table.
            // This is a hack that assumes that the JVM isn't running numHTables threads for any reason.
            int numThreads = Thread.getAllStackTraces().size();
            Assert.assertTrue(numThreads < numHTables);
        } finally {
            for(HTableInterface ht: hTables) {
                ht.close();
            }
        }
        
    }
}
