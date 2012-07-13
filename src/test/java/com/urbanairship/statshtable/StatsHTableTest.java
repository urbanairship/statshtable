/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.statshtable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;

public class StatsHTableTest extends NeedsEmbeddedCluster {
    private static final String scope = "stats_test";
    private static final byte[] table =  StatsHTableTest.class.getName().getBytes();
    private static final byte[] cf = "testcf".getBytes(); 

    private static HTablePool statsHTablePool = null;
    
    private Set<HTableInterface> tablesToReturn = new HashSet<HTableInterface>();
    
    @BeforeClass
    public static final void init() throws Exception {
        createTable(table, cf);
        statsHTablePool = new StatsHTablePool(TEST_UTIL.getConfiguration(), true, scope);
   }
    
    @SuppressWarnings("deprecation")
    @After
    public void afterEach() throws Exception {
        // Return all htables to the pool after each test
        for(HTableInterface hTable: tablesToReturn) {
           statsHTablePool.putTable(hTable); 
        }
        statsHTablePool.closeTablePool(table);

        // Recreate table between tests.
        TEST_UTIL.deleteTable(table);
        TEST_UTIL.createTable(table, cf).close();
        statsHTablePool = new StatsHTablePool(TEST_UTIL.getConfiguration(),true, scope);
        
        // Clear the set of metrics that track each OpType
        Set<MetricName> metricNames = new HashSet<MetricName>(StatsHTable.opTypeTimers.allMetrics().keySet()); 
        for(MetricName metricName: metricNames) {
            StatsHTable.opTypeTimers.removeMetric(metricName);
        }
    }
    
    @Test
    public void basicGetPutTest() throws Exception {
        HTableInterface hTable = getHTable();
        byte[] key = randomRowKey();
        Put put = new Put(key);
        put.add(cf, key, key);
        hTable.put(put);
        Result result = hTable.get(new Get(key));

        // Make sure the Put made it into the DB and the Get got it back
        Assert.assertNotNull(result);
        Assert.assertArrayEquals(key, result.getColumnLatest(cf, key).getValue());
        
        assertMetricsUpdated(OpType.GET, OpType.PUT);
    }
    
    @Test
    public void testExists() throws Exception {
        HTableInterface hTable = getHTable();
        byte[] row = randomRowKey();
        Put put = new Put(row);
        put.add(cf, row, row); // Just put anything, doesn't matter what
        hTable.put(put);
        
        Get get = new Get(row);
        Assert.assertTrue(hTable.exists(get));
        
        assertMetricsUpdated(OpType.EXISTS, OpType.PUT);
    }

    @Test
    public void testBatchNewResultArray() throws Exception {
        HTableInterface hTable = getHTable();
        byte[] row = randomRowKey();
        Put put = new Put(row);
        put.add(cf, row, row); // put whatever, just write something into the row 
        hTable.batch(ImmutableList.<Row>of(put));
        
        Get get = new Get(row);
        Assert.assertTrue(hTable.exists(get));
        
        assertMetricsUpdated(OpType.BATCH, OpType.EXISTS);
    }
    
    @Test
    public void testBatchPassedResultArray() throws Exception {
        HTableInterface hTable = getHTable();
        byte[] row = randomRowKey();
        Put put = new Put(row);
        put.add(cf, row, row); // put whatever, just write something into the row 
        hTable.batch(ImmutableList.<Row>of(put));
        
        Object[] results = new Result[1];
        Get get = new Get(row);
        hTable.batch(ImmutableList.<Row>of(get), results);
        
        Assert.assertArrayEquals(row, ((Result)(results[0])).getRow());
        assertMetricsUpdated(OpType.BATCH);
    }

    @Test
    public void testMultiget() throws Exception {
        HTableInterface hTable = getHTable();
        byte[] row1 = randomRowKey();
        byte[] row2 = randomRowKey();
        
        Put put1 = new Put(row1);
        put1.add(cf, row1, row1);
        Put put2 = new Put(row2);
        put2.add(cf, row2, row2);
        
        hTable.batch(ImmutableList.<Row>of(put1, put2));
        
        List<Get> gets = ImmutableList.<Get>of(new Get(row1), new Get(row2));
        Result[] results = hTable.get(gets);

        Assert.assertArrayEquals(row1, results[0].getRow());
        Assert.assertArrayEquals(row2, results[1].getRow());
        
        assertMetricsUpdated(OpType.BATCH, OpType.MULTIGET);
    }

    @Test
    public void testGetRowOrBefore() throws Exception {
        HTableInterface hTable = getHTable();
        byte[] row = randomRowKey();
        Put put = new Put(row);
        put.add(cf, row, row);
        hTable.put(put);
        
        @SuppressWarnings("deprecation")
        Result result = hTable.getRowOrBefore(row, cf);
        
        Assert.assertArrayEquals(row, result.getRow());
        assertMetricsUpdated(OpType.PUT, OpType.GET_ROW_OR_BEFORE);
    }

    @Test
    public void testGetScannerScanOnly() throws Exception {
        HTableInterface hTable = getHTable();
        byte[] row = randomRowKey();
        Put put = new Put(row);
        put.add(cf, row, row);
        hTable.put(put);

        ResultScanner rs = hTable.getScanner(new Scan());
        
        Assert.assertArrayEquals(row, rs.next().getRow());
        assertMetricsUpdated(OpType.PUT, OpType.GET_SCANNER, OpType.RESULTSCANNER_NEXT);
        
        rs.close();
    }

    @Test
    public void testResultScannerIterator() throws Exception {
        HTableInterface hTable = getHTable();
        byte[] row = randomRowKey();
        Put put = new Put(row);
        put.add(cf, row, row);
        hTable.put(put);

        ResultScanner rs = hTable.getScanner(new Scan());
        
        Assert.assertArrayEquals(row, rs.iterator().next().getRow());
        assertMetricsUpdated(OpType.PUT, OpType.GET_SCANNER, OpType.RESULTSCANNER_ITERATOR,
                OpType.RESULTITERATOR_NEXT);
        
        rs.close();
    }

    /**
     * Test that the wrapped ResultScanner.next(int) works, and also ResultScanner.close()
     */
    @Test
    public void testResultScannerIteratorMultiRowAndClose() throws Exception {
        HTableInterface hTable = getHTable();
        
        List<byte[]> rows = getRandomRows(2);

        Put put1 = new Put(rows.get(0));
        Put put2 = new Put(rows.get(1));
        put1.add(cf, rows.get(0), rows.get(0));
        put2.add(cf, rows.get(1), rows.get(1));
        hTable.batch(ImmutableList.<Row>of(put1, put2));

        ResultScanner rs = hTable.getScanner(new Scan());
        
        Result[] results = rs.next(2);
        Assert.assertArrayEquals(rows.get(0), results[0].getRow());
        Assert.assertArrayEquals(rows.get(1), results[1].getRow());

        rs.close();

        assertMetricsUpdated(OpType.BATCH, OpType.GET_SCANNER, OpType.RESULTSCANNER_NEXTARRAY,
                OpType.RESULTSCANNER_CLOSE);
    }
    
    /**
     * Test that the wrapped ResultScanner.next(int) works, and also ResultScanner.close()
     */
    @Test
    public void testResultScannerIteratorHasNext() throws Exception {
        HTableInterface hTable = getHTable();
        byte[] row = randomRowKey();

        Put put = new Put(row);
        put.add(cf, row, row);
        hTable.put(put);

        ResultScanner rs = hTable.getScanner(new Scan());

        Assert.assertTrue(rs.iterator().hasNext());

        rs.close();

        assertMetricsUpdated(OpType.PUT, OpType.GET_SCANNER, OpType.RESULTSCANNER_ITERATOR,
                OpType.RESULTSCANNER_CLOSE, OpType.RESULTITERATOR_HASNEXT);
    }
    
    /**
     * Test getScanner(byte[] family)
     */
    @Test
    public void testGetScannerCfName() throws Exception {
        HTableInterface hTable = getHTable();
        byte[] row = randomRowKey();

        Put put = new Put(row);
        put.add(cf, row, row);
        hTable.put(put);

        ResultScanner rs = hTable.getScanner(cf);

        Assert.assertNotNull(rs.next());

        assertMetricsUpdated(OpType.PUT, OpType.GET_SCANNER, OpType.RESULTSCANNER_NEXT);
        
        rs.close();
    }
    
   /**
    * Test getScanner(byte[] family)
    */
   @Test
   public void testGetScannerCfAndQual() throws Exception {
       HTableInterface hTable = getHTable();
       byte[] row = randomRowKey();
       byte[] qual = randomRowKey();

       Put put = new Put(row);
       put.add(cf, qual, row);
       hTable.put(put);

       ResultScanner rs = hTable.getScanner(cf, qual);

       Assert.assertNotNull(rs.next());

       assertMetricsUpdated(OpType.PUT, OpType.GET_SCANNER, OpType.RESULTSCANNER_NEXT);

       rs.close();
   }
   
   @Test
   public void testMultiPut() throws Exception {
       HTableInterface hTable = getHTable();
       List<byte[]> rows = getRandomRows(2); 

       Put put1 = new Put(rows.get(0));
       Put put2 = new Put(rows.get(1));
       put1.add(cf, rows.get(0), rows.get(0));
       put2.add(cf, rows.get(1), rows.get(1));
       hTable.put(ImmutableList.of(put1, put2));

       ResultScanner rs = hTable.getScanner(cf);
       Result[] results = rs.next(2);
       Assert.assertArrayEquals(rows.get(0), results[0].getRow());
       Assert.assertArrayEquals(rows.get(1), results[1].getRow());
       
       assertMetricsUpdated(OpType.MULTIPUT, OpType.RESULTSCANNER_NEXTARRAY, 
               OpType.GET_SCANNER);
       
       rs.close();
   }
   
   @Test
   public void testcheckAndPut() throws Exception {
       HTableInterface hTable = getHTable();
       byte[] row = randomRowKey();
       Put put = new Put(row);
       put.add(cf, row, row);
       hTable.put(put);

       Put newPut = new Put(row);
       byte[] randBytes = randomRowKey();
       newPut.add(cf, randBytes, randBytes);
       hTable.checkAndPut(row, cf, row, row, newPut);

       Result result = hTable.get(new Get(row));
       Assert.assertEquals(2, result.list().size());
       
       assertMetricsUpdated(OpType.PUT, OpType.CHECK_AND_PUT, OpType.GET);
   }

   @Test
   public void testDelete() throws Exception {
       HTableInterface hTable = getHTable();
       byte[] row = randomRowKey();
       Put put = new Put(row);
       put.add(cf, row, row);
       hTable.put(put);

       hTable.delete(new Delete(row));
       
       Assert.assertTrue(hTable.get(new Get(row)).isEmpty());
       
       assertMetricsUpdated(OpType.PUT, OpType.GET, OpType.DELETE);
   }
   
   @Test
   public void testMultiDelete() throws Exception {
       HTableInterface hTable = getHTable();
       
       final int ROWS_TO_INSERT = 5;
       
       List<byte[]> rows = getRandomRows(ROWS_TO_INSERT);
       for(byte[] row: rows) {
           Put put = new Put(row);
           put.add(cf, row, row);
           hTable.put(put);
       }

       ResultScanner rs = hTable.getScanner(new Scan());
       int countRows = 0;
       while(rs.next() != null) {
           countRows++;
       }
       rs.close();
       Assert.assertEquals(ROWS_TO_INSERT, countRows);
       
       List<Delete> deletes = new ArrayList<Delete>();
       for(byte[] row: rows) {
           deletes.add(new Delete(row));
       }
       hTable.delete(deletes);
       
       rs = hTable.getScanner(new Scan());
       countRows = 0;
       while(rs.next() != null) {
           countRows++;
       }
       Assert.assertEquals(0, countRows);
       rs.close();
       
       assertMetricsUpdated(OpType.PUT, OpType.RESULTSCANNER_NEXT, OpType.MULTIDELETE,
               OpType.RESULTSCANNER_CLOSE, OpType.GET_SCANNER);
       
   }
   
   @Test
   public void testCheckAndDelete() throws Exception {
       HTableInterface hTable = getHTable();
       byte[] row = randomRowKey();
       Put put = new Put(row);
       put.add(cf, row, row);
       hTable.put(put);

       hTable.checkAndDelete(row, cf, row, row, new Delete(row));

       Assert.assertTrue(hTable.get(new Get(row)).isEmpty());
       assertMetricsUpdated(OpType.PUT, OpType.CHECK_AND_DELETE, OpType.GET);

   }
   
   @Test
   public void testIcv() throws Exception {
       HTableInterface hTable = getHTable();
       byte[] row = randomRowKey();
       Put put = new Put(row);
       put.add(cf, row, Bytes.toBytes(10L));
       hTable.put(put);

       hTable.incrementColumnValue(row, cf, row, 5);

       Result result = hTable.get(new Get(row));
       Assert.assertEquals(15L, Bytes.toLong(result.value()));
       assertMetricsUpdated(OpType.PUT, OpType.INCREMENT, OpType.GET);

   }

   @Test
   public void testIcvWithWal() throws Exception {
       HTableInterface hTable = getHTable();
       byte[] row = randomRowKey();
       Put put = new Put(row);
       put.add(cf, row, Bytes.toBytes(10L));
       hTable.put(put);

       hTable.incrementColumnValue(row, cf, row, 5, true);

       Result result = hTable.get(new Get(row));
       Assert.assertEquals(15L, Bytes.toLong(result.value()));
       assertMetricsUpdated(OpType.PUT, OpType.INCREMENT, OpType.GET);

   }

   @Test
   public void testFlushCommits() throws Exception {
       HTableInterface hTable = getHTable();
       hTable.flushCommits();
       assertMetricsUpdated(OpType.FLUSHCOMMITS);

   }

   @Test
   public void testLockUnlock() throws Exception {
       HTableInterface hTable = getHTable();
       byte[] row = randomRowKey();
       Put put = new Put(row);
       put.add(cf, row, row);
       hTable.put(put);

       RowLock lock = hTable.lockRow(row);
       lock.getLockId();
       hTable.unlockRow(lock);

       assertMetricsUpdated(OpType.PUT, OpType.LOCKROW, OpType.UNLOCKROW);
   }

// Copy-paste template for future tests
//  @Test
//  public void testX() throws Exception {
//      HTableInterface hTable = getHTable();
//      byte[] row = randomRowKey();
//      Put put = new Put(row);
//      put.add(cf, row, row);
//      hTable.put(put);
//
//      hTable.
//      
//      Assert.assertArrayEquals(row, )
//      assertMetricsUpdated(OpType.PUT, );
//      
//  }

    
    private HTableInterface getHTable() {
        HTableInterface hTable = statsHTablePool.getTable(table);
        tablesToReturn.add(hTable);
        return hTable;
    }
    
    /**
     * Get a random row key guaranteed not to collide with any other keys being used in the test.
     */
    private static byte[] randomRowKey() {
        UUID uuid = UUID.randomUUID();
        byte[] highBytes = Bytes.toBytes(uuid.getMostSignificantBits());
        byte[] lowBytes = Bytes.toBytes(uuid.getLeastSignificantBits());
        return Bytes.add(highBytes, lowBytes);
    }
    
    private void assertMetricsUpdated(OpType... opTypes) {
        assertMetricsUpdated(Arrays.asList(opTypes));
    }
    
    /**
     * Verify that the metrics for the given OpTypes have been updated, and no others. 
     */
    private void assertMetricsUpdated(List<OpType> opTypes) {
        Set<MetricName> expectedMetrics = new HashSet<MetricName>();
        for(OpType opType: opTypes) {
            expectedMetrics.add(StatsHTable.newMetricName(scope + "_opTypes", opType.toString()));
        }
        
        Map<MetricName,Metric> updatedMetrics = StatsHTable.opTypeTimers.allMetrics();
        
        // Assert that the expected metrics were present and received at least one update
        for(MetricName metricName: expectedMetrics) {
            Metric metric = updatedMetrics.get(metricName);
            if(metric == null) {
                Assert.fail("Metric was unexpectedly absent for " + metricName);
            }
            SHTimerMetric timerMetric = (SHTimerMetric)metric;
            if(timerMetric.count() < 1) {
                Assert.fail("Metric should have received an update but had count of 0 for " + metricName);
            }
        }

        // Assert that no other metrics got updated other than the ones specified
        for(MetricName metricName: updatedMetrics.keySet()) {
            if(!expectedMetrics.contains(metricName)) {
                Assert.fail("Some other metric was unexpectedly updated. Expected one of: " + 
                        opTypes + " but saw " + metricName);
            }
        }
    }
    
    /**
     * Get multiple random row keys, in lexicographic sort order.
     */
    private List<byte[]> getRandomRows(int howMany) {
        List<byte[]> rows = new ArrayList<byte[]>();
        for(int i=0; i<howMany; i++) {
            rows.add(randomRowKey());
        }
        Collections.sort(rows, Bytes.BYTES_RAWCOMPARATOR);
        return rows;
    }
}
