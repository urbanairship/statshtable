package com.urbanairship.statshtable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;

/**
 * A parent class for test classes that want to use an embedded HBase test cluster. Subclasses can
 * access the minicluster using the {@link #TEST_UTIL} member. 
 */
public class NeedsEmbeddedCluster {
    private static final Logger log = LogManager.getLogger(NeedsEmbeddedCluster.class);
    protected static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    
//    protected static final String scope = "stats_test";
//    protected static final byte[] table = "testtable".getBytes();
//    protected static final byte[] cf = "testcf".getBytes(); 

    private static boolean inited = false;
    
    @BeforeClass
    public static void init() throws Exception {
        if(!inited) {
            inited = true;

            // Workaround for HBASE-5711, we need to set config value dfs.datanode.data.dir.perm
            // equal to the permissions of the temp dirs on the filesystem. These temp dirs were
            // probably created using this process' umask. So we guess the temp dir permissions as
            // 0777 & ~umask, and use that to set the config value.
            try {
                Process process = Runtime.getRuntime().exec("/bin/sh -c umask");
                BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
                int rc = process.waitFor();
                if(rc == 0) {
                    String umask = br.readLine();
    
                    int umaskBits = Integer.parseInt(umask, 8);
                    int permBits = 0777 & ~umaskBits;
                    String perms = Integer.toString(permBits, 8);
                    
                    log.info("Setting dfs.datanode.data.dir.perm to " + perms);
                    TEST_UTIL.getConfiguration().set("dfs.datanode.data.dir.perm", perms);
                } else {
                    log.warn("Failed running umask command in a shell, nonzero return value");
                }
            } catch (Exception e) {
                // ignore errors, we might not be running on POSIX, or "sh" might not be on the path
                log.warn("Couldn't get umask", e);
            }
            
            TEST_UTIL.startMiniCluster();
        }
    }
    
    public static void createTable(byte[] tableName, byte[] cf) throws Exception {
        init();
        TEST_UTIL.createTable(tableName, cf).close();
    }
}
