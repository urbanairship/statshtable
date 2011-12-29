package com.urbanairship.statshtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

/**
 * An HTablePool whose HTables will be wrapped for stats gathering.
 */
public class StatsHTablePool extends HTablePool {
    /**
     * @param useStats if true, will use StatsHTables; if false, will use normal HTables
     * @param metricsScope the "scope" argument for yammer metrics, for disambiguating separate sets of
     * stats inside the same JVM.
     */
    public StatsHTablePool(boolean useStats, String metricsScope) {
        this(HBaseConfiguration.create(), useStats, metricsScope);
    }
    
    public StatsHTablePool(Configuration config, boolean useStats, String metricsScope) {
        super(config, Integer.MAX_VALUE, new StatsHTableFactory(useStats, metricsScope));
    }
}