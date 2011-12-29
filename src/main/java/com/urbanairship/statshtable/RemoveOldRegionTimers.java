package com.urbanairship.statshtable;

import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.TimerMetric;

/**
 * Since we track latency by HBase region, after a region disappears we want to stop tracking it. This will
 * occur if regions are split or merged. So we have a background thread that runs periodically to remove
 * unused TimerMetrics whose name starts with "region:".
 */
public class RemoveOldRegionTimers {
    private static final Logger log = LogManager.getLogger(RemoveOldRegionTimers.class);
    
    public static final Long SLEEP_BETWEEN_CHECKS = 60000L;
    
    static Thread thread = null;
    
    private RemoveOldRegionTimers() {
        // don't instantiate
    }
    
    /**
     * There should only be one cleanup thread per JVM. This function will start unless it's already running.
     */
    synchronized static public void startIfNot() {
        if(thread != null) {
            return;
        }
        thread = new Thread() {
            @Override
            public void run() {
                while(true) {
                    for(Map.Entry<MetricName,Metric> e: Metrics.defaultRegistry().allMetrics().entrySet()) {
                        MetricName metricName = e.getKey();
                        Metric metric = e.getValue();
                        
                        if(!(metric instanceof TimerMetric)) {
                            continue;
                        }
                        TimerMetric timerMetric = (TimerMetric) metric;
                        
                        if(!metricName.getName().startsWith(StatsHTable.REGION_TIMER_PREFIX)) {
                            continue;
                        }
                        
                        if(timerMetric.fifteenMinuteRate() == 0D) {
                            log.warn("Purging region timer for " + metricName + " due to inactivity");
                            Metrics.defaultRegistry().removeMetric(metricName);
                        }
                    }
                    try {
                        Thread.sleep(SLEEP_BETWEEN_CHECKS);
                    } catch (InterruptedException e) {
                        log.warn("Region timer cleanup thread interrupted, exiting");
                        break;
                    }
                } 
            }
        };
        thread.start();
    }
}
