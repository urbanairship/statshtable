package com.urbanairship.statshtable;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

/**
 * Since we track latency by HBase region, after a region or server disappears we want to stop tracking it. 
 * This will occur if regions are split, or if a server is decommissioned. So we have a background thread that 
 * runs periodically to remove TimerMetrics that haven't been touched in a while. 
 */
public class RemoveOldTimers {
    private static final Log log = LogFactory.getLog(RemoveOldTimers.class);
    
    // Timers that go this long without updating are purged. Should be large to avoid purging timers for regions 
    // that are accessed rarely.
    private static final long EXPIRE_REGION_TIMERS_MS = TimeUnit.DAYS.toMillis(1); 
    
    // How often to check for stale timers
    private static final long CHECK_EVERY_MS = TimeUnit.MINUTES.toMillis(10);
    
    private static Object startupLock = new Object();
    private static boolean started = false;
    private static Set<MetricsRegistry> atomicRegistries = 
            new HashSet<MetricsRegistry>();

    static {
        atomicRegistries.add(RegionTimers.getInstance());
        atomicRegistries.add(ServerTimers.getInstance());
    }
    
    private static class Worker extends Thread {
        @Override
        public void run() {
            try {
                while(true) {
                    Thread.sleep(CHECK_EVERY_MS);
                    for(MetricsRegistry registry: atomicRegistries) {
                        expireOldTimers(registry);
                    }
                }
            } catch (InterruptedException e) {
                log.warn("Interrupted");
            }
        } 
    }
    
    private static void expireOldTimers(MetricsRegistry registry) {
        long expireOlderThan = System.currentTimeMillis() - EXPIRE_REGION_TIMERS_MS;
        for(Map.Entry<MetricName,Metric> e: registry.allMetrics().entrySet()) {
            if(!(e.getValue() instanceof SHTimerMetric)) {
                continue;
            }
            SHTimerMetric metric = (SHTimerMetric)e.getValue();
            
            if(metric.getLastUpdateMillis() < expireOlderThan) {
                registry.removeMetric(e.getKey());
            }
        } 
    }
    
    /**
     * Start the background thread if it has not already been started. 
     */
    public static void startIfNot() {
        if(started) {
            return;
        }
        synchronized (startupLock) {
            if(started) {
                return;
            }
            Thread thread = new Thread(new Worker(), "Stale timer remover");
            thread.setDaemon(true);
            thread.start();
            started = true;
        }
    }
}
