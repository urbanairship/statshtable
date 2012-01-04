package com.urbanairship.statshtable;

import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.management.JMException;
import javax.management.MBeanServer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Since we track latency by HBase region, after a region or server disappears we want to stop tracking it. 
 * This will occur if regions are split, or if a server is decommissioned. So we have a background thread that 
 * runs periodically to remove TimerMetrics that haven't been touched in a while. 
 */
public class RemoveOldTimers {
    private static final Logger log = LogManager.getLogger(RemoveOldTimers.class);
    
    // Timers that go this long without updating are purged
    private static final long EXPIRE_REGION_TIMERS_MS = TimeUnit.HOURS.toMillis(6); 
    
    // How often to check for stale timers
    private static final long CHECK_EVERY_MS = TimeUnit.MINUTES.toMillis(10);
    
    private static Object startupLock = new Object();
    private static boolean started = false;
    private static Set<AtomicRegistry<String,String,SHTimerMetric>> atomicRegistries = 
            new HashSet<AtomicRegistry<String,String,SHTimerMetric>>();

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
                    for(AtomicRegistry<String,String,SHTimerMetric> registry: atomicRegistries) {
                        expireOldTimers(registry);
                    }
                }
            } catch (InterruptedException e) {
                log.warn("Interrupted");
            }
        } 
    }
    
    private static void expireOldTimers(AtomicRegistry<String,String,SHTimerMetric> registry) {
        long expireOlderThan = System.currentTimeMillis() - EXPIRE_REGION_TIMERS_MS;
        for(String scope: registry.getScopes()) {
            Map<String,SHTimerMetric> metricsForScope = registry.innerMap(scope);
            if(metricsForScope == null) {
                continue;
            }
            for(Map.Entry<String,SHTimerMetric> e: metricsForScope.entrySet()) {
                SHTimerMetric metric = e.getValue();
                if(metric.getLastUpdateMillis() < expireOlderThan) {
                    String metricName = e.getKey();
                    registry.remove(scope, metricName);
                    
                    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                    try {
                        mbs.unregisterMBean(metric.getJmxName());
                    } catch (JMException ex) {
                        log.error(ex);
                    }
                }
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
