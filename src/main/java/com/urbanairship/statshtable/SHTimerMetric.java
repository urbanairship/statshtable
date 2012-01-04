package com.urbanairship.statshtable;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import com.yammer.metrics.core.TimerMetric;

/**
 * A TimerMetric with some extra features: 
 *  - it remembers the timestamp of the last call to update(). This lets us find metrics that haven't been 
 *    updated recently.
 *  - it knows its JMX ObjectName (for easy removal)
 */
public class SHTimerMetric extends TimerMetric {
    private final ObjectName jmxName;
    private long lastUpdateMillis = 0L;

    public SHTimerMetric(ObjectName jmxName, ScheduledExecutorService tickThread, TimeUnit durationUnit, 
            TimeUnit rateUnit) {
        super(tickThread, durationUnit, rateUnit);
        this.jmxName = jmxName;
    }
    
    @Override
    public void update(long duration, TimeUnit timeUnit) {
        lastUpdateMillis = System.currentTimeMillis();
        super.update(duration, timeUnit);
    }
    
    /**
     * Get the UTC millisecond timestamp of the last update to this metric.
     */
    public long getLastUpdateMillis() {
        return lastUpdateMillis;
    }
    
    public ObjectName getJmxName() {
        return jmxName;
    }
    
    public static class Factory implements Callable<SHTimerMetric> {
        private final String scope;
        private final String metricName;
        private final ScheduledExecutorService tickExecutor;
        
        public Factory(ScheduledExecutorService tickExecutor, String scope, String metricName) {
            this.scope = scope;
            this.metricName = metricName;
            this.tickExecutor = tickExecutor;
        }
        
        public SHTimerMetric call() throws Exception {
            ObjectName jmxName = new ObjectName("com.urbanairship.statshtable:type=" + scope + ", name=" + metricName);
            return new SHTimerMetric(jmxName, tickExecutor, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        }
    }
}
