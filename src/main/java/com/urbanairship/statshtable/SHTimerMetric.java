/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.statshtable;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.core.TimerMetric;

/**
 * A TimerMetric with some extra features: 
 *  - it remembers the timestamp of the last call to update(). This lets us find metrics that haven't been 
 *    updated recently.
 *  - it knows its JMX ObjectName (for easy removal)
 */
public class SHTimerMetric extends TimerMetric {
    private long lastUpdateMillis = 0L;

    public SHTimerMetric(ScheduledExecutorService tickThread, TimeUnit durationUnit, TimeUnit rateUnit) {
        super(tickThread, durationUnit, rateUnit);
    }
    
    @Override
    public void update(long duration, TimeUnit timeUnit) {
        lastUpdateMillis = System.currentTimeMillis();
        super.update(duration, timeUnit);
    }
    
    /**
     * Get the UTC millisecond timestamp of the last update to this metric, or 0 if there were no updates.
     */
    public long getLastUpdateMillis() {
        return lastUpdateMillis;
    }
}
