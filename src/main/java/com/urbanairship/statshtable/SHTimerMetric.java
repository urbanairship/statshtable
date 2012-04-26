/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.statshtable;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import com.yammer.metrics.stats.Snapshot;

/**
 * A Timer with some extra features:
 *  - it remembers the timestamp of the last call to update(). This lets us find metrics that haven't been 
 *    updated recently.
 *  - it knows its JMX ObjectName (for easy removal)
 */
public class SHTimerMetric implements Metered, Stoppable, Sampling, Summarizable  {

    private long lastUpdateMillis = 0L;
    Timer t;

    SHTimerMetric(ScheduledExecutorService tickThread, TimeUnit durationUnit, TimeUnit rateUnit) {
        t = Metrics.newTimer(this.getClass(),"Timer",durationUnit, rateUnit);
    }

    public void update(long duration, TimeUnit timeUnit) {
        lastUpdateMillis = System.currentTimeMillis();
        t.update(duration, timeUnit);
    }

    /**
     * Get the UTC millisecond timestamp of the last update to this metric, or 0 if there were no updates.
     */
    public long getLastUpdateMillis() {
        return lastUpdateMillis;
    }

    @Override
    public TimeUnit rateUnit() {
        return t.rateUnit();
    }

    @Override
    public String eventType() {
        return t.eventType();
    }

    @Override
    public long count() {
        return t.count();
    }

    @Override
    public double fifteenMinuteRate() {
        return t.fifteenMinuteRate();
    }

    @Override
    public double fiveMinuteRate() {
        return t.fiveMinuteRate();
    }

    @Override
    public double meanRate() {
        return t.meanRate();
    }

    @Override
    public double oneMinuteRate() {
        return t.oneMinuteRate();
    }

    @Override
    public <T> void processWith(MetricProcessor<T> processor, MetricName name, T context) throws Exception {
        t.processWith(processor, name, context);
    }

    @Override
    public Snapshot getSnapshot() {
        return t.getSnapshot();
    }

    @Override
    public void stop() {
        t.stop();
    }

    @Override
    public double max() {
        return t.max();
    }

    @Override
    public double min() {
        return t.min();
    }

    @Override
    public double mean() {
        return t.mean();
    }

    @Override
    public double stdDev() {
        return t.stdDev();
    }

    @Override
    public double sum() {
        return t.sum();
    }
}
