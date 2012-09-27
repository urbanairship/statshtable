/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.statshtable;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;

import java.util.concurrent.TimeUnit;

/**
 * A Timer with some extra features:
 *  - it remembers the timestamp of the last call to update(). This lets us find metrics that haven't been 
 *    updated recently.
 *  - it knows its JMX ObjectName (for easy removal)
 */
public class SHTimerMetric implements Metered, Stoppable {

    private long lastUpdateMillis = 0L;
    TimerMetric t;
    HistogramMetric h;

    SHTimerMetric(TimeUnit durationUnit, TimeUnit rateUnit) {
        t = Metrics.newTimer(this.getClass(),"Timer",durationUnit, rateUnit);
        h = Metrics.newHistogram(this.getClass(), "lag");
    }

    public void update(long duration, TimeUnit timeUnit) {
        lastUpdateMillis = System.currentTimeMillis();
        t.update(duration, timeUnit);
        if(timeUnit != TimeUnit.MILLISECONDS){
            h.update(TimeUnit.MILLISECONDS.convert(duration,timeUnit));
        }
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
    public <T> void processWith(MetricsProcessor<T> processor, MetricName name, T context) throws Exception {
        t.processWith(processor, name, context);
        h.processWith(processor, name, context);
    }

    @Override
    public void stop() {
        t.stop();
    }

    public Double getValue(double v) {
        return h.percentile(v);
    }
}
