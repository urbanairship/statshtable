/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.statshtable;

import java.util.concurrent.TimeUnit;

import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

/**
 * We have to subclass MetricsRegistry since MetricsRegistry since we want to use SHTimerMetrics instead of
 * normal TimerMetrics. The normal MetricsRegistry doesn't allow this.
 */
public class StatsTimerRegistry extends MetricsRegistry {
    private final String scopeSuffix;
    
    public StatsTimerRegistry(String scopeSuffix) {
        this.scopeSuffix = scopeSuffix;
    }
    /**
     * Get the SHTimerMetric if one already exists for the given scope&name, else create one.
     */
    public SHTimerMetric newSHTimerMetric(String scope, String name) {
        if(scope == null) {
            scope = "";
        }
        MetricName metricName = StatsHTable.newMetricName(scope + scopeSuffix, name);
        SHTimerMetric existingMetric = (SHTimerMetric)allMetrics().get(metricName);
        if(existingMetric != null) {
            return existingMetric;
        }
        SHTimerMetric newMetric = new SHTimerMetric(TimerThreadPoolExecutor.getTimerThreadPoolExecutor(), TimeUnit.MILLISECONDS,
                TimeUnit.SECONDS);
        existingMetric = getOrAdd(metricName, newMetric);
        if(existingMetric != null) {
            return existingMetric;
        } else {
            return newMetric;
        }        
    }
}
